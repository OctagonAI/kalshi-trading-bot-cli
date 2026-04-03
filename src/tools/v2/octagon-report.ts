import { DynamicStructuredTool } from '@langchain/core/tools';
import { z } from 'zod';
import { getDb } from '../../db/index.js';
import { getLatestReport } from '../../db/octagon-cache.js';
import { createOctagonInvoker, callOctagon } from '../../scan/invoker.js';
import { OctagonClient } from '../../scan/octagon-client.js';
import { auditTrail } from '../../audit/index.js';
import { formatToolResult } from '../types.js';

export const octagonReportTool = new DynamicStructuredTool({
  name: 'octagon_report',
  description: 'Fetch a full Octagon AI research report for a Kalshi market. Accepts a ticker or a full Kalshi market URL.',
  schema: z.object({
    ticker: z.string().describe('Market ticker (e.g. KXBTC-26MAR-B80000) or full Kalshi URL (e.g. https://kalshi.com/markets/kxcpiyoy/inflation/kxcpiyoy-26mar)'),
    forceRefresh: z.boolean().optional().describe('Force a fresh API call instead of using cache'),
  }),
  func: async ({ ticker, forceRefresh }) => {
    const db = getDb();
    const isUrl = ticker.startsWith('http');

    // Extract ticker from URL for cache lookup if needed
    const cacheKey = isUrl ? ticker.split('/').pop() ?? ticker : ticker;

    // Always call Octagon API — no local DB cache
    // Octagon's own API has a cache variant (0 credits) that handles caching server-side
    if (!process.env.OCTAGON_API_KEY) {
      return formatToolResult({
        error: 'OCTAGON_API_KEY not set. Cannot fetch fresh report.',
        ticker: cacheKey,
        ...((() => {
          const stale = getLatestReport(db, cacheKey);
          if (stale) {
            return {
              staleCache: true,
              modelProb: stale.model_prob,
              marketProb: stale.market_prob,
              mispricingSignal: stale.mispricing_signal,
              drivers: stale.drivers_json ? JSON.parse(stale.drivers_json) : [],
              fetchedAt: stale.fetched_at,
            };
          }
          return {};
        })()),
      });
    }

    const invoker = createOctagonInvoker();
    const client = new OctagonClient(invoker, db, auditTrail);
    const parts = cacheKey.split('-');
    const eventTicker = parts.length >= 2 ? `${parts[0]}-${parts[1]}` : cacheKey;

    // Try Octagon's cache first (0 credits), fall back to default if useless
    const input = isUrl ? ticker : cacheKey;
    let variant: 'cache' | 'default' | 'refresh' = 'cache';
    let raw = await callOctagon(input, variant);
    let report = client.parseReport(raw, cacheKey, eventTicker, variant);

    // If cache returned a useless report (default 0.5/0.5), retry with 'default'
    if (report.modelProb === 0.5 && report.drivers.length === 0) {
      variant = 'default';
      raw = await callOctagon(input, variant);
      report = client.parseReport(raw, cacheKey, eventTicker, variant);
    }

    // Persist to DB
    const { insertReport } = await import('../../db/octagon-cache.js');
    const dbRow = client.toDbRow(report);
    insertReport(db, dbRow);

    return formatToolResult({
      ticker: cacheKey,
      cached: variant === 'cache',
      modelProb: report.modelProb,
      marketProb: report.marketProb,
      mispricingSignal: report.mispricingSignal,
      drivers: report.drivers,
      catalysts: report.catalysts,
      sources: report.sources,
      resolutionHistory: report.resolutionHistory,
      contractSnapshot: report.contractSnapshot,
      variantUsed: report.variantUsed,
      fetchedAt: report.fetchedAt,
    });
  },
});

export const OCTAGON_REPORT_DESCRIPTION = `
Fetch a full Octagon AI research report for a Kalshi market. Returns model probability, price drivers, catalysts, and sources.

## When to Use
- User asks for a deep dive, analysis, or research on any market
- User asks about edge, mispricing, or probability estimates
- Any time you want model fair value vs market price
- Use alongside kalshi_search for comprehensive analysis

## Input
- IMPORTANT: NEVER guess or construct tickers yourself — only use exact tickers returned by kalshi_search results
- kalshi_search already auto-fetches an Octagon report for the top result — check if the data you need is already in the kalshi_search response before calling this tool separately
- PREFERRED: Pass a full Kalshi URL (e.g. https://kalshi.com/markets/kxcpiyoy/inflation/kxcpiyoy-26mar) — this is what Octagon expects
- Also accepts a market ticker (e.g. KXBTC-26MAR-B80000) — will be resolved to a URL automatically
- If you got market data from kalshi_search, construct the URL as: https://kalshi.com/markets/SERIES/EVENT/TICKER (all lowercase) using the series_ticker, event_ticker, and ticker fields

## When NOT to Use
- For quick edge data already in the database (use edge_query)
- For market prices or orderbook data only (use kalshi_search)
- When kalshi_search already returned an octagon_report in its response — don't call again

## Notes
- Returns cached reports when available (< 24h old)
- Use forceRefresh=true to get a fresh report (costs API credits)
- When analyzing an event with multiple markets, pick the most relevant ticker and call this tool — don't ask the user to choose
`.trim();
