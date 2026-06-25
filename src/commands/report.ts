/**
 * `report <ticker>` — print the full Octagon markdown report for an event.
 *
 * Focused command: no Kelly, no risk gate, no trade prompts — just the raw
 * markdown body Octagon generates for the event. Common use case: a user
 * wants to see the deep-research report so they can decide whether to
 * --refresh it.
 *
 * Input handling is more forgiving than `analyze`:
 *   1. Normalize URL / case via normalizeKalshiInput
 *   2. Try Octagon's events endpoint directly first (works for any covered
 *      event regardless of Kalshi market liquidity / trade state)
 *   3. Fall back to resolveMarket for series tickers and other forms that
 *      need the Kalshi /events lookup chain
 *
 * Step 2 fixes the user-reported case where `analyze KXAAPLCEOCHANGE` blew
 * up at the Kalshi resolver but Octagon clearly has the event.
 */
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import { getDb } from '../db/index.js';
import { auditTrail } from '../audit/index.js';
import { OctagonClient } from '../scan/octagon-client.js';
import { createOctagonInvoker } from '../scan/invoker.js';
import { fetchOctagonEventDirect } from '../scan/octagon-events-api.js';
import type { OctagonEventEntry } from '../scan/octagon-events-api.js';
import { normalizeKalshiInput, resolveMarket } from './analyze.js';
import { callKalshiApi } from '../tools/kalshi/api.js';
import { formatRawReport } from '../controllers/browse.js';
import { theme } from '../theme.js';
import { formatAge } from '../utils/time.js';

/**
 * Pick a market_ticker we can hand to the Octagon invoker. The invoker calls
 * Kalshi `/markets/{ticker}` to build the canonical kalshi.com URL — it needs
 * a MARKET ticker (e.g. `KXAAPLCEOCHANGE-T2027`), not an event ticker.
 *
 * Three sources, in preference order:
 *   1. Octagon's `outcome_probabilities[0].market_ticker` (always present for
 *      events with a deep-research report).
 *   2. Kalshi `/events/{event_ticker}?with_nested_markets=true`, picking any
 *      market (open if available, else first listed — historical reports for
 *      closed events still need a real market_ticker to build the URL).
 *   3. The event_ticker itself, as a last-resort guess (a few one-market
 *      events use it as their market ticker too).
 */
async function pickMarketTickerForInvoker(eventTicker: string, ev: OctagonEventEntry | null): Promise<string> {
  const outcomes = ev?.outcome_probabilities ?? [];
  if (outcomes.length > 0 && outcomes[0]?.market_ticker) return outcomes[0].market_ticker;

  try {
    const res = await callKalshiApi('GET', `/events/${eventTicker}`, {
      params: { with_nested_markets: true },
    });
    const event = ((res as Record<string, unknown>).event ?? res) as Record<string, unknown>;
    const markets = (event.markets as Array<Record<string, unknown>> | undefined) ?? [];
    if (markets.length > 0) {
      const open = markets.find((m) => m.status === 'open' || m.status === 'active');
      const pick = (open ?? markets[0]).ticker as string | undefined;
      if (pick) return pick;
    }
  } catch {
    // Kalshi auth missing / event missing / network — fall through to the
    // event_ticker guess. The invoker will produce a clearer downstream
    // error if the guess turns out to be wrong.
  }

  return eventTicker;
}

/**
 * Octagon's cache-variant endpoint returns a small JSON envelope when there is
 * no cached report — typically `{"versions":[]}` or `{"cache_miss":true}`.
 * For the report command, the raw markdown body is the value; OctagonClient's
 * inferential `cacheMiss` flag (which triggers on "no model_prob extracted")
 * mislabels legitimate report bodies that lack a parseable probability.
 * Detect the actual cache-miss envelope shape instead.
 */
function isOctagonCacheMissEnvelope(raw: string): boolean {
  const trimmed = raw.trim();
  if (!trimmed) return true;
  if (!trimmed.startsWith('{')) return false;
  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    if (parsed.cache_miss === true || parsed.cacheMiss === true) return true;
    if (Array.isArray(parsed.versions) && parsed.versions.length === 0) return true;
    return false;
  } catch {
    return false;
  }
}

export interface ReportData {
  /** The ticker we ended up using to fetch the report (event_ticker). */
  ticker: string;
  /** What the user typed (before normalization). */
  requestedTicker: string;
  /** Octagon's event name / title, when available. */
  title: string | null;
  /** Source: 'cache' (served from local cache), 'fresh' (just pulled from Octagon), 'cache-miss' (couldn't get either). */
  source: 'cache' | 'fresh' | 'cache-miss';
  /** Raw markdown report body. Empty string when source = 'cache-miss'. */
  rawReport: string;
  /** Local cache timestamp (UTC), if we have one. */
  refreshedAt: string | null;
  /** Octagon upstream model-run timestamp, if known. */
  modelRunAt: string | null;
  /** Human-readable age string like "12m ago". */
  reportAge: string | null;
}

export async function handleReport(args: ParsedArgs): Promise<CLIResponse<ReportData>> {
  const rawInput = args.positionalArgs[0];
  if (!rawInput) {
    return wrapError(
      'report',
      'MISSING_TICKER',
      'Usage: report <event_ticker | market_ticker | series_ticker | kalshi_url> [--refresh]',
    );
  }
  const input = normalizeKalshiInput(rawInput);
  const db = getDb();

  // Step 1: try input as an event ticker directly via Octagon. This works
  // for events where Kalshi's resolver chain fails (e.g. the series has
  // no open markets right now, or the event ticker form doesn't match
  // Kalshi's expected shape).
  let eventTicker: string | null = null;
  let title: string | null = null;
  let octagonEvent: OctagonEventEntry | null = null;
  let analysisLastUpdated: string | null = null;
  try {
    octagonEvent = await fetchOctagonEventDirect(input);
    if (octagonEvent) {
      eventTicker = octagonEvent.event_ticker;
      title = octagonEvent.name ?? null;
      analysisLastUpdated = octagonEvent.analysis_last_updated ?? null;
    }
  } catch {
    // Octagon failure shouldn't block — we'll try the Kalshi path next.
  }

  // Step 2: fall back to Kalshi's resolver chain (market → event → series).
  if (!eventTicker) {
    try {
      const market = await resolveMarket(input);
      eventTicker = market.event_ticker;
      title = market.title ?? null;
      // Take one more shot at the events endpoint with the resolved
      // event ticker — gets us a better title and the upstream timestamp.
      try {
        octagonEvent = await fetchOctagonEventDirect(eventTicker);
        if (octagonEvent?.name) title = octagonEvent.name;
        if (octagonEvent?.analysis_last_updated) analysisLastUpdated = octagonEvent.analysis_last_updated;
      } catch { /* ignore */ }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return wrapError(
        'report',
        'EVENT_NOT_FOUND',
        `Could not find an event for "${rawInput}" (normalized: "${input}"). ${msg}`,
      );
    }
  }

  // Step 3: fetch the report. The invoker expects a MARKET ticker (not an
  // event ticker) — it calls Kalshi `/markets/{ticker}` to build the
  // canonical kalshi.com URL Octagon needs. Pick one before invoking.
  const marketTickerForInvoker = await pickMarketTickerForInvoker(eventTicker, octagonEvent);

  const invoker = createOctagonInvoker();
  const client = new OctagonClient(invoker, db, auditTrail);

  let rawReport = '';
  let source: ReportData['source'] = 'cache-miss';
  let refreshedAtEpoch: number | null = null;

  if (!args.refresh) {
    const prefetched = client.tryFromPrefetch(marketTickerForInvoker, eventTicker);
    if (prefetched?.rawResponse && !isOctagonCacheMissEnvelope(prefetched.rawResponse)) {
      rawReport = prefetched.rawResponse;
      source = 'cache';
      refreshedAtEpoch = prefetched.fetchedAt;
    }
  }

  if (!rawReport) {
    try {
      const variant = args.refresh ? 'refresh' : 'cache';
      const fresh = await client.fetchReport(marketTickerForInvoker, eventTicker, variant);
      if (fresh.rawResponse && !isOctagonCacheMissEnvelope(fresh.rawResponse)) {
        rawReport = fresh.rawResponse;
        source = args.refresh ? 'fresh' : 'cache';
        refreshedAtEpoch = fresh.fetchedAt;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return wrapError('report', 'OCTAGON_ERROR', msg);
    }
  }

  if (!rawReport) {
    return wrapError(
      'report',
      'NO_REPORT',
      `Octagon has no report body for ${eventTicker} yet. The event exists but no deep-research report has been generated. Try \`report ${eventTicker} --refresh\` to force one.`,
    );
  }

  const refreshedAt = refreshedAtEpoch
    ? new Date(refreshedAtEpoch * 1000).toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
    : null;
  const reportAge = refreshedAtEpoch ? formatAge(refreshedAtEpoch) : null;
  const modelRunAt = analysisLastUpdated
    ? analysisLastUpdated.replace('T', ' ').slice(0, 16) + ' UTC'
    : null;

  return wrapSuccess('report', {
    ticker: eventTicker,
    requestedTicker: rawInput,
    title,
    source,
    rawReport,
    refreshedAt,
    modelRunAt,
    reportAge,
  });
}

export function formatReportHuman(data: ReportData): string {
  const lines: string[] = [];
  lines.push(formatRawReport(data.rawReport, data.ticker));
  lines.push('');
  if (data.title) lines.push(theme.muted(`  Title: ${data.title}`));
  if (data.refreshedAt) {
    const age = data.reportAge ? ` (${data.reportAge})` : '';
    lines.push(theme.muted(`  Cache refreshed at:    ${data.refreshedAt}${age}`));
  }
  if (data.modelRunAt) {
    lines.push(theme.muted(`  Report body updated at: ${data.modelRunAt}   (upstream Octagon analysis_last_updated)`));
  }
  if (data.source === 'cache') {
    lines.push(theme.muted(`  Source: cached. Run \`report ${data.ticker} --refresh\` to force a fresh pull (costs 3 Octagon credits).`));
  } else if (data.source === 'fresh') {
    lines.push(theme.muted(`  Source: freshly generated.`));
  }
  return lines.join('\n');
}
