import { DynamicStructuredTool } from '@langchain/core/tools';
import { z } from 'zod';
import { reviewPortfolio } from '../../commands/review.js';
import { formatToolResult } from '../types.js';

export const portfolioReviewTool = new DynamicStructuredTool({
  name: 'portfolio_review',
  description: 'Review all open positions for close recommendations. Analyzes edge direction vs position direction and flags positions where the edge has reversed.',
  schema: z.object({
    sellOnly: z.boolean().optional().describe('If true, only return positions with SELL signals'),
  }),
  func: async ({ sellOnly }) => {
    const reviews = await reviewPortfolio();

    const filtered = sellOnly ? reviews.filter((r) => r.signal === 'SELL') : reviews;

    const summary = {
      totalPositions: reviews.length,
      sellSignals: reviews.filter((r) => r.signal === 'SELL').length,
      holdSignals: reviews.filter((r) => r.signal === 'HOLD').length,
    };

    return formatToolResult({
      summary,
      positions: filtered.map((r) => ({
        ticker: r.ticker,
        direction: r.direction,
        size: r.size,
        edge: r.edge,
        edgePp: `${r.edge >= 0 ? '+' : ''}${(r.edge * 100).toFixed(0)}pp`,
        signal: r.signal,
        reason: r.reason,
        closePriceCents: r.closePriceCents,
        sellCommand: r.signal === 'SELL'
          ? `/sell ${r.ticker} ${r.size} ${r.closePriceCents} ${r.direction}`
          : null,
        analyzeError: r.analyzeError ?? null,
      })),
    });
  },
});

export const PORTFOLIO_REVIEW_DESCRIPTION = `
Review all open positions for close (sell) recommendations based on edge reversal.

## When to Use
- User asks "what should I close?", "review my positions", "any sells?"
- User wants to close underwater or reversed-edge positions
- Before executing portfolio-wide close actions
- User asks to "close my positions" or "sell my holdings"

## When NOT to Use
- Quick balance check only (use portfolio_overview)
- Opening new positions (use edge_query or scan_markets)

## How to Act on Results
For each position with signal=SELL, use kalshi_trade to execute the close.
Each trade requires user approval. Present the sell recommendations first, then execute.
`.trim();
