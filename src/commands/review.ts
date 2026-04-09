import { callKalshiApi } from '../tools/kalshi/api.js';
import type { KalshiPosition } from '../tools/kalshi/types.js';
import { handleAnalyze } from './analyze.js';
import type { AnalyzeData } from './analyze.js';
import { parsePriceField } from '../controllers/browse.js';
import { formatBoxHeader } from './formatters.js';

export interface PositionReview {
  ticker: string;
  direction: 'yes' | 'no';
  size: number;
  entryPrice: number | null;
  currentMarketProb: number;
  modelProb: number;
  edge: number;
  signal: 'HOLD' | 'SELL';
  sellSide: 'yes' | 'no';
  closePriceCents: number;
  reason: string;
  analyzeError?: string;
}

const SELL_THRESHOLD = 0.03; // minimum edge reversal to trigger SELL signal

/**
 * Fetch all live Kalshi positions with non-zero holdings,
 * run edge analysis on each, and return HOLD/SELL recommendations.
 */
export async function reviewPortfolio(): Promise<PositionReview[]> {
  const data = await callKalshiApi('GET', '/portfolio/positions');
  const allPositions = (data.market_positions ?? data.positions ?? []) as KalshiPosition[];

  const nonZero = allPositions.filter((p) => {
    const pos = parseFloat(String(p.position ?? '0'));
    return pos !== 0;
  });

  if (nonZero.length === 0) return [];

  // Run analysis concurrently (cached — no Octagon credits consumed)
  // Pass preloaded position to avoid N+1 portfolio fetches inside handleAnalyze
  const results = await Promise.allSettled(
    nonZero.map((p) => {
      const rawPos = parseFloat(String(p.position ?? '0'));
      const pos = rawPos !== 0
        ? { direction: (rawPos > 0 ? 'yes' : 'no') as 'yes' | 'no', size: Math.abs(Math.round(rawPos)) }
        : null;
      return handleAnalyze(p.ticker, false, pos);
    })
  );

  return results.map((result, i) => {
    const pos = nonZero[i];
    const rawPos = parseFloat(String(pos.position ?? '0'));
    const direction: 'yes' | 'no' = rawPos > 0 ? 'yes' : 'no';
    const size = Math.abs(Math.round(rawPos));

    if (result.status === 'rejected') {
      const err = result.reason instanceof Error ? result.reason.message : String(result.reason);
      return {
        ticker: pos.ticker,
        direction,
        size,
        entryPrice: null,
        currentMarketProb: 0,
        modelProb: 0,
        edge: 0,
        signal: 'HOLD' as const,
        sellSide: direction,
        closePriceCents: 0,
        reason: 'Analysis failed — manual review required',
        analyzeError: err,
      };
    }

    const analysis: AnalyzeData = result.value;
    const { edge, marketProb, modelProb, kelly } = analysis;

    // Determine if edge has reversed against our position
    let signal: 'HOLD' | 'SELL' = 'HOLD';
    let reason = '';

    if (direction === 'yes' && edge < -SELL_THRESHOLD) {
      signal = 'SELL';
      reason = `Edge reversed: model now favors NO by ${Math.abs(edge * 100).toFixed(0)}pp`;
    } else if (direction === 'no' && edge > SELL_THRESHOLD) {
      signal = 'SELL';
      reason = `Edge reversed: model now favors YES by ${(edge * 100).toFixed(0)}pp`;
    } else if (direction === 'yes' && edge >= 0) {
      reason = `Still favorable: +${(edge * 100).toFixed(0)}pp edge`;
    } else if (direction === 'no' && edge <= 0) {
      reason = `Still favorable: ${(edge * 100).toFixed(0)}pp edge`;
    } else {
      // Edge has decayed but not reversed past threshold
      const decay = direction === 'yes' ? edge : -edge;
      reason = `Edge decayed (${(decay * 100).toFixed(0)}pp) but below sell threshold`;
    }

    // Use the bid-derived close price from handleAnalyze when available,
    // fall back to marketProb approximation only if missing
    const closePriceCents =
      analysis.closePriceCents && analysis.closePriceCents > 0
        ? analysis.closePriceCents
        : Math.round(
            direction === 'yes'
              ? marketProb * 100 - 1
              : (1 - marketProb) * 100 - 1
          );

    return {
      ticker: pos.ticker,
      direction,
      size,
      entryPrice: kelly.entryPriceCents > 0 ? kelly.entryPriceCents : null,
      currentMarketProb: marketProb,
      modelProb,
      edge,
      signal,
      sellSide: direction,
      closePriceCents: Math.max(1, closePriceCents),
      reason,
    };
  });
}

export function formatReviewHuman(reviews: PositionReview[]): string {
  const lines: string[] = [];

  lines.push(...formatBoxHeader('PORTFOLIO REVIEW'));
  lines.push('');

  if (reviews.length === 0) {
    lines.push('  No open positions found.');
    return lines.join('\n');
  }

  const sells = reviews.filter((r) => r.signal === 'SELL');
  const holds = reviews.filter((r) => r.signal === 'HOLD');

  lines.push(`  ${reviews.length} position${reviews.length === 1 ? '' : 's'} analyzed  |  ${sells.length} SELL signal${sells.length === 1 ? '' : 's'}  |  ${holds.length} HOLD`);
  lines.push('');

  // Show SELL signals first
  for (const r of sells) {
    const dirLabel = r.direction.toUpperCase();
    const edgePp = `${r.edge >= 0 ? '+' : ''}${(r.edge * 100).toFixed(0)}pp`;
    lines.push(`  ⚠  ${r.ticker}  ${dirLabel} ×${r.size}`);
    lines.push(`     Edge: ${edgePp}  |  ${r.reason}`);
    lines.push(`     → SELL ${dirLabel} @ ${r.closePriceCents}¢`);
    lines.push(`     Command: /sell ${r.ticker} ${r.size} ${r.closePriceCents} ${r.direction}`);
    if (r.analyzeError) {
      lines.push(`     ⚠ Analysis error: ${r.analyzeError}`);
    }
    lines.push('');
  }

  // Show HOLD positions
  for (const r of holds) {
    const dirLabel = r.direction.toUpperCase();
    const edgePp = `${r.edge >= 0 ? '+' : ''}${(r.edge * 100).toFixed(0)}pp`;
    lines.push(`  ✓  ${r.ticker}  ${dirLabel} ×${r.size}`);
    lines.push(`     Edge: ${edgePp}  |  ${r.reason}`);
    if (r.analyzeError) {
      lines.push(`     ⚠ Analysis error: ${r.analyzeError}`);
    }
    lines.push('');
  }

  if (sells.length > 0) {
    lines.push(`  Run the commands above to close flagged positions, or use /analyze <ticker> for details.`);
  } else {
    lines.push('  All positions are within acceptable edge range. No closes recommended.');
  }

  return lines.join('\n');
}
