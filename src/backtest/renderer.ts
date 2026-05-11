import type { BacktestResult, ScoredSignal } from './types.js';
import { writeFileSync } from 'fs';

export interface FormatOpts {
  minEdge?: number;          // 0-1 scale, default 0.005 (0.5pp)
}

/**
 * Format complete backtest result for terminal display.
 */
export function formatBacktestHuman(result: BacktestResult, opts?: FormatOpts): string {
  const minEdgePp = ((opts?.minEdge ?? 0.005) * 100).toFixed(1);
  const now = new Date();
  const from = new Date(now.getTime() - result.days * 24 * 60 * 60 * 1000);
  const fromStr = from.toISOString().slice(5, 10).replace('-', '/');
  const toStr = now.toISOString().slice(5, 10).replace('-', '/');

  const lines: string[] = [];
  lines.push(`Octagon Backtest — ${result.days}-day lookback (${fromStr} – ${toStr})`);
  lines.push('══════════════════════════════════════════════════════════');
  lines.push('');

  if (result.subscription_notice) {
    lines.push(`  ${result.subscription_notice}`);
    lines.push('');
    // Still show unresolved signals if any
    const unresolvedSignals = result.signals.filter(s => !s.resolved);
    if (unresolvedSignals.length > 0) {
      lines.push(formatUnresolvedTable(unresolvedSignals, minEdgePp));
    }
    return lines.join('\n');
  }

  if (result.signals.length === 0) {
    lines.push('No data available. Try a longer lookback (--days 60) or broader filter.');
    return lines.join('\n');
  }

  // Unified scorecard
  lines.push(`  Events         ${result.events_scored}`);
  lines.push(`  Markets        ${result.markets_resolved + result.markets_unresolved}   (${result.markets_resolved} resolved, ${result.markets_unresolved} unresolved)`);
  lines.push('');
  // Brier scores and Skill Score are hidden for now (keep values in result for JSON/CSV consumers).
  // lines.push(`  Brier (Octagon)   ${result.brier_octagon.toFixed(3)}`);
  // lines.push(`  Brier (Market)    ${result.brier_market.toFixed(3)}`);
  // lines.push(`  Skill Score       ${result.skill_score >= 0 ? '+' : ''}${(result.skill_score * 100).toFixed(1)}%  [95% CI: ${(result.skill_ci[0] * 100).toFixed(1)}% to ${(result.skill_ci[1] * 100).toFixed(1)}%]`);
  // lines.push('');
  lines.push(`  Edge signals      ${result.edge_signals}   (min edge: ${minEdgePp}pp)`);
  if (result.edge_signals > 0) {
    lines.push(`  Hit rate          ${(result.edge_hit_rate * 100).toFixed(1)}%  [95% CI: ${(result.hit_rate_ci[0] * 100).toFixed(1)}% to ${(result.hit_rate_ci[1] * 100).toFixed(1)}%]`);
    lines.push(`  Flat-bet P&L      ${result.flat_bet_pnl >= 0 ? '+' : ''}$${result.flat_bet_pnl.toFixed(2)} (ROI: ${result.flat_bet_roi >= 0 ? '+' : ''}${(result.flat_bet_roi * 100).toFixed(1)}%)`);
    lines.push(`  Capital deployed  $${result.total_capital.toFixed(2)}   (capital-weighted ROI)`);
  }

  // Resolved detail table
  const resolved = result.signals.filter(s => s.resolved);
  if (resolved.length > 0) {
    lines.push('');
    lines.push(formatResolvedTable(resolved));
  }

  // Unresolved detail table
  const unresolved = result.signals.filter(s => !s.resolved);
  if (unresolved.length > 0) {
    lines.push('');
    lines.push(formatUnresolvedTable(unresolved, minEdgePp));
  }

  return lines.join('\n');
}

function formatResolvedTable(signals: ScoredSignal[]): string {
  const lines: string[] = [];
  lines.push(`RESOLVED (${signals.length} markets — scored against Kalshi settlement)`);
  lines.push('─────────────────────────────────────────────────────────');

  const header = '  ' + [
    'Ticker'.padEnd(30),
    'Model'.padStart(6),
    'Mkt Then'.padStart(9),
    'Outcome'.padStart(10),
    'Edge'.padStart(7),
    'Bkt'.padStart(7),
    'P&L'.padStart(8),
    'ROI'.padStart(8),
  ].join('  ');
  lines.push(header);

  // Sort by |P&L| descending
  const sorted = [...signals].sort((a, b) => Math.abs(b.pnl) - Math.abs(a.pnl));
  for (const s of sorted.slice(0, 20)) {
    const outcome = s.market_now === 100 ? 'YES 100%' : 'NO  0%';
    const roi = s.capital > 0 ? (s.pnl / s.capital) * 100 : 0;
    const roiStr = s.capital > 0
      ? `${roi >= 0 ? '+' : ''}${roi.toFixed(1)}%`
      : '—';
    const row = '  ' + [
      s.market_ticker.padEnd(30),
      `${s.model_prob.toFixed(0)}%`.padStart(6),
      `${s.market_then.toFixed(0)}%`.padStart(9),
      outcome.padStart(10),
      `${s.edge_pp >= 0 ? '+' : ''}${s.edge_pp.toFixed(0)}pp`.padStart(7),
      s.edge_bucket.padStart(7),
      `${s.pnl >= 0 ? '+' : ''}$${s.pnl.toFixed(2)}`.padStart(8),
      roiStr.padStart(8),
    ].join('  ');
    lines.push(row);
  }
  if (sorted.length > 20) {
    lines.push(`  ... and ${sorted.length - 20} more`);
  }

  return lines.join('\n');
}

function formatUnresolvedTable(signals: ScoredSignal[], minEdgePp: string): string {
  const lines: string[] = [];
  lines.push(`UNRESOLVED (${signals.length} markets — mark-to-market vs Kalshi trading price)`);
  lines.push('────────────────────────────────────────────────────────────────');

  const header = '  ' + [
    'Ticker'.padEnd(30),
    'Model'.padStart(6),
    'Mkt Then'.padStart(9),
    'Now'.padStart(6),
    'Edge'.padStart(7),
    'Bkt'.padStart(7),
    'M2M'.padStart(8),
    'ROI'.padStart(8),
  ].join('  ');
  lines.push(header);

  // Sort by |edge| descending
  const sorted = [...signals].sort((a, b) => Math.abs(b.edge_pp) - Math.abs(a.edge_pp));
  for (const s of sorted.slice(0, 20)) {
    const roi = s.capital > 0 ? (s.pnl / s.capital) * 100 : 0;
    const roiStr = s.capital > 0
      ? `${roi >= 0 ? '+' : ''}${roi.toFixed(1)}%`
      : '—';
    const row = '  ' + [
      s.market_ticker.padEnd(30),
      `${s.model_prob.toFixed(0)}%`.padStart(6),
      `${s.market_then.toFixed(0)}%`.padStart(9),
      `${s.market_now.toFixed(0)}%`.padStart(6),
      `${s.edge_pp >= 0 ? '+' : ''}${s.edge_pp.toFixed(0)}pp`.padStart(7),
      s.edge_bucket.padStart(7),
      `${s.pnl >= 0 ? '+' : ''}$${s.pnl.toFixed(2)}`.padStart(8),
      roiStr.padStart(8),
    ].join('  ');
    lines.push(row);
  }
  if (sorted.length > 20) {
    lines.push(`  ... and ${sorted.length - 20} more`);
  }

  return lines.join('\n');
}

/** Escape a CSV cell: wrap in quotes if it contains comma, quote, or newline. */
function csvEscape(val: string | number): string {
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/**
 * Export per-market detail to CSV.
 */
export function exportCSV(result: BacktestResult, path: string): void {
  const rows: string[] = [];
  rows.push('type,ticker,event_ticker,series_category,edge_bucket,model_prob,market_then,market_now,edge_pp,pnl,capital,resolved,close_time');

  for (const s of result.signals) {
    rows.push([
      s.resolved ? 'resolved' : 'unresolved',
      csvEscape(s.market_ticker),
      csvEscape(s.event_ticker),
      csvEscape(s.series_category),
      csvEscape(s.edge_bucket),
      s.model_prob.toFixed(1),
      s.market_then.toFixed(1),
      s.market_now.toFixed(1),
      s.edge_pp.toFixed(1),
      s.pnl.toFixed(4),
      s.capital.toFixed(4),
      s.resolved ? '1' : '0',
      csvEscape(s.close_time),
    ].join(','));
  }

  writeFileSync(path, rows.join('\n') + '\n');
}
