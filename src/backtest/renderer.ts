import type { BacktestResult, UnresolvedEdge, ResolvedMarket } from './types.js';
import { writeFileSync } from 'fs';

/**
 * Format the resolved scorecard for terminal display.
 */
function formatResolved(r: NonNullable<BacktestResult['resolved']>, snapshotLabel: string): string {
  const lines: string[] = [];
  lines.push('RESOLVED — Model Scorecard');
  lines.push('──────────────────────────');
  lines.push(`VERDICT: ${r.verdict.summary}`);
  lines.push('');
  lines.push(`  Markets        ${r.markets_evaluated}  (${r.events_evaluated} events)`);
  lines.push(`  Coverage       ${(r.coverage * 100).toFixed(0)}%  of settled Kalshi markets`);
  lines.push(`  Snapshot       ${snapshotLabel}`);
  lines.push('');
  lines.push(`  Brier (Octagon)   ${r.brier_octagon.toFixed(3)}`);
  lines.push(`  Brier (Market)    ${r.brier_market.toFixed(3)}`);
  lines.push(`  Skill Score       ${r.skill_score >= 0 ? '+' : ''}${(r.skill_score * 100).toFixed(1)}%  [95% CI: ${(r.skill_ci[0] * 100).toFixed(1)}% to ${(r.skill_ci[1] * 100).toFixed(1)}%]`);
  lines.push('');
  lines.push(`  Edge signals      ${r.edge_signals}`);
  if (r.edge_signals > 0) {
    lines.push(`  Hit rate          ${(r.edge_hit_rate * 100).toFixed(1)}%   [95% CI: ${(r.hit_rate_ci[0] * 100).toFixed(1)}% to ${(r.hit_rate_ci[1] * 100).toFixed(1)}%]`);
    lines.push(`  Flat-bet P&L      ${r.flat_bet_pnl >= 0 ? '+' : ''}$${r.flat_bet_pnl.toFixed(2)} (ROI: ${r.flat_bet_roi >= 0 ? '+' : ''}${(r.flat_bet_roi * 100).toFixed(1)}%)`);
  }

  // Category breakdown
  const byCat = new Map<string, ResolvedMarket[]>();
  for (const m of r.markets) {
    const cat = m.series_category || 'other';
    const arr = byCat.get(cat) ?? [];
    arr.push(m);
    byCat.set(cat, arr);
  }
  if (byCat.size > 1) {
    lines.push('');
    lines.push('  By category:');
    const sorted = [...byCat.entries()].sort((a, b) => b[1].length - a[1].length);
    for (const [cat, markets] of sorted) {
      const events = new Set(markets.map(m => m.event_ticker)).size;
      lines.push(`    ${cat.padEnd(20)} ${String(markets.length).padStart(4)} markets  (${events} events)`);
    }
  }

  return lines.join('\n');
}

/**
 * Format the unresolved edge scanner for terminal display.
 */
function formatUnresolved(u: NonNullable<BacktestResult['unresolved']>, minEdge: number): string {
  const lines: string[] = [];
  const minPp = (minEdge * 100).toFixed(0);
  lines.push(`UNRESOLVED — Live Edge Scanner (min edge: ${minPp}pp)`);
  lines.push('──────────────────────────────────────────────');

  if (u.edges.length === 0) {
    lines.push('  No markets with edge above threshold.');
    return lines.join('\n');
  }

  // Header
  const header = '  ' + [
    'Ticker'.padEnd(30),
    'Model'.padStart(6),
    'Market'.padStart(7),
    'Edge'.padStart(7),
    'Dir'.padStart(6),
    'Conf'.padStart(6),
    'Closes'.padStart(12),
  ].join('  ');
  lines.push(header);

  for (const e of u.edges) {
    const dir = e.direction === 'YES' ? 'YES ▲' : 'NO  ▼';
    const closes = formatTimeUntil(e.closes_at);
    const confLabel = formatConfidence(e.confidence_score);

    const row = '  ' + [
      e.ticker.padEnd(30),
      `${(e.model_prob * 100).toFixed(0)}%`.padStart(6),
      `${(e.market_prob * 100).toFixed(0)}%`.padStart(7),
      `${e.edge_pp >= 0 ? '+' : ''}${e.edge_pp.toFixed(0)}pp`.padStart(7),
      dir.padStart(6),
      confLabel.padStart(6),
      closes.padStart(12),
    ].join('  ');
    lines.push(row);
  }

  lines.push('');
  lines.push(`  ${u.edges.length} markets with edge ≥ ${minPp}pp (of ${u.total_open_with_coverage} open markets with Octagon coverage)`);

  return lines.join('\n');
}

function formatConfidence(score: number): string {
  if (score >= 8) return 'high';
  if (score >= 5) return 'med';
  return 'low';
}

function formatTimeUntil(isoDate: string): string {
  const diff = new Date(isoDate).getTime() - Date.now();
  if (diff <= 0) return 'closed';
  const days = diff / (1000 * 60 * 60 * 24);
  if (days >= 365) return `${(days / 365).toFixed(1)} yrs`;
  if (days >= 30) return `${Math.round(days)} days`;
  if (days >= 1) return `${Math.round(days)} days`;
  const hours = diff / (1000 * 60 * 60);
  return `${Math.round(hours)}h`;
}

export interface FormatOpts {
  minEdge?: number;          // 0-1 scale, default 0.05
  minHoursBeforeClose?: number;
  snapshotLast?: boolean;
}

/**
 * Format complete backtest result for terminal display.
 */
export function formatBacktestHuman(result: BacktestResult, opts?: FormatOpts): string {
  const minEdge = opts?.minEdge ?? 0.05;
  const snapshotLabel = opts?.snapshotLast ? 'last (no lead time)' : `last-${opts?.minHoursBeforeClose ?? 24}h`;
  const lines: string[] = [];
  lines.push(`Octagon Backtest — ${result.date_range.from} – ${result.date_range.to}`);
  lines.push('═══════════════════════════════════════');
  lines.push('');

  if (result.resolved) {
    lines.push(formatResolved(result.resolved, snapshotLabel));
    lines.push('');
  }

  if (result.unresolved) {
    lines.push(formatUnresolved(result.unresolved, minEdge));
  }

  if (!result.resolved && !result.unresolved) {
    lines.push('No data available. Run `bun start backtest` with a broader filter.');
  }

  return lines.join('\n');
}

/**
 * Export per-market detail to CSV.
 */
export function exportCSV(result: BacktestResult, path: string): void {
  const rows: string[] = [];
  rows.push('type,ticker,event_ticker,model_prob,market_prob,edge_pp,outcome,close_time,series_category');

  if (result.resolved) {
    for (const m of result.resolved.markets) {
      rows.push([
        'resolved',
        m.ticker,
        m.event_ticker,
        m.model_prob.toFixed(4),
        m.market_prob.toFixed(4),
        m.edge_pp.toFixed(1),
        m.outcome,
        m.close_time,
        m.series_category,
      ].join(','));
    }
  }

  if (result.unresolved) {
    for (const e of result.unresolved.edges) {
      rows.push([
        'unresolved',
        e.ticker,
        e.event_ticker,
        e.model_prob.toFixed(4),
        e.market_prob.toFixed(4),
        e.edge_pp.toFixed(1),
        '',
        e.closes_at,
        e.series_category,
      ].join(','));
    }
  }

  writeFileSync(path, rows.join('\n') + '\n');
}
