import type { ParsedArgs } from './parse-args.js';
import { formatBoxHeader } from './formatters.js';
import type { CLIResponse } from './json.js';
import { wrapSuccess } from './json.js';
import { getDb } from '../db/index.js';
import { getOpenPositions, getPositionWithEdge } from '../db/positions.js';
import type { PositionWithEdge } from '../db/positions.js';
import { fetchLiveBankroll } from '../risk/kelly.js';
import { getLatestSnapshot } from '../db/risk.js';
import type { RiskSnapshot } from '../db/risk.js';
import { formatTable } from './scan-formatters.js';
import { computePerformance } from '../eval/performance.js';
import type { PerformanceStats } from '../eval/performance.js';

export type { PerformanceStats };

export interface PositionView {
  ticker: string;
  direction: string;
  size: number;
  entryPrice: number;
  entryEdge: number | null;
  currentEdge: number | null;
  unrealizedPnl: number | null;
  watchdogStatus: string;
}

export interface PortfolioData {
  positions: PositionView[];
  accountSummary: {
    cashBalance: number;
    portfolioValue: number;
    openExposure: number;
    available: number;
    positionsCount: number;
  } | null;
  riskSnapshot: RiskSnapshot | null;
  performance?: PerformanceStats;
}

function deriveWatchdogStatus(pos: PositionWithEdge): string {
  if (!pos.latest_edge) return 'unknown';
  const entryEdge = pos.entry_edge ?? 0;
  const currentEdge = pos.latest_edge.edge;

  // Edge converging toward zero or flipped
  if (Math.sign(entryEdge) !== Math.sign(currentEdge) && entryEdge !== 0) return 'adverse_move';
  if (Math.abs(currentEdge) < Math.abs(entryEdge) * 0.5) return 'converging';
  return 'stable';
}

export async function handlePortfolio(args: ParsedArgs): Promise<CLIResponse<PortfolioData>> {
  const db = getDb();
  const warnings: string[] = [];

  // Get open positions with edge data
  let positionViews: PositionView[] = [];
  try {
    const openPositions = getOpenPositions(db);
    const views = openPositions.map((pos) => {
      const withEdge = getPositionWithEdge(db, pos.position_id);
      return {
        ticker: pos.ticker,
        direction: pos.direction,
        size: pos.size,
        entryPrice: pos.entry_price,
        entryEdge: pos.entry_edge ?? null,
        currentEdge: withEdge?.latest_edge?.edge ?? null,
        unrealizedPnl: pos.current_pnl ?? null,
        watchdogStatus: withEdge ? deriveWatchdogStatus(withEdge) : 'unknown',
      };
    });
    positionViews = views;
  } catch (err) {
    warnings.push(`Positions unavailable: ${err instanceof Error ? err.message : String(err)}`);
  }

  // Live bankroll — degrade gracefully on API failure (e.g. 503)
  let bankroll: Awaited<ReturnType<typeof fetchLiveBankroll>> | null = null;
  try {
    bankroll = await fetchLiveBankroll();
  } catch (err) {
    warnings.push(`Balance unavailable: ${err instanceof Error ? err.message : String(err)}`);
  }

  // Risk snapshot
  let riskSnapshot: RiskSnapshot | null = null;
  try {
    riskSnapshot = getLatestSnapshot(db);
  } catch (err) {
    warnings.push(`Risk snapshot unavailable: ${err instanceof Error ? err.message : String(err)}`);
  }

  const data: PortfolioData = {
    positions: positionViews,
    accountSummary: bankroll
      ? {
          cashBalance: bankroll.cashBalance,
          portfolioValue: bankroll.portfolioValue,
          openExposure: bankroll.openExposure,
          available: bankroll.availableBankroll,
          // From the venue, like the dollar lines above it: the table counts only
          // positions this CLI opened, so it misses ones opened on kalshi.com.
          positionsCount: bankroll.openPositions,
        }
      : null,
    riskSnapshot,
  };

  // Performance stats if requested
  if (args.performance) {
    try {
      data.performance = computePerformance(db);
    } catch (err) {
      warnings.push(`Performance unavailable: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  const meta: CLIResponse<PortfolioData>['meta'] = {
    ...(bankroll
      ? {
          bankroll: {
            cash_balance: bankroll.cashBalance,
            portfolio_value: bankroll.portfolioValue,
            open_exposure: bankroll.openExposure,
            available: bankroll.availableBankroll,
            positions_count: bankroll.openPositions,
          },
        }
      : {}),
    ...(warnings.length > 0 ? { warnings } : {}),
  };

  return wrapSuccess('portfolio', data, meta);
}

export function formatPortfolioHuman(data: PortfolioData): string {
  const lines: string[] = [];

  lines.push(...formatBoxHeader('PORTFOLIO'));
  lines.push('');

  // Positions table
  if (data.positions.length === 0) {
    lines.push('  No open positions.');
  } else {
    const rows = data.positions.map((p) => [
      p.ticker,
      p.direction.toUpperCase(),
      String(p.size),
      `$${(p.entryPrice / 100).toFixed(2)}`,
      p.currentEdge !== null ? `${(p.currentEdge * 100).toFixed(1)}%` : '-',
      p.unrealizedPnl !== null ? `$${(p.unrealizedPnl / 100).toFixed(2)}` : '-',
      p.watchdogStatus,
    ]);
    lines.push(formatTable(
      ['Ticker', 'Dir', 'Size', 'Entry', 'Current Edge', 'P&L', 'Status'],
      rows
    ));
  }
  lines.push('');

  // Account summary
  lines.push('  Account Summary:');
  if (data.accountSummary) {
    lines.push(`    Cash Balance:    $${(data.accountSummary.cashBalance / 100).toFixed(2)}`);
    lines.push(`    Portfolio Value: $${(data.accountSummary.portfolioValue / 100).toFixed(2)}`);
    lines.push(`    Open Exposure:   $${(data.accountSummary.openExposure / 100).toFixed(2)}`);
    lines.push(`    Available:       $${(data.accountSummary.available / 100).toFixed(2)}`);
    lines.push(`    Positions:       ${data.accountSummary.positionsCount}`);
  } else {
    lines.push('    (unavailable — Kalshi API returned an error)');
  }
  lines.push('');

  // Risk snapshot
  if (data.riskSnapshot) {
    const snap = data.riskSnapshot;
    lines.push('  Risk Snapshot:');
    if (snap.drawdown_current != null) {
      lines.push(`    Current Drawdown: ${(snap.drawdown_current * 100).toFixed(1)}%`);
    }
    if (snap.drawdown_max != null) {
      lines.push(`    Max Drawdown:     ${(snap.drawdown_max * 100).toFixed(1)}%`);
    }
    if (snap.daily_pnl != null) {
      lines.push(`    Daily P&L:        $${(snap.daily_pnl / 100).toFixed(2)}`);
    }
    if (snap.circuit_breaker_on) {
      lines.push('    Circuit Breaker:  ACTIVE');
    }
    lines.push('');
  }

  // Performance stats
  if (data.performance) {
    const perf = data.performance;
    lines.push('  Performance:');
    if (perf.winRate !== null) {
      lines.push(`    Win Rate:    ${(perf.winRate * 100).toFixed(1)}%`);
    }
    lines.push(`    Total P&L:   $${(perf.totalPnl / 100).toFixed(2)}`);
    if (perf.sharpeRatio !== null) {
      lines.push(`    Sharpe Ratio: ${perf.sharpeRatio.toFixed(2)}`);
    }

    const brierEntries = Object.entries(perf.brierByCategory);
    if (brierEntries.length > 0) {
      lines.push('');
      lines.push('  Brier Scores by Category:');
      const brierRows = brierEntries.map(([cat, score]) => [cat, score.toFixed(3)]);
      lines.push(formatTable(['Category', 'Brier Score'], brierRows));
    }

    const pnlEntries = Object.entries(perf.pnlByCategory);
    if (pnlEntries.length > 0) {
      lines.push('');
      lines.push('  P&L by Category:');
      const pnlRows = pnlEntries.map(([cat, pnl]) => [cat, `$${(pnl / 100).toFixed(2)}`]);
      lines.push(formatTable(['Category', 'P&L'], pnlRows));
    }

    if (perf.underperformingCategories && perf.underperformingCategories.length > 0) {
      lines.push('');
      lines.push('  ⚠ Underperforming Categories (Brier > 0.30):');
      for (const cat of perf.underperformingCategories) {
        const score = perf.brierByCategory[cat];
        lines.push(`    - ${cat}: ${score !== undefined ? score.toFixed(3) : 'N/A'}`);
      }
    }
    lines.push('');
  }

  return lines.join('\n');
}
