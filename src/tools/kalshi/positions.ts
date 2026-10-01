import type { KalshiPosition } from './types.js';

/**
 * Net contracts held in a market: positive is YES, negative is NO.
 *
 * GET /portfolio/positions now sends only `position_fp`, a fixed-point string
 * ("10.00"). Reading the legacy `position` alone gives 0 for every row, so
 * open positions look closed. Falls back to `position` for environments that
 * still send it.
 */
export function netPosition(p: Pick<KalshiPosition, 'position' | 'position_fp'>): number {
  const n = parseFloat(String(p.position_fp ?? p.position ?? '0'));
  return Number.isFinite(n) ? n : 0;
}

/**
 * The side and size held in a market, or null when flat. Size keeps fractional
 * contracts: Kalshi counts in 0.01 steps, and rounding would show a 0.40
 * position as ×0 and size a close of a 0.60 position at 1 contract.
 */
export function heldPosition(
  p: Pick<KalshiPosition, 'position' | 'position_fp'>,
): { direction: 'yes' | 'no'; size: number } | null {
  const net = netPosition(p);
  if (net === 0) return null;
  return { direction: net > 0 ? 'yes' : 'no', size: Math.abs(net) };
}
