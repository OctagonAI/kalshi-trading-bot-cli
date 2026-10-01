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
