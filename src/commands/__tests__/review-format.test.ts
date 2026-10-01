import { describe, expect, test } from 'bun:test';
import { formatReviewHuman, type PositionReview } from '../review.js';

function sell(o: Partial<PositionReview>): PositionReview {
  return {
    ticker: 'KXOPEN-26',
    direction: 'yes',
    size: 3,
    entryPrice: null,
    currentMarketProb: 0.4,
    modelProb: 0.3,
    edge: -0.1,
    signal: 'SELL',
    sellSide: 'yes',
    closePriceCents: 39,
    reason: 'Edge reversed',
    ...o,
  };
}

describe('formatReviewHuman — close command', () => {
  test('a whole position gets a runnable /sell command', () => {
    const out = formatReviewHuman([sell({ size: 3 })]);
    expect(out).toContain('YES ×3');
    expect(out).toContain('Command: /sell KXOPEN-26 3 39 yes');
  });

  test('a fractional position shows its exact size and is not rounded into /sell', () => {
    const out = formatReviewHuman([sell({ size: 0.4, direction: 'no', sellSide: 'no' })]);
    expect(out).toContain('NO ×0.4');
    expect(out).not.toContain('Command: /sell');
    expect(out).toContain('Command: kalshi analyze KXOPEN-26');
  });
});
