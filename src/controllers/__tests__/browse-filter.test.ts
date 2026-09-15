import { describe, test, expect } from 'bun:test';
import { isMarketActive, type MarketRow } from '../browse.js';

function market(over: Partial<MarketRow> = {}): MarketRow {
  return { status: 'active', result: '', volume_24h: 1000, last_price: 50, ...over } as MarketRow;
}

describe('isMarketActive', () => {
  test('a quiet market with lifetime volume is still tradeable', () => {
    // The regression this guards: requiring volume_24h > 0 hid 96.9% of the
    // index — 26,980 markets with real lifetime volume but a quiet 24h, which
    // is normal for long-dated contracts (KXELONMARS-99, $1.7k lifetime).
    expect(isMarketActive(market({ volume_24h: 0, volume: '1730.18' } as Partial<MarketRow>))).toBe(true);
  });

  test('a market that has never traded is still tradeable', () => {
    // Zero last_price means "no trades yet", not "not open for business".
    expect(isMarketActive(market({ volume_24h: 0, last_price: 0, last_price_dollars: null }))).toBe(true);
  });

  test('volume filtering is opt-in, not implicit', () => {
    // Nothing here inspects volume at all — that belongs to --min-volume.
    expect(isMarketActive(market({ volume_24h: 0 }))).toBe(true);
    expect(isMarketActive(market({ volume_24h: 5_000_000 }))).toBe(true);
  });

  test('non-tradeable states are excluded', () => {
    expect(isMarketActive(market({ status: 'settled' }))).toBe(false);
    expect(isMarketActive(market({ status: 'closed' }))).toBe(false);
    expect(isMarketActive(market({ status: 'finalized' }))).toBe(false);
  });

  test('a resolved market is excluded', () => {
    expect(isMarketActive(market({ result: 'yes' }))).toBe(false);
    expect(isMarketActive(market({ result: 'no' }))).toBe(false);
  });

  test('open and active both count as tradeable', () => {
    expect(isMarketActive(market({ status: 'open' }))).toBe(true);
    expect(isMarketActive(market({ status: 'active' }))).toBe(true);
  });
});
