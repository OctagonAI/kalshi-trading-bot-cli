import { describe, expect, test } from 'bun:test';
import { stripVTControlCharacters } from 'node:util';
import { formatPositions } from '../formatters.js';

describe('formatPositions', () => {
  /** A row with only the fields Kalshi's GetPositions docs list for a market position. */
  const row = {
    ticker: 'KXOPEN-26',
    exchange_index: 0,
    total_traded_dollars: '1.30',
    position_fp: '1.00',
    market_exposure_dollars: '1.30',
    realized_pnl_dollars: '0.00',
    fees_paid_dollars: '0.02',
    last_updated_ts: '2026-10-01T22:00:00Z',
  };

  test('renders a production-shaped row', () => {
    const out = stripVTControlCharacters(formatPositions([row]));
    expect(out).toMatch(/│ KXOPEN-26 +│ \+1 +│ \$0\.00 +│ \$1\.30 +│/);
  });

  test('has no Orders column: the API no longer reports resting orders per position', () => {
    const out = stripVTControlCharacters(formatPositions([row]));
    expect(out).toContain('Exposure');
    expect(out).not.toContain('Orders');
  });
});
