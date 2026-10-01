import { describe, test, expect, beforeEach, afterEach, mock, spyOn } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { createDb } from '../../db/index.js';
import { openPosition } from '../../db/positions.js';
import { upsertEvent } from '../../db/events.js';
import { insertRiskSnapshot } from '../../db/risk.js';
import type { KalshiMarket } from '../../tools/kalshi/types.js';
import type { KellyResult } from '../kelly.js';
import { kellySize, fetchLiveBankroll } from '../kelly.js';
import { riskGate } from '../gate.js';
import { getCorrelationByCategory, isCorrelated } from '../correlation.js';
import { CircuitBreaker } from '../circuit-breaker.js';
import * as kalshiApi from '../../tools/kalshi/api.js';

// --- Mock callKalshiApi to avoid needing real API keys ---

const mockApiResponses: Record<string, unknown> = {};
let apiSpy: ReturnType<typeof spyOn> | null = null;

function installApiMock() {
  apiSpy = spyOn(kalshiApi, 'callKalshiApi').mockImplementation(async (_method, path) => {
    if (mockApiResponses[path]) {
      return mockApiResponses[path] as any;
    }
    return {} as any;
  });
}

function restoreApiMock() {
  apiSpy?.mockRestore();
  apiSpy = null;
}

// --- Helpers ---

function setMockBankroll(balance: number, payout: number, positions: Array<{ market_exposure: number }>) {
  mockApiResponses['/portfolio/balance'] = { balance, payout, portfolio_value: balance + payout, reserved_fees: 0, fees: 0 };
  mockApiResponses['/portfolio/positions'] = { market_positions: positions };
}

function makeMarket(overrides: Partial<KalshiMarket> = {}): KalshiMarket {
  return {
    ticker: 'MKT-YES',
    event_ticker: 'EV-1',
    market_type: 'binary',
    title: 'Test',
    subtitle: '',
    yes_sub_title: '',
    no_sub_title: '',
    open_time: '',
    close_time: '',
    expected_expiration_time: '',
    expiration_time: '',
    latest_expiration_time: '',
    settlement_timer_seconds: 0,
    status: 'open',
    response_price_units: 'cents',
    notional_value: 100,
    tick_size: 1,
    yes_bid: 50,
    yes_ask: 52,
    no_bid: 48,
    no_ask: 50,
    last_price: 51,
    previous_yes_bid: 50,
    previous_yes_ask: 52,
    previous_price: 51,
    volume: 5000,
    volume_24h: 3000,
    liquidity: 10000,
    open_interest: 2000,
    result: '',
    settlement_value: '',
    can_close_early: false,
    expiration_value: '',
    category: 'politics',
    risk_limit_cents: 100,
    strike_type: 'binary',
    floor_strike: 0,
    cap_strike: 100,
    ...overrides,
  } as KalshiMarket;
}

function insertTestPosition(db: Database, overrides: Record<string, unknown> = {}): void {
  openPosition(db, {
    position_id: (overrides.position_id as string) ?? `pos-${Math.random().toString(36).slice(2)}`,
    ticker: (overrides.ticker as string) ?? 'MKT-YES',
    event_ticker: (overrides.event_ticker as string) ?? 'EV-1',
    direction: (overrides.direction as string) ?? 'buy_yes',
    size: (overrides.size as number) ?? 10,
    entry_price: (overrides.entry_price as number) ?? 55,
    entry_edge: (overrides.entry_edge as number) ?? 0.10,
    status: 'open',
    opened_at: Math.floor(Date.now() / 1000),
  });
}

// --- Tests ---

describe('Kelly Sizing', () => {
  beforeEach(() => {
    for (const key of Object.keys(mockApiResponses)) {
      delete mockApiResponses[key];
    }
    installApiMock();
  });

  afterEach(() => {
    restoreApiMock();
  });

  test('computes correct size with $1000 balance, $300 exposure', async () => {
    // $1000 balance = 100000 cents, $300 exposure = 30000 cents
    setMockBankroll(100000, 0, [
      { market_exposure: 15000 },
      { market_exposure: 15000 },
    ]);

    const result = await kellySize({
      edge: 0.10,
      marketProb: 0.50,
    });

    // available = 100000 - 30000 = 70000
    expect(result.availableBankroll).toBe(70000);
    expect(result.cashBalance).toBe(100000);
    expect(result.openExposure).toBe(30000);
    expect(result.side).toBe('yes');

    // f* = 0.10 / (1 - 0.50) = 0.20
    expect(result.fraction).toBeCloseTo(0.20, 5);

    // half-Kelly: 0.20 * 0.5 = 0.10
    expect(result.adjustedFraction).toBeCloseTo(0.10, 5);

    // dollarAmountCents = floor(0.10 * 70000) = 7000 cents ($70)
    // But capped at maxPositionPct (10%) * 70000 = 7000 → same
    // entryPriceCents = marketProb * 100 = 50 (no market obj, uses midpoint fallback)
    // contracts = floor(7000 / 50) = 140
    expect(result.contracts).toBe(140);
    expect(result.dollarAmountCents).toBe(7000);
    expect(result.liquidityAdjusted).toBe(false);
  });

  test('full-Kelly doubles the size vs half-Kelly', async () => {
    setMockBankroll(100000, 0, [{ market_exposure: 30000 }]);

    const half = await kellySize({ edge: 0.10, marketProb: 0.50, multiplier: 0.5 });
    const full = await kellySize({ edge: 0.10, marketProb: 0.50, multiplier: 1.0 });

    // full-Kelly adjustedFraction should be double half-Kelly
    expect(full.adjustedFraction).toBeCloseTo(half.adjustedFraction * 2, 5);

    // But both are capped at maxPositionPct (10%) of bankroll
    // half: 0.10 * 70000 = 7000, cap = 7000 → 7000
    // full: 0.20 * 70000 = 14000, cap = 7000 → 7000
    // So contracts should be same due to cap
    expect(full.dollarAmountCents).toBeLessThanOrEqual(full.availableBankroll * 0.10);
  });

  test('liquidity adjustment caps size at 50% for wide spread', async () => {
    setMockBankroll(100000, 0, []);

    // Use dollar-string fields for spread (new API format): 7¢ spread > 3¢
    const market = makeMarket({
      yes_bid: 48,
      yes_ask: 55,
      yes_bid_dollars: '0.48',
      yes_ask_dollars: '0.55',
    });

    const result = await kellySize({
      edge: 0.15,
      marketProb: 0.50,
      market,
    });

    expect(result.liquidityAdjusted).toBe(true);
    // With executable quote: edge vs ask (0.55) = (0.50+0.15)-0.55 = 0.10, fraction = 0.10/0.45 ≈ 0.2222
    // adjustedFraction = 0.2222 * 0.5 (half-Kelly) * 0.5 (liquidity) ≈ 0.0556
    expect(result.adjustedFraction).toBeCloseTo(0.0556, 3);
  });

  test('edge below threshold produces 0 contracts with reason', async () => {
    setMockBankroll(100000, 0, []);

    const result = await kellySize({ edge: 0.01, marketProb: 0.50 });

    expect(result.contracts).toBe(0);
    expect(result.skippedReason).toContain('threshold');
  });

  test('zero edge produces 0 contracts', async () => {
    setMockBankroll(100000, 0, []);

    const result = await kellySize({ edge: 0, marketProb: 0.50 });

    expect(result.fraction).toBe(0);
    expect(result.contracts).toBe(0);
    expect(result.dollarAmountCents).toBe(0);
  });

  test('negative edge sizes NO contracts', async () => {
    setMockBankroll(100000, 0, []);

    const result = await kellySize({ edge: -0.10, marketProb: 0.50 });

    expect(result.side).toBe('no');
    // f* = |edge| / marketProb = 0.10 / 0.50 = 0.20
    expect(result.fraction).toBeCloseTo(0.20, 5);
    expect(result.contracts).toBeGreaterThan(0);
  });

  test('small negative edge below threshold produces 0 contracts', async () => {
    setMockBankroll(100000, 0, []);

    const result = await kellySize({ edge: -0.01, marketProb: 0.50 });

    expect(result.side).toBe('no');
    expect(result.contracts).toBe(0);
    expect(result.skippedReason).toContain('threshold');
  });
});

describe('fetchLiveBankroll', () => {
  beforeEach(() => {
    for (const key of Object.keys(mockApiResponses)) {
      delete mockApiResponses[key];
    }
    installApiMock();
  });

  afterEach(() => {
    restoreApiMock();
  });

  test('counts open positions from position_fp, the only count production sends', async () => {
    mockApiResponses['/portfolio/balance'] = { balance: 9862, portfolio_value: 130 };
    mockApiResponses['/portfolio/positions'] = {
      market_positions: [
        { ticker: 'KXOPEN-26', position_fp: '1.00', market_exposure_dollars: '1.30' },
        { ticker: 'KXCLOSED-26', position_fp: '0.00', market_exposure_dollars: '0.00' },
      ],
    };
    const bankroll = await fetchLiveBankroll();
    expect(bankroll.openPositions).toBe(1);
    expect(bankroll.openExposure).toBe(130);
    expect(bankroll.availableBankroll).toBe(9732);
  });
});

describe('Risk Gate', () => {
  let db: Database;

  beforeEach(() => {
    db = createDb(':memory:');
    for (const key of Object.keys(mockApiResponses)) {
      delete mockApiResponses[key];
    }
  });

  function makeKelly(overrides: Partial<KellyResult> = {}): KellyResult {
    return {
      side: 'yes',
      fraction: 0.20,
      adjustedFraction: 0.10,
      contracts: 10,
      dollarAmountCents: 500,
      entryPriceCents: 50,
      availableBankroll: 70000,
      openExposure: 30000,
      cashBalance: 100000,
      portfolioValue: 100000,
      liquidityAdjusted: false,
      ...overrides,
    };
  }

  test('all checks pass for healthy setup', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });

    const result = riskGate({
      ticker: 'MKT-YES',
      eventTicker: 'EV-1',
      kelly: makeKelly(),
      market: makeMarket(),
      db,
    });

    expect(result.passed).toBe(true);
    expect(result.checks).toHaveLength(5);
    expect(result.checks.every((c) => c.passed)).toBe(true);
  });

  test('fails when kelly produces 0 contracts', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });

    const result = riskGate({
      ticker: 'MKT-YES',
      eventTicker: 'EV-1',
      kelly: makeKelly({ contracts: 0, dollarAmountCents: 0 }),
      market: makeMarket(),
      db,
    });

    expect(result.passed).toBe(false);
    const kellyCheck = result.checks.find((c) => c.name === 'kelly');
    expect(kellyCheck!.passed).toBe(false);
  });

  test('fails on wide spread', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });

    const result = riskGate({
      ticker: 'MKT-YES',
      eventTicker: 'EV-1',
      kelly: makeKelly(),
      market: makeMarket({ yes_bid: 45, yes_ask: 55, yes_bid_dollars: '0.45', yes_ask_dollars: '0.55' }), // 10¢ spread
      db,
    });

    expect(result.passed).toBe(false);
    const liqCheck = result.checks.find((c) => c.name === 'liquidity');
    expect(liqCheck!.passed).toBe(false);
  });

  test('fails when too many positions in same category', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-2', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-3', category: 'politics', active: 1 });

    // Open 3 positions in 'politics' category
    insertTestPosition(db, { position_id: 'p1', event_ticker: 'EV-1' });
    insertTestPosition(db, { position_id: 'p2', event_ticker: 'EV-2' });
    insertTestPosition(db, { position_id: 'p3', event_ticker: 'EV-3' });

    const result = riskGate({
      ticker: 'MKT-NEW',
      eventTicker: 'EV-1', // same category
      kelly: makeKelly(),
      market: makeMarket(),
      db,
    });

    expect(result.passed).toBe(false);
    const corrCheck = result.checks.find((c) => c.name === 'correlation');
    expect(corrCheck!.passed).toBe(false);
  });

  test('fails when too many total positions', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });

    // Insert maxTotalPositions positions
    for (let i = 0; i < 10; i++) {
      upsertEvent(db, { ticker: `EV-${i}`, category: `cat-${i}`, active: 1 });
      insertTestPosition(db, { position_id: `pos-${i}`, event_ticker: `EV-${i}` });
    }

    const result = riskGate({
      ticker: 'MKT-NEW',
      eventTicker: 'EV-1',
      kelly: makeKelly(),
      market: makeMarket(),
      db,
    });

    expect(result.passed).toBe(false);
    const concCheck = result.checks.find((c) => c.name === 'concentration');
    expect(concCheck!.passed).toBe(false);
  });

  test('fails when drawdown exceeds limit', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });

    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      drawdown_current: 0.25, // 25% > 20% default max
    });

    const result = riskGate({
      ticker: 'MKT-YES',
      eventTicker: 'EV-1',
      kelly: makeKelly(),
      market: makeMarket(),
      db,
    });

    expect(result.passed).toBe(false);
    const ddCheck = result.checks.find((c) => c.name === 'drawdown');
    expect(ddCheck!.passed).toBe(false);
  });
});

describe('Circuit Breaker', () => {
  let db: Database;

  beforeEach(() => {
    db = createDb(':memory:');
  });

  test('activates on drawdown breach', () => {
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      drawdown_current: 0.25,
      daily_pnl: -1000,
    });

    const cb = new CircuitBreaker({ maxDrawdown: 0.20 });
    const status = cb.check(db);

    expect(status.active).toBe(true);
    expect(status.reason).toContain('Drawdown');
  });

  test('activates when daily loss exceeds limit', () => {
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      drawdown_current: 0.05,
      daily_pnl: -6000, // -$60 > -$50 limit
    });

    const cb = new CircuitBreaker({ dailyLossLimit: 5000 });
    const status = cb.check(db);

    expect(status.active).toBe(true);
    expect(status.reason).toContain('Daily P&L');
  });

  test('inactive when within limits', () => {
    insertRiskSnapshot(db, {
      timestamp: Math.floor(Date.now() / 1000),
      drawdown_current: 0.05,
      daily_pnl: -1000,
    });

    const cb = new CircuitBreaker();
    const status = cb.check(db);

    expect(status.active).toBe(false);
  });

  test('snapshot fetches live data and inserts', async () => {
    const spy = spyOn(kalshiApi, 'callKalshiApi').mockImplementation(async (_method, path) => {
      if (path === '/portfolio/balance') {
        return { balance: 100000, payout: 5000, portfolio_value: 105000, reserved_fees: 0, fees: 0 } as any;
      }
      if (path === '/portfolio/positions') {
        return { market_positions: [{ market_exposure: 20000 }] } as any;
      }
      return {} as any;
    });

    const cb = new CircuitBreaker();
    const snap = await cb.snapshot(db);

    expect(snap.cash_balance).toBe(100000);
    expect(snap.portfolio_value).toBe(105000);
    expect(snap.open_exposure).toBe(20000);
    expect(snap.available_bankroll).toBe(80000);
    expect(snap.drawdown_current).toBeGreaterThanOrEqual(0);

    spy.mockRestore();
  });
});

describe('Correlation', () => {
  let db: Database;

  beforeEach(() => {
    db = createDb(':memory:');
  });

  test('counts positions per category correctly', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-2', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-3', category: 'weather', active: 1 });

    insertTestPosition(db, { position_id: 'p1', event_ticker: 'EV-1' });
    insertTestPosition(db, { position_id: 'p2', event_ticker: 'EV-2' });
    insertTestPosition(db, { position_id: 'p3', event_ticker: 'EV-3' });

    const counts = getCorrelationByCategory(db);

    expect(counts.get('politics')).toBe(2);
    expect(counts.get('weather')).toBe(1);
  });

  test('isCorrelated returns true when at limit', () => {
    upsertEvent(db, { ticker: 'EV-1', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-2', category: 'politics', active: 1 });
    upsertEvent(db, { ticker: 'EV-3', category: 'politics', active: 1 });

    insertTestPosition(db, { position_id: 'p1', event_ticker: 'EV-1' });
    insertTestPosition(db, { position_id: 'p2', event_ticker: 'EV-2' });
    insertTestPosition(db, { position_id: 'p3', event_ticker: 'EV-3' });

    expect(isCorrelated('EV-1', db, 3)).toBe(true);
    expect(isCorrelated('EV-1', db, 5)).toBe(false);
  });

  test('isCorrelated returns false for unknown event', () => {
    expect(isCorrelated('EV-UNKNOWN', db)).toBe(false);
  });
});
