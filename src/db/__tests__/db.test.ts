import { describe, test, expect, beforeEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import { createDb } from '../index.js';
import { upsertTheme, getActiveThemes, getThemeTickers } from '../themes.js';
import { upsertEvent, getActiveEvents, deactivateExpired } from '../events.js';
import { insertEdge, getLatestEdge, getEdgeHistory, getActionableEdges } from '../edge.js';
import { insertReport, getReport, getLatestReport, isStale } from '../octagon-cache.js';
import { openPosition, closePosition, getOpenPositions, getPositionWithEdge } from '../positions.js';
import { logTrade, getTradesForPosition, getRecentTrades } from '../trades.js';
import { insertRiskSnapshot, getLatestSnapshot, getDrawdownHistory } from '../risk.js';
import { createAlert, getPendingAlerts, markAlertSent } from '../alerts.js';
import { searchEventIndex, getEventsFromIndex } from '../event-index.js';

let db: Database;

beforeEach(() => {
  db = createDb(':memory:');
});

describe('themes', () => {
  test('upsert and read back', () => {
    upsertTheme(db, {
      theme_id: 'crypto',
      name: 'Crypto Markets',
      tickers: JSON.stringify(['KXBTC-26MAR', 'KXETH-26MAR']),
      last_resolved_at: 1710800000,
    });

    const themes = getActiveThemes(db);
    expect(themes).toHaveLength(1);
    expect(themes[0]!.theme_id).toBe('crypto');
    expect(themes[0]!.name).toBe('Crypto Markets');
  });

  test('getThemeTickers parses JSON', () => {
    upsertTheme(db, {
      theme_id: 'crypto',
      name: 'Crypto',
      tickers: JSON.stringify(['KXBTC-26MAR', 'KXETH-26MAR']),
    });

    const tickers = getThemeTickers(db, 'crypto');
    expect(tickers).toEqual(['KXBTC-26MAR', 'KXETH-26MAR']);
  });

  test('getThemeTickers returns empty for null tickers', () => {
    upsertTheme(db, { theme_id: 'empty', name: 'Empty' });
    expect(getThemeTickers(db, 'empty')).toEqual([]);
  });
});

describe('events', () => {
  test('upsert and read active events', () => {
    upsertEvent(db, {
      ticker: 'KXBTC-26MAR',
      category: 'Crypto',
      expiry: 1711500000,
      vol_24h: 50000,
      active: 1,
      updated_at: 1710800000,
    });

    const events = getActiveEvents(db);
    expect(events).toHaveLength(1);
    expect(events[0]!.ticker).toBe('KXBTC-26MAR');
  });

  test('deactivateExpired marks old events inactive', () => {
    upsertEvent(db, { ticker: 'OLD', expiry: 1000, active: 1 });
    upsertEvent(db, { ticker: 'NEW', expiry: 9999999999, active: 1 });

    const changed = deactivateExpired(db, 2000);
    expect(changed).toBe(1);

    const active = getActiveEvents(db);
    expect(active).toHaveLength(1);
    expect(active[0]!.ticker).toBe('NEW');
  });
});

describe('edge_history', () => {
  test('insert and getLatestEdge', () => {
    insertEdge(db, {
      ticker: 'KXBTC-26MAR-B80000',
      event_ticker: 'KXBTC-26MAR',
      timestamp: 1710800000,
      model_prob: 0.72,
      market_prob: 0.58,
      edge: 0.14,
      confidence: 'very_high',
    });

    const latest = getLatestEdge(db, 'KXBTC-26MAR-B80000');
    expect(latest).not.toBeNull();
    expect(latest!.model_prob).toBe(0.72);
    expect(latest!.market_prob).toBe(0.58);
    expect(latest!.edge).toBe(0.14);
    expect(latest!.confidence).toBe('very_high');
  });

  test('getEdgeHistory returns edges since timestamp', () => {
    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 100,
      model_prob: 0.5, market_prob: 0.4, edge: 0.1, confidence: 'high',
    });
    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 200,
      model_prob: 0.6, market_prob: 0.4, edge: 0.2, confidence: 'very_high',
    });
    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 300,
      model_prob: 0.55, market_prob: 0.4, edge: 0.15, confidence: 'high',
    });

    const history = getEdgeHistory(db, 'T1', 200);
    expect(history).toHaveLength(2);
    expect(history[0]!.timestamp).toBe(200);
    expect(history[1]!.timestamp).toBe(300);
  });

  test('getActionableEdges returns only edges above confidence threshold', () => {
    insertEdge(db, {
      ticker: 'LOW', event_ticker: 'E1', timestamp: 100,
      model_prob: 0.5, market_prob: 0.48, edge: 0.02, confidence: 'low',
    });
    insertEdge(db, {
      ticker: 'MOD', event_ticker: 'E2', timestamp: 100,
      model_prob: 0.6, market_prob: 0.55, edge: 0.05, confidence: 'moderate',
    });
    insertEdge(db, {
      ticker: 'HIGH', event_ticker: 'E3', timestamp: 100,
      model_prob: 0.7, market_prob: 0.6, edge: 0.10, confidence: 'high',
    });
    insertEdge(db, {
      ticker: 'VHIGH', event_ticker: 'E4', timestamp: 100,
      model_prob: 0.8, market_prob: 0.6, edge: 0.20, confidence: 'very_high',
    });

    const highAndAbove = getActionableEdges(db, 'high');
    expect(highAndAbove).toHaveLength(2);
    const tickers = highAndAbove.map((e) => e.ticker).sort();
    expect(tickers).toEqual(['HIGH', 'VHIGH']);

    const veryHighOnly = getActionableEdges(db, 'very_high');
    expect(veryHighOnly).toHaveLength(1);
    expect(veryHighOnly[0]!.ticker).toBe('VHIGH');

    const all = getActionableEdges(db, 'low');
    expect(all).toHaveLength(4);
  });

  test('getActionableEdges returns latest edge per ticker', () => {
    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 100,
      model_prob: 0.5, market_prob: 0.4, edge: 0.1, confidence: 'high',
    });
    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 200,
      model_prob: 0.6, market_prob: 0.4, edge: 0.2, confidence: 'low',
    });

    // Latest edge for T1 is 'low' confidence, so filtering by 'high' should exclude it
    const highEdges = getActionableEdges(db, 'high');
    expect(highEdges).toHaveLength(0);
  });
});

describe('octagon_reports', () => {
  const report = {
    report_id: 'rpt-001',
    ticker: 'KXBTC-26MAR-B80000',
    event_ticker: 'KXBTC-26MAR',
    model_prob: 0.72,
    market_prob: 0.58,
    mispricing_signal: 'underpriced',
    variant_used: 'default',
    fetched_at: 1710800000,
    expires_at: 1710800000 + 86400,
  };

  test('insert and getReport', () => {
    insertReport(db, report);
    const fetched = getReport(db, 'rpt-001');
    expect(fetched).not.toBeNull();
    expect(fetched!.model_prob).toBe(0.72);
    expect(fetched!.mispricing_signal).toBe('underpriced');
  });

  test('getLatestReport returns most recent', () => {
    insertReport(db, report);
    insertReport(db, {
      ...report,
      report_id: 'rpt-002',
      fetched_at: 1710900000,
      expires_at: 1710900000 + 86400,
    });

    const latest = getLatestReport(db, 'KXBTC-26MAR-B80000');
    expect(latest!.report_id).toBe('rpt-002');
  });

  test('isStale returns true for reports older than 24h', () => {
    insertReport(db, {
      ...report,
      fetched_at: 1710800000,
      expires_at: 1710800000 + 86400,
    });

    // 25 hours later
    const future = 1710800000 + 90000;
    expect(isStale(db, 'KXBTC-26MAR-B80000', future)).toBe(true);
  });

  test('isStale returns false for fresh reports', () => {
    insertReport(db, {
      ...report,
      fetched_at: 1710800000,
      expires_at: 1710800000 + 86400,
    });

    // 1 hour later
    const nearFuture = 1710800000 + 3600;
    expect(isStale(db, 'KXBTC-26MAR-B80000', nearFuture)).toBe(false);
  });

  test('isStale returns true when no report exists', () => {
    expect(isStale(db, 'NONEXISTENT', 1710800000)).toBe(true);
  });
});

describe('positions', () => {
  test('open, read, and close position', () => {
    openPosition(db, {
      position_id: 'pos-001',
      ticker: 'KXBTC-26MAR-B80000',
      event_ticker: 'KXBTC-26MAR',
      direction: 'YES',
      size: 45.2,
      entry_price: 0.58,
      entry_edge: 0.14,
      entry_kelly: 0.5,
      opened_at: 1710800000,
    });

    const open = getOpenPositions(db);
    expect(open).toHaveLength(1);
    expect(open[0]!.direction).toBe('YES');
    expect(open[0]!.size).toBe(45.2);

    closePosition(db, 'pos-001', 1710900000);
    expect(getOpenPositions(db)).toHaveLength(0);
  });

  test('getPositionWithEdge joins latest edge', () => {
    openPosition(db, {
      position_id: 'pos-002',
      ticker: 'T1',
      event_ticker: 'E1',
      direction: 'YES',
      size: 10,
      entry_price: 0.50,
      opened_at: 1710800000,
    });

    insertEdge(db, {
      ticker: 'T1', event_ticker: 'E1', timestamp: 1710800000,
      model_prob: 0.6, market_prob: 0.5, edge: 0.1, confidence: 'high',
    });

    const result = getPositionWithEdge(db, 'pos-002');
    expect(result).not.toBeNull();
    expect(result!.position_id).toBe('pos-002');
    expect(result!.latest_edge).not.toBeNull();
    expect(result!.latest_edge!.edge).toBe(0.1);
  });
});

describe('trades', () => {
  test('logTrade and read back', () => {
    // Create referenced position first (FK constraint)
    openPosition(db, {
      position_id: 'pos-001',
      ticker: 'KXBTC-26MAR-B80000',
      event_ticker: 'KXBTC-26MAR',
      direction: 'YES',
      size: 45.2,
      entry_price: 0.58,
      opened_at: 1710800000,
    });

    logTrade(db, {
      trade_id: 'trd-001',
      position_id: 'pos-001',
      order_id: 'kalshi-abc',
      ticker: 'KXBTC-26MAR-B80000',
      action: 'buy',
      side: 'yes',
      size: 45.2,
      price: 0.58,
      fill_status: 'filled',
      created_at: 1710800000,
    });

    const trades = getTradesForPosition(db, 'pos-001');
    expect(trades).toHaveLength(1);
    expect(trades[0]!.price).toBe(0.58);
    expect(trades[0]!.fill_status).toBe('filled');
  });

  test('getRecentTrades respects limit', () => {
    logTrade(db, {
      trade_id: 'a', ticker: 'T1', action: 'buy', side: 'yes',
      size: 1, price: 0.5, created_at: 100,
    });
    logTrade(db, {
      trade_id: 'b', ticker: 'T2', action: 'sell', side: 'no',
      size: 2, price: 0.6, created_at: 200,
    });
    logTrade(db, {
      trade_id: 'c', ticker: 'T3', action: 'buy', side: 'yes',
      size: 3, price: 0.7, created_at: 300,
    });

    const recent = getRecentTrades(db, 2);
    expect(recent).toHaveLength(2);
    expect(recent[0]!.trade_id).toBe('c');
    expect(recent[1]!.trade_id).toBe('b');
  });
});

describe('risk_snapshots', () => {
  test('insert and getLatestSnapshot', () => {
    insertRiskSnapshot(db, {
      timestamp: 1710800000,
      cash_balance: 5000,
      portfolio_value: 7500,
      open_exposure: 2500,
      available_bankroll: 2500,
      daily_pnl: 150,
      drawdown_current: 0.03,
      drawdown_max: 0.15,
      positions_count: 5,
    });

    const latest = getLatestSnapshot(db);
    expect(latest).not.toBeNull();
    expect(latest!.cash_balance).toBe(5000);
    expect(latest!.drawdown_current).toBe(0.03);
  });

  test('getDrawdownHistory returns snapshots since timestamp', () => {
    insertRiskSnapshot(db, { timestamp: 100, drawdown_current: 0.01 });
    insertRiskSnapshot(db, { timestamp: 200, drawdown_current: 0.05 });
    insertRiskSnapshot(db, { timestamp: 300, drawdown_current: 0.03 });

    const history = getDrawdownHistory(db, 200);
    expect(history).toHaveLength(2);
    expect(history[0]!.drawdown_current).toBe(0.05);
    expect(history[1]!.drawdown_current).toBe(0.03);
  });
});

describe('alerts', () => {
  test('create, read pending, and mark sent', () => {
    createAlert(db, {
      alert_id: 'alert-001',
      ticker: 'KXBTC-26MAR-B80000',
      alert_type: 'edge_detected',
      edge: 0.14,
      message: 'High edge detected on BTC 80k',
      channels: JSON.stringify(['terminal', 'whatsapp']),
      created_at: 1710800000,
    });

    const pending = getPendingAlerts(db);
    expect(pending).toHaveLength(1);
    expect(pending[0]!.alert_type).toBe('edge_detected');
    expect(pending[0]!.message).toBe('High edge detected on BTC 80k');

    markAlertSent(db, 'alert-001');
    expect(getPendingAlerts(db)).toHaveLength(0);
  });
});

describe('event_index search', () => {
  const insertIndexed = (
    db: Database,
    row: { event_ticker: string; title: string; markets: Array<Record<string, unknown>> },
  ) => {
    db.query(
      `INSERT INTO event_index (event_ticker, series_ticker, title, category, strike_date, sub_title, tags, markets_json, indexed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(row.event_ticker, null, row.title, null, null, null, null, JSON.stringify(row.markets), Date.now());
  };

  const yesterday = () => new Date(Date.now() - 86400000).toISOString();
  const tomorrow = () => new Date(Date.now() + 86400000).toISOString();

  test('defaults to active-only events and strips expired markets', () => {
    insertIndexed(db, {
      event_ticker: 'EXPIRED',
      title: 'Bitcoin expired event',
      markets: [
        { ticker: 'X-1', status: 'finalized', volume: 100, close_time: yesterday() },
        { ticker: 'X-2', status: 'inactive', volume: 50 },
      ],
    });
    insertIndexed(db, {
      event_ticker: 'STALE',
      title: 'Bitcoin stale event',
      markets: [{ ticker: 'S-1', status: 'active', volume: 200, close_time: yesterday() }],
    });
    insertIndexed(db, {
      event_ticker: 'MIXED',
      title: 'Bitcoin mixed event',
      markets: [
        { ticker: 'M-1', status: 'active', volume: 100, close_time: tomorrow() },
        { ticker: 'M-2', status: 'finalized', volume: 50, close_time: yesterday() },
      ],
    });
    insertIndexed(db, {
      event_ticker: 'LIVE',
      title: 'Bitcoin live event',
      markets: [
        { ticker: 'L-1', status: 'active', volume: 500, close_time: tomorrow() },
        { ticker: 'L-2', status: 'open', volume: 300, close_time: tomorrow() },
      ],
    });

    const active = searchEventIndex(db, 'bitcoin', 50);
    const tickers = active.map((r) => r.event_ticker).sort();
    expect(tickers).toEqual(['LIVE', 'MIXED']);

    const mixed = active.find((r) => r.event_ticker === 'MIXED');
    const mixedMarkets = JSON.parse(mixed!.markets_json!);
    expect(mixedMarkets).toHaveLength(1);
    expect(mixedMarkets[0].ticker).toBe('M-1');
  });

  test('includeExpired returns all matching events with all markets', () => {
    insertIndexed(db, {
      event_ticker: 'EXPIRED',
      title: 'Bitcoin expired event',
      markets: [{ ticker: 'X-1', status: 'finalized', volume: 100, close_time: yesterday() }],
    });
    insertIndexed(db, {
      event_ticker: 'LIVE',
      title: 'Bitcoin live event',
      markets: [{ ticker: 'L-1', status: 'active', volume: 500, close_time: tomorrow() }],
    });

    const all = searchEventIndex(db, 'bitcoin', 50, { includeExpired: true });
    const tickers = all.map((r) => r.event_ticker).sort();
    expect(tickers).toEqual(['EXPIRED', 'LIVE']);

    const expired = all.find((r) => r.event_ticker === 'EXPIRED');
    expect(JSON.parse(expired!.markets_json!)).toHaveLength(1);
  });

  test('getEventsFromIndex strips expired markets by default', () => {
    insertIndexed(db, {
      event_ticker: 'MIXED',
      title: 'Mixed',
      markets: [
        { ticker: 'M-1', status: 'active', volume: 100, close_time: tomorrow() },
        { ticker: 'M-2', status: 'finalized', volume: 50, close_time: yesterday() },
      ],
    });

    const [event] = getEventsFromIndex(db, ['MIXED']);
    const markets = (event!.markets ?? []) as unknown as Array<Record<string, unknown>>;
    expect(markets).toHaveLength(1);
    expect(markets[0]!.ticker).toBe('M-1');

    const [allEvent] = getEventsFromIndex(db, ['MIXED'], { includeExpired: true });
    expect(allEvent!.markets).toHaveLength(2);
  });
});
