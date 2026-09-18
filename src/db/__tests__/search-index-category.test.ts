import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import type { Database } from 'bun:sqlite';
import type { KalshiMarket } from '../../tools/kalshi/types.js';
import { createDb } from '../index.js';
import { searchEventIndex, upsertIndexEvents } from '../event-index.js';

interface Seed {
  ticker: string;
  title: string;
  category?: string;
  tags?: string;
  volume?: number;
  closed?: boolean;
  result?: string;
}

function seed(db: Database, rows: Seed[]): void {
  upsertIndexEvents(
    db,
    rows.map((r) => ({
      event_ticker: r.ticker,
      title: r.title,
      series_ticker: 'KXTEST',
      category: r.category,
      markets: [
        {
          ticker: `${r.ticker}-T1`,
          title: r.title,
          yes_sub_title: 'yes',
          status: r.closed ? 'closed' : 'active',
          close_time: '2099-01-01T00:00:00Z',
          result: r.result ?? '',
          volume: r.volume ?? 100,
        } as unknown as KalshiMarket,
      ],
    })),
  );
  // upsertIndexEvents deliberately never writes tags (a separate series pass owns
  // them), so set them directly the way that pass does.
  for (const r of rows) {
    if (r.tags) db.query('UPDATE event_index SET tags = ? WHERE event_ticker = ?').run(r.tags, r.ticker);
  }
}

describe('searchEventIndex categoryLabels', () => {
  let db: Database;
  beforeEach(() => { db = createDb(':memory:'); });
  afterEach(() => { db.close(); });

  test('a bare theme searches by category with no keyword', () => {
    // The regression this guards: the CLI passed `crypto:btc` as one keyword and
    // matched nothing, while the TUI filtered by category and searched within it.
    seed(db, [
      { ticker: 'KXBTC', title: 'Bitcoin above 100k', category: 'Crypto' },
      { ticker: 'KXSEN', title: 'Senate control', category: 'Politics' },
    ]);
    const rows = searchEventIndex(db, '', 50, { categoryLabels: ['Crypto'] });
    expect(rows.map((r) => r.event_ticker)).toEqual(['KXBTC']);
  });

  test('a subtheme narrows within the category and does not escape it', () => {
    seed(db, [
      { ticker: 'KXBTC', title: 'Bitcoin above 100k', category: 'Crypto' },
      { ticker: 'KXETH', title: 'Ethereum above 5k', category: 'Crypto' },
      { ticker: 'KXBAN', title: 'Bitcoin ban passes', category: 'Politics' },
    ]);
    const rows = searchEventIndex(db, 'bitcoin', 50, { categoryLabels: ['Crypto'] });
    expect(rows.map((r) => r.event_ticker)).toEqual(['KXBTC']);
  });

  test('a label matches a whole tag as well as the category column', () => {
    seed(db, [{ ticker: 'KXTAG', title: 'Tagged only', category: 'Other', tags: 'Bitcoin,Crypto' }]);
    expect(searchEventIndex(db, '', 50, { categoryLabels: ['Crypto'] }).map((r) => r.event_ticker))
      .toEqual(['KXTAG']);
  });

  test('a label does not match a longer tag that merely starts with it', () => {
    // Comma-wrapping is what stops "Tech" hitting "Tech Stocks".
    seed(db, [{ ticker: 'KXPART', title: 'Partial tag', category: 'Other', tags: 'Crypto Prices' }]);
    expect(searchEventIndex(db, '', 50, { categoryLabels: ['Crypto'] })).toHaveLength(0);
  });

  test('neither keyword nor label is still an empty query', () => {
    seed(db, [{ ticker: 'KXBTC', title: 'Bitcoin above 100k', category: 'Crypto' }]);
    expect(searchEventIndex(db, '', 50)).toHaveLength(0);
    expect(searchEventIndex(db, '', 50, { categoryLabels: [] })).toHaveLength(0);
  });

  test('the active-market filter still applies when labels are supplied', () => {
    seed(db, [
      { ticker: 'KXLIVE', title: 'Live crypto event', category: 'Crypto' },
      { ticker: 'KXDEAD', title: 'Closed crypto event', category: 'Crypto', closed: true },
    ]);
    expect(searchEventIndex(db, '', 50, { categoryLabels: ['Crypto'] }).map((r) => r.event_ticker))
      .toEqual(['KXLIVE']);
  });

  test('volume ordering still applies when labels are supplied', () => {
    seed(db, [
      { ticker: 'KXSMALL', title: 'Quiet crypto event', category: 'Crypto', volume: 10 },
      { ticker: 'KXBIG', title: 'Busy crypto event', category: 'Crypto', volume: 9000 },
    ]);
    expect(searchEventIndex(db, '', 50, { categoryLabels: ['Crypto'] }).map((r) => r.event_ticker))
      .toEqual(['KXBIG', 'KXSMALL']);
  });
});

describe('searchEventIndex tradeable predicate', () => {
  let db: Database;

  beforeEach(() => {
    db = createDb(':memory:');
  });
  afterEach(() => {
    db.close();
  });

  test('an event whose only market has settled is not returned', () => {
    // status alone does not mean tradeable: a market can settle upstream while
    // the index still carries status 'active'.
    seed(db, [
      { ticker: 'SETTLED', title: 'crypto settled', category: 'Crypto', tags: 'Crypto', result: 'yes' },
      { ticker: 'LIVE', title: 'crypto live', category: 'Crypto', tags: 'Crypto' },
    ]);

    const got = searchEventIndex(db, '', 30, { categoryLabels: ['Crypto'] });
    expect(got.map((e) => e.event_ticker)).toEqual(['LIVE']);
  });

  test('a settled market contributes no volume to the ranking', () => {
    // The filter and the ranking have to agree, or a settled market keeps
    // pushing its event up the list after it stops being tradeable.
    seed(db, [
      { ticker: 'MIXED', title: 'crypto mixed', category: 'Crypto', tags: 'Crypto', volume: 10 },
      { ticker: 'BUSY', title: 'crypto busy', category: 'Crypto', tags: 'Crypto', volume: 500 },
    ]);
    const row = db
      .query('SELECT markets_json FROM event_index WHERE event_ticker = ?')
      .get('MIXED') as { markets_json: string };
    const markets = JSON.parse(row.markets_json);
    markets.push({ ...markets[0], ticker: 'MIXED-T2', result: 'yes', volume: 999999 });
    db.query('UPDATE event_index SET markets_json = ? WHERE event_ticker = ?').run(
      JSON.stringify(markets),
      'MIXED',
    );

    const got = searchEventIndex(db, '', 30, { categoryLabels: ['Crypto'] });
    expect(got.map((e) => e.event_ticker)).toEqual(['BUSY', 'MIXED']);
  });
});
