import type { Database } from 'bun:sqlite';
import type { KalshiEvent, KalshiMarket } from '../tools/kalshi/types.js';

export interface IndexedEvent {
  event_ticker: string;
  series_ticker: string | null;
  title: string;
  category: string | null;
  strike_date: string | null;
  sub_title: string | null;
  tags: string | null;
  markets_json: string | null;
  indexed_at: number;
}

/**
 * Search the local event index using keyword matching.
 * All keywords must match against title, event_ticker, series_ticker, or category.
 * Returns up to `limit` results.
 *
 * By default, only events with at least one active (open/active status, not past
 * close_time) market are returned, and expired markets are stripped from each
 * event's `markets_json`. Pass `{ includeExpired: true }` to disable both filters.
 */
export function searchEventIndex(
  db: Database,
  query: string,
  limit = 50,
  options: { includeExpired?: boolean } = {},
): IndexedEvent[] {
  const { includeExpired = false } = options;
  const keywords = query
    .toLowerCase()
    .split(/\s+/)
    .filter((k) => k.length > 0);

  if (keywords.length === 0) return [];

  // Build WHERE clause: each keyword must match somewhere in the searchable fields
  const conditions = keywords.map((_, i) => `(search_text LIKE $kw${i})`);
  const whereClause = conditions.join(' AND ');

  const now = new Date().toISOString();
  const params: Record<string, string | number> = { $limit: limit, $now: now };
  keywords.forEach((kw, i) => {
    params[`$kw${i}`] = `%${kw}%`;
  });

  // Require at least one active market unless caller opts in to expired events.
  const activeMarketsClause = includeExpired
    ? ''
    : `AND EXISTS (
        SELECT 1 FROM json_each(markets_json)
        WHERE json_extract(value, '$.status') IN ('open','active')
          AND (json_extract(value, '$.close_time') IS NULL OR json_extract(value, '$.close_time') > $now)
      )`;

  // Use a CTE to compute search_text, filter expired markets, and rank by open-market volume descending
  const fullSql = `
    WITH indexed AS (
      SELECT *,
        lower(title) || ' ' || lower(coalesce(event_ticker,'')) || ' ' || lower(coalesce(series_ticker,'')) || ' ' || lower(coalesce(category,'')) || ' ' || lower(coalesce(sub_title,'')) || ' ' || lower(coalesce(tags,'')) AS search_text
      FROM event_index
    ),
    matched AS (
      SELECT event_ticker, series_ticker, title, category, strike_date, sub_title, tags, markets_json, indexed_at
      FROM indexed
      WHERE ${whereClause}
        ${activeMarketsClause}
    )
    SELECT *
    FROM matched
    ORDER BY (
      SELECT coalesce(sum(
        CASE WHEN json_extract(value, '$.status') IN ('open','active')
              AND (json_extract(value, '$.close_time') IS NULL OR json_extract(value, '$.close_time') > $now)
             THEN json_extract(value, '$.volume')
             ELSE 0
        END
      ), 0)
      FROM json_each(markets_json)
    ) DESC
    LIMIT $limit
  `;

  const rows = db.query(fullSql).all(params) as IndexedEvent[];
  if (includeExpired) return rows;

  // Strip expired markets from each event's markets_json so callers never see them.
  return rows.map((r) => ({
    ...r,
    markets_json: filterActiveMarketsJson(r.markets_json, now),
  }));
}

/**
 * Predicate shared by every call site that filters expired markets.
 * "Active" means status in ('open','active') AND (no close_time or close_time > now).
 */
function isActiveMarketRecord(record: Record<string, unknown>, nowIso: string): boolean {
  const status = record.status;
  if (status !== 'open' && status !== 'active') return false;
  const closeTime = record.close_time;
  if (closeTime != null && typeof closeTime === 'string' && closeTime <= nowIso) return false;
  return true;
}

/**
 * Parse markets_json from the index into an array of object records.
 * Returns [] on parse failure or non-array payloads, and drops any non-object entries.
 */
function parseMarketsJsonSafe(markets_json: string | null): Array<Record<string, unknown>> {
  if (!markets_json) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(markets_json);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  return parsed.filter((m): m is Record<string, unknown> => typeof m === 'object' && m !== null);
}

/**
 * Parse markets_json, drop markets that aren't currently active, and re-serialize.
 * Returns the original string on parse failure (so callers don't lose data they could re-parse).
 */
function filterActiveMarketsJson(markets_json: string | null, nowIso: string): string | null {
  if (!markets_json) return markets_json;
  let parsed: unknown;
  try {
    parsed = JSON.parse(markets_json);
  } catch {
    return markets_json;
  }
  if (!Array.isArray(parsed)) return markets_json;
  const active = parsed.filter(
    (m): m is Record<string, unknown> =>
      typeof m === 'object' && m !== null && isActiveMarketRecord(m as Record<string, unknown>, nowIso),
  );
  return JSON.stringify(active);
}

/**
 * Clear and repopulate the event index in a single transaction.
 */
export function clearAndPopulateIndex(
  db: Database,
  events: Array<{
    event_ticker: string;
    series_ticker?: string;
    title: string;
    category?: string;
    strike_date?: string;
    sub_title?: string;
    tags?: string[];
    markets?: KalshiMarket[];
  }>,
  lastPriceMap?: Map<string, { last_price?: number; dollar_last_price?: string; volume_24h_fp?: string }>,
): void {
  const now = Date.now();

  const insert = db.prepare(`
    INSERT INTO event_index (event_ticker, series_ticker, title, category, strike_date, sub_title, tags, markets_json, indexed_at)
    VALUES ($event_ticker, $series_ticker, $title, $category, $strike_date, $sub_title, $tags, $markets_json, $indexed_at)
  `);

  db.transaction(() => {
    db.exec('DELETE FROM event_index');

    for (const event of events) {
      const compactMarkets = event.markets?.map((m) => {
        const ticker = m.ticker as string;
        const priceData = lastPriceMap?.get(ticker);
        return {
          ticker,
          title: m.title,
          yes_sub_title: m.yes_sub_title,
          yes_bid: m.yes_bid,
          yes_ask: m.yes_ask,
          yes_bid_dollars: m.yes_bid_dollars,
          yes_ask_dollars: m.yes_ask_dollars,
          no_bid: m.no_bid,
          no_ask: m.no_ask,
          no_bid_dollars: m.no_bid_dollars,
          no_ask_dollars: m.no_ask_dollars,
          last_price: priceData?.last_price ?? m.last_price,
          last_price_dollars: priceData?.dollar_last_price ?? m.last_price_dollars,
          dollar_last_price: priceData?.dollar_last_price ?? m.dollar_last_price,
          volume: m.volume_fp ?? m.volume ?? 0,
          volume_24h: parseFloat(priceData?.volume_24h_fp ?? String(m.volume_24h_fp ?? m.volume_24h ?? 0)),
          close_time: m.close_time,
          status: m.status,
          result: m.result,
        };
      });

      insert.run({
        $event_ticker: event.event_ticker,
        $series_ticker: event.series_ticker ?? null,
        $title: event.title,
        $category: event.category ?? null,
        $strike_date: event.strike_date ?? null,
        $sub_title: event.sub_title ?? null,
        $tags: event.tags?.length ? event.tags.join(',') : null,
        $markets_json: compactMarkets ? JSON.stringify(compactMarkets) : null,
        $indexed_at: now,
      });
    }
  })();
}

/**
 * Enrich existing index rows with market price/volume data from the API.
 * Groups market data by event_ticker and upserts markets_json for each event,
 * creating it from scratch if it was NULL (e.g. after Phase 1 index build).
 */
export function enrichIndexPrices(
  db: Database,
  priceMap: Map<string, { last_price?: number; dollar_last_price?: string; volume_24h_fp?: string }>,
  marketsByEvent?: Map<string, Array<Record<string, unknown>>>,
): void {
  if (priceMap.size === 0 && (!marketsByEvent || marketsByEvent.size === 0)) return;

  const update = db.prepare('UPDATE event_index SET markets_json = $markets_json WHERE event_ticker = $event_ticker');

  db.transaction(() => {
    if (marketsByEvent) {
      // Build markets_json from full market data, enriched with prices
      for (const [eventTicker, markets] of marketsByEvent) {
        const compactMarkets = markets.map((m) => {
          const ticker = m.ticker as string;
          const priceData = priceMap.get(ticker);
          return {
            ticker,
            title: m.title,
            yes_sub_title: m.yes_sub_title,
            yes_bid: m.yes_bid,
            yes_ask: m.yes_ask,
            yes_bid_dollars: m.yes_bid_dollars,
            yes_ask_dollars: m.yes_ask_dollars,
            no_bid: m.no_bid,
            no_ask: m.no_ask,
            no_bid_dollars: m.no_bid_dollars,
            no_ask_dollars: m.no_ask_dollars,
            last_price: priceData?.last_price ?? m.last_price,
            dollar_last_price: priceData?.dollar_last_price ?? m.dollar_last_price,
            last_price_dollars: priceData?.dollar_last_price ?? m.last_price_dollars,
            volume: m.volume_fp ?? m.volume ?? 0,
            volume_24h: parseFloat(priceData?.volume_24h_fp ?? String(m.volume_24h_fp ?? m.volume_24h ?? 0)),
            close_time: m.close_time,
            status: m.status,
            result: m.result,
          };
        });
        update.run({ $markets_json: JSON.stringify(compactMarkets), $event_ticker: eventTicker });
      }
    } else {
      // Fallback: update existing markets_json rows with price data
      const rows = db.query('SELECT event_ticker, markets_json FROM event_index WHERE markets_json IS NOT NULL').all() as Array<{
        event_ticker: string;
        markets_json: string;
      }>;

      for (const row of rows) {
        let markets: Array<Record<string, unknown>>;
        try {
          markets = JSON.parse(row.markets_json);
        } catch {
          continue;
        }

        let changed = false;
        for (const m of markets) {
          const ticker = m.ticker as string;
          const priceData = priceMap.get(ticker);
          if (!priceData) continue;
          if (priceData.last_price != null) m.last_price = priceData.last_price;
          if (priceData.dollar_last_price != null) m.dollar_last_price = priceData.dollar_last_price;
          if (priceData.volume_24h_fp != null) m.volume_24h = parseFloat(priceData.volume_24h_fp);
          changed = true;
        }

        if (changed) {
          update.run({ $markets_json: JSON.stringify(markets), $event_ticker: row.event_ticker });
        }
      }
    }
  })();
}

/**
 * Get the timestamp of the last successful index refresh, or null if never refreshed.
 */
export function getLastRefresh(db: Database): number | null {
  const row = db.query("SELECT value FROM event_index_meta WHERE key = 'last_refresh'").get() as
    | { value: string }
    | null;
  return row ? parseInt(row.value, 10) : null;
}

/**
 * Set the last refresh timestamp.
 */
export function setLastRefresh(db: Database, timestamp: number): void {
  db.query("INSERT OR REPLACE INTO event_index_meta (key, value) VALUES ('last_refresh', $ts)").run({
    $ts: String(timestamp),
  });
}

/**
 * Reconstruct KalshiEvent[] from the local index for given event tickers.
 * Parses markets_json back into nested market objects.
 *
 * By default, expired markets (status not open/active, or past close_time) are
 * stripped from each event. Pass `{ includeExpired: true }` to keep them.
 */
export function getEventsFromIndex(
  db: Database,
  eventTickers: string[],
  options: { includeExpired?: boolean } = {},
): KalshiEvent[] {
  if (eventTickers.length === 0) return [];

  const { includeExpired = false } = options;
  const nowIso = new Date().toISOString();

  const placeholders = eventTickers.map(() => '?').join(',');
  const rows = db
    .query(
      `SELECT event_ticker, series_ticker, title, category, strike_date, sub_title, markets_json
       FROM event_index
       WHERE event_ticker IN (${placeholders})`,
    )
    .all(...eventTickers) as IndexedEvent[];

  return rows.map((r) => {
    let markets = parseMarketsJsonSafe(r.markets_json);
    if (!includeExpired) {
      markets = markets.filter((m) => isActiveMarketRecord(m, nowIso));
    }
    return {
      event_ticker: r.event_ticker,
      series_ticker: r.series_ticker ?? '',
      title: r.title,
      category: r.category ?? '',
      sub_title: r.sub_title ?? '',
      strike_date: r.strike_date ?? '',
      mutually_exclusive: false,
      markets: markets as unknown as KalshiMarket[],
    } as KalshiEvent;
  });
}

/**
 * Get top N events by total market volume from the index.
 * Parses markets_json, sums volume per event, sorts descending.
 */
export function getTopEventsByVolume(db: Database, limit: number): KalshiEvent[] {
  const rows = db
    .query(
      `SELECT event_ticker, series_ticker, title, category, strike_date, sub_title, markets_json
       FROM event_index
       WHERE markets_json IS NOT NULL`,
    )
    .all() as IndexedEvent[];

  const events: Array<{ event: KalshiEvent; totalVolume: number }> = [];
  for (const r of rows) {
    const markets = parseMarketsJsonSafe(r.markets_json);
    const totalVolume = markets.reduce(
      (sum, m) => sum + (parseFloat(String(m.volume ?? '')) || parseFloat(String(m.volume_fp ?? '')) || 0),
      0,
    );
    events.push({
      event: {
        event_ticker: r.event_ticker,
        series_ticker: r.series_ticker ?? '',
        title: r.title,
        category: r.category ?? '',
        sub_title: r.sub_title ?? '',
        strike_date: r.strike_date ?? '',
        mutually_exclusive: false,
        markets: markets as unknown as KalshiMarket[],
      } as KalshiEvent,
      totalVolume,
    });
  }

  events.sort((a, b) => b.totalVolume - a.totalVolume);
  return events.slice(0, limit).map((e) => e.event);
}

/**
 * Get the age of the index in milliseconds, or Infinity if never refreshed.
 */
export function getIndexAge(db: Database): number {
  const last = getLastRefresh(db);
  if (last === null) return Infinity;
  return Date.now() - last;
}
