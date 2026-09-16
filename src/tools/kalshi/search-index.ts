import { getDb } from '../../db/index.js';
import {
  countIndexRows,
  getIndexAge,
  getLastRefresh,
  pruneStaleEvents,
  setLastRefresh,
  upsertIndexEvents,
} from '../../db/event-index.js';
import { callKalshiApi, streamAllPages } from './api.js';
import { logger } from '../../utils/logger.js';
import type { KalshiEvent, KalshiSeries } from './types.js';

/** Stale threshold: triggers background refresh */
const INDEX_STALE_MS = 2 * 60 * 60 * 1000; // 2 hours

/** Hard TTL: data still usable but stale */
const INDEX_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

/** Singleton promise to prevent concurrent refreshes */
let _refreshPromise: Promise<void> | null = null;

/** Max concurrent series fetches during index refresh */
const SERIES_CONCURRENCY = 20;

/**
 * Page cap for a full build. Kalshi's open universe is ~12,800 events = 64
 * pages at 200/page; the old cap of 20 stopped at exactly 4,000, silently
 * indexing the first 31% of an arbitrary pagination order. This is a safety
 * valve against a runaway cursor, not a budget — the walk normally ends when
 * the cursor is exhausted.
 */
const MAX_INDEX_PAGES = 100;

/**
 * Overlap applied to the incremental watermark. Cheap insurance against events
 * updated in the seconds around the previous refresh.
 */
const INCREMENTAL_SLACK_MS = 5 * 60 * 1000;

// --- Progress observable ---

export type IndexProgressPhase = 'fetching_events' | 'fetching_series' | 'populating';

export interface IndexProgressInfo {
  phase: IndexProgressPhase;
  fetchedItems: number;
  page: number;
  maxPages: number;
  detail?: string;
}

export type IndexProgressListener = (info: IndexProgressInfo) => void;

const _progressListeners = new Set<IndexProgressListener>();

/** Subscribe to index refresh progress. Returns an unsubscribe function. */
export function onIndexProgress(listener: IndexProgressListener): () => void {
  _progressListeners.add(listener);
  return () => { _progressListeners.delete(listener); };
}

function emitProgress(info: IndexProgressInfo): void {
  for (const listener of _progressListeners) {
    try { listener(info); } catch { /* ignore listener errors */ }
  }
}

/** Get the current refresh promise so callers can await it if desired. */
export function getRefreshPromise(): Promise<void> | null {
  return _refreshPromise;
}

/**
 * Fetch series tags for a set of unique series tickers.
 * Returns a map of series_ticker → tags array.
 */
async function fetchSeriesTags(seriesTickers: string[], totalEvents: number): Promise<Map<string, string[]>> {
  const tagsMap = new Map<string, string[]>();
  // Process in batches to limit concurrency
  for (let i = 0; i < seriesTickers.length; i += SERIES_CONCURRENCY) {
    const batch = seriesTickers.slice(i, i + SERIES_CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(async (ticker) => {
        const data = await callKalshiApi('GET', `/series/${ticker}`);
        const series = (data.series ?? data) as KalshiSeries;
        return { ticker, tags: series.tags ?? [] };
      }),
    );
    for (const result of results) {
      // Keep empty lists too: they clear tags a series no longer carries.
      if (result.status === 'fulfilled') {
        tagsMap.set(result.value.ticker, result.value.tags);
      }
    }
    emitProgress({
      phase: 'fetching_series',
      fetchedItems: Math.min(i + SERIES_CONCURRENCY, seriesTickers.length),
      page: 0,
      maxPages: 0,
      detail: `Series tags: ${Math.min(i + SERIES_CONCURRENCY, seriesTickers.length)}/${seriesTickers.length} (${totalEvents} events)`,
    });
  }
  return tagsMap;
}

/**
 * Whether a refresh may run as an incremental delta instead of a full rebuild.
 *
 * Exported only so it can be tested directly. The regression it guards: a
 * forced refresh used to satisfy this test on its own — a populated, recent
 * index — so `forceRefreshIndex()` quietly fetched a `min_updated_ts` delta and
 * skipped the staleness sweep, leaving the index exactly as stale as before.
 * Observed live as a "rebuild" that produced 4,882 rows instead of ~12,700.
 */
export function shouldRunIncremental(
  force: boolean,
  lastRefresh: number | null,
  rowCount: number,
  now: number,
): boolean {
  if (force) return false;
  if (lastRefresh === null || rowCount === 0) return false;
  return now - lastRefresh < INDEX_TTL_MS;
}

/**
 * Refresh the local event index by fetching all open events from Kalshi API.
 *
 * `force` rebuilds in full. It has to be an explicit argument rather than
 * something inferred from index age: a forced refresh on a recent index would
 * otherwise satisfy the incremental test and quietly fetch only the delta, so
 * `forceRefreshIndex()` would leave delisted events in place — the exact bug it
 * is usually invoked to clear.
 */
async function refreshIndex(force = false): Promise<void> {
  const db = getDb();
  logger.info('[search-index] Refreshing event index from Kalshi API...');
  const start = Date.now();

  try {
    // An incremental pass only makes sense from an index that is populated and
    // recent enough to trust as a baseline; anything older rebuilds in full.
    const lastRefresh = getLastRefresh(db);
    const incremental = shouldRunIncremental(force, lastRefresh, countIndexRows(db), Date.now());

    // Only ~2.5% of open events change in an hour, so the routine 2-hourly
    // refresh walks 2-6 pages instead of all 64.
    const params: Record<string, string | number | boolean> = {
      status: 'open',
      with_nested_markets: true,
      ...(incremental && lastRefresh !== null
        ? { min_updated_ts: Math.floor((lastRefresh - INCREMENTAL_SLACK_MS) / 1000) }
        : {}),
    };

    // Pages are written as they arrive rather than accumulated: a cold index
    // becomes searchable within a second or so instead of after the full walk,
    // and the whole nested-market payload never sits in memory at once.
    const seriesSeen = new Set<string>();
    const { count, truncated } = await streamAllPages<KalshiEvent>(
      '/events',
      params,
      'events',
      MAX_INDEX_PAGES,
      (page) => {
        for (const e of page) {
          if (e.series_ticker) seriesSeen.add(e.series_ticker);
        }
        upsertIndexEvents(
          db,
          page.map((e) => ({
            event_ticker: e.event_ticker,
            series_ticker: e.series_ticker,
            title: e.title,
            category: e.category,
            strike_date: e.strike_date,
            sub_title: e.sub_title,
            markets: e.markets,
          })),
        );
      },
      (info) => {
        emitProgress({
          phase: 'fetching_events',
          fetchedItems: info.fetchedItems,
          page: info.page,
          maxPages: info.maxPages,
        });
      },
    );

    // A full build that returned nothing is a transient failure, not an empty
    // universe. Bail before pruning — otherwise the prune below would read
    // "nothing was seen this build" and delete every row.
    if (!incremental && count === 0) {
      throw new Error('Refusing to rebuild the event index from an empty result set');
    }
    if (truncated) {
      logger.warn(`[search-index] Event walk hit the ${MAX_INDEX_PAGES}-page cap — index may be partial`);
    }

    // Fetch series tags for whatever this pass touched
    const seriesTagsPromise = fetchSeriesTags([...seriesSeen], count);

    emitProgress({
      phase: 'populating',
      fetchedItems: count,
      page: 0,
      maxPages: 0,
      detail: `Wrote ${count} events to index...`,
    });

    // Sweeping rows this pass did not touch is only sound when the pass saw the
    // whole universe. An incremental run sees just the delta, and a truncated
    // walk never reached the tail — in both cases "not seen" carries no
    // information, and sweeping on it would delete live events. Fall back to
    // tradeability pruning alone.
    const completeSweep = !incremental && !truncated;
    const pruned = pruneStaleEvents(db, completeSweep ? start : 0);
    if (pruned > 0) {
      logger.info(`[search-index] Pruned ${pruned} stale or untradeable events`);
    }

    // Update tags on index rows
    const seriesTags = await seriesTagsPromise;
    if (seriesTags.size > 0) {
      const updateTags = db.prepare('UPDATE event_index SET tags = $tags WHERE series_ticker = $series_ticker');
      db.transaction(() => {
        for (const [seriesTicker, tags] of seriesTags) {
          updateTags.run({ $tags: tags.join(','), $series_ticker: seriesTicker });
        }
      })();
    }

    setLastRefresh(db, Date.now());

    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    logger.info(
      `[search-index] Index ${incremental ? 'updated' : 'rebuilt'}: ${count} events in ${elapsed}s (${countIndexRows(db)} rows total)`,
    );
  } catch (error) {
    logger.error('[search-index] Failed to refresh index:', error);
    throw error;
  }
}

/**
 * Force an immediate index rebuild, bypassing the 2-hour stale check.
 * If a refresh is already in progress, waits for it to complete first,
 * then starts a new one.
 */
export async function forceRefreshIndex(): Promise<void> {
  if (_refreshPromise) {
    await _refreshPromise;
  }
  _refreshPromise = refreshIndex(true).finally(() => {
    _refreshPromise = null;
  });
  await _refreshPromise;
}

/**
 * Ensure the local event index is fresh. If stale or empty, triggers a refresh.
 * Always returns immediately (never blocks).
 *
 * - age < 2h: nothing to do
 * - age >= 2h but < Infinity: fire-and-forget refresh, serve stale data
 * - age === Infinity (first run): fire-and-forget refresh, return immediately
 */
export async function ensureIndex(): Promise<void> {
  const db = getDb();
  const age = getIndexAge(db);

  // Fresh index that actually has rows — nothing to do. The row count is the
  // real emptiness test: a 0-row index can carry a fresh timestamp, which
  // would leave every search returning nothing for the full stale window.
  if (age < INDEX_STALE_MS && countIndexRows(db) > 0) return;

  // Stale or first-run: trigger background refresh if not already running
  if (!_refreshPromise) {
    _refreshPromise = refreshIndex()
      .catch((err) => {
        logger.error('[search-index] Background refresh failed:', err);
      })
      .finally(() => {
        _refreshPromise = null;
      });
  }
  // Never block — return immediately regardless of first-run or stale
}
