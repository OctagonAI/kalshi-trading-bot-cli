import { getDb } from '../../db/index.js';
import { clearAndPopulateIndex, countIndexRows, getIndexAge, setLastRefresh } from '../../db/event-index.js';
import { callKalshiApi, fetchAllPages } from './api.js';
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
 * Refresh the local event index by fetching all open events from Kalshi API.
 */
async function refreshIndex(): Promise<void> {
  const db = getDb();
  logger.info('[search-index] Refreshing event index from Kalshi API...');
  const start = Date.now();

  try {
    // Fetch events with nested markets — the API now includes volume_24h_fp
    // and last_price_dollars on nested markets, so no separate enrichment needed
    const events = await fetchAllPages<KalshiEvent>(
      '/events',
      { status: 'open', with_nested_markets: true },
      'events',
      20,
      (info) => {
        emitProgress({
          phase: 'fetching_events',
          fetchedItems: info.fetchedItems,
          page: info.page,
          maxPages: info.maxPages,
        });
      }
    );

    // Fetch series tags in parallel with index write
    const uniqueSeries = [...new Set(events.map((e) => e.series_ticker).filter(Boolean))];
    const seriesTagsPromise = fetchSeriesTags(uniqueSeries, events.length);

    emitProgress({
      phase: 'populating',
      fetchedItems: events.length,
      page: 0,
      maxPages: 0,
      detail: `Writing ${events.length} events to index...`,
    });

    // Write events with nested markets directly to the index
    clearAndPopulateIndex(
      db,
      events.map((e) => ({
        event_ticker: e.event_ticker,
        series_ticker: e.series_ticker,
        title: e.title,
        category: e.category,
        strike_date: e.strike_date,
        sub_title: e.sub_title,
        markets: e.markets,
      })),
    );

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
    logger.info(`[search-index] Index refreshed: ${events.length} events in ${elapsed}s`);
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
  _refreshPromise = refreshIndex().finally(() => {
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
