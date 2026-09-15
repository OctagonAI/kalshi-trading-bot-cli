import type { Database } from 'bun:sqlite';
import type { AuditTrail } from '../audit/trail.js';
import { callKalshiApi, fetchAllPages } from '../tools/kalshi/api.js';
import type { KalshiEvent, KalshiMarket, KalshiSeries } from '../tools/kalshi/types.js';
import { ensureIndex, getRefreshPromise } from '../tools/kalshi/search-index.js';
import { upsertEvent, deactivateExpired } from '../db/events.js';
import { getThemeTickers } from '../db/themes.js';
import { findTheme, themeCategoryLabels } from './theme-registry.js';

/**
 * Maps lowercase theme IDs → exact Kalshi category labels.
 *
 * Derived from the theme registry rather than hand-maintained: the old literal
 * had drifted from the data (`Transportation` matched zero rows, and the
 * `Science & Technology` spelling was unreachable). Values are arrays because
 * one theme legitimately spans several upstream labels.
 */
export const CATEGORY_MAP: Record<string, string[]> = themeCategoryLabels();

/**
 * Fetch all series from Kalshi and build a map of category → sorted subcategory tags.
 * Each series has a `tags` field; we collect unique tags per category.
 */
export async function fetchSubcategories(): Promise<Record<string, string[]>> {
  const allSeries = await fetchAllPages<KalshiSeries>('/series', {}, 'series', 50);
  const catTags: Record<string, Set<string>> = {};

  for (const s of allSeries) {
    const cat = s.category;
    if (!cat) continue;
    if (!catTags[cat]) catTags[cat] = new Set();
    for (const tag of s.tags ?? []) {
      catTags[cat].add(tag);
    }
  }

  const result: Record<string, string[]> = {};
  for (const [cat, tags] of Object.entries(catTags)) {
    result[cat] = [...tags].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  }
  return result;
}

export class ThemeResolver {
  private db: Database;
  private audit: AuditTrail;

  constructor(db: Database, audit: AuditTrail) {
    this.db = db;
    this.audit = audit;
  }

  async resolve(themeName: string): Promise<string[]> {
    const now = Math.floor(Date.now() / 1000);
    let eventTickers: string[];

    if (themeName === 'top50') {
      eventTickers = await this.resolveTop50();
    } else if (themeName.includes(':')) {
      // Subcategory filter: "crypto:btc", "sports:football"
      eventTickers = await this.resolveSubcategory(themeName);
    } else if (CATEGORY_MAP[themeName]) {
      eventTickers = await this.resolveCategory(themeName);
    } else {
      eventTickers = getThemeTickers(this.db, themeName);
    }

    // Upsert resolved events
    for (const ticker of eventTickers) {
      upsertEvent(this.db, { ticker, active: 1, updated_at: now });
    }

    // Deactivate expired events
    deactivateExpired(this.db, now);

    // Audit log
    this.audit.log({
      type: 'SCAN_START',
      theme: themeName,
      events_count: eventTickers.length,
    });

    return eventTickers;
  }

  private async resolveTop50(): Promise<string[]> {
    const markets = await fetchAllPages<KalshiMarket>(
      '/markets',
      { status: 'open', limit: 200 },
      'markets',
      3
    );

    // Sort by volume_24h descending
    markets.sort((a, b) => (b.volume_24h ?? 0) - (a.volume_24h ?? 0));

    // Take top 50 unique event tickers
    const seen = new Set<string>();
    const result: string[] = [];
    for (const m of markets) {
      if (!seen.has(m.event_ticker)) {
        seen.add(m.event_ticker);
        result.push(m.event_ticker);
        if (result.length >= 50) break;
      }
    }
    return result;
  }

  private async resolveCategory(themeName: string): Promise<string[]> {
    const theme = findTheme(themeName);
    const labels = theme?.kalshiCategories ?? [];
    const tags = theme?.tags ?? [];
    if (labels.length === 0 && tags.length === 0) return [];

    // Kalshi /events API does not support server-side category filtering,
    // so query the local SQLite index instead of fetching all open events
    await ensureIndex();
    // If ensureIndex kicked off a background refresh (first run / empty index),
    // await it so we don't query an unpopulated event_index table
    const pending = getRefreshPromise();
    if (pending) await pending;

    const seen = new Set<string>();
    if (labels.length > 0) {
      const placeholders = labels.map(() => '?').join(',');
      const rows = this.db.query(
        `SELECT event_ticker FROM event_index WHERE category IN (${placeholders})`,
      ).all(...labels) as { event_ticker: string }[];
      for (const r of rows) seen.add(r.event_ticker);
    }
    // Comma-wrap both sides so a tag matches as a whole token: "Tech" must not
    // also match "Tech Stocks".
    for (const tag of tags) {
      const rows = this.db.query(
        `SELECT event_ticker FROM event_index WHERE ',' || COALESCE(tags, '') || ',' LIKE ?`,
      ).all(`%,${tag},%`) as { event_ticker: string }[];
      for (const r of rows) seen.add(r.event_ticker);
    }
    return [...seen];
  }

  private async resolveSubcategory(themeName: string): Promise<string[]> {
    const [catKey, ...subParts] = themeName.split(':');
    const subTag = subParts.join(':').toLowerCase();
    const labels = findTheme(catKey ?? '')?.kalshiCategories ?? [];
    if (labels.length === 0) return [];

    // Find series in these categories with a matching tag
    const perLabel = await Promise.all(
      labels.map((label) =>
        fetchAllPages<KalshiSeries>('/series', { category: label }, 'series', 50),
      ),
    );
    const wanted = new Set(labels);
    const matchingSeries = new Set<string>();
    for (const series of perLabel) {
      for (const s of series) {
        if (!s.category || !wanted.has(s.category)) continue;
        const hasTag = (s.tags ?? []).some((t) => {
          const tagLower = t.toLowerCase();
          const tagKebab = tagLower.replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
          return tagLower === subTag || tagKebab === subTag;
        });
        if (hasTag) matchingSeries.add(s.ticker);
      }
    }

    if (matchingSeries.size === 0) return [];

    // Fetch open events for matching series in parallel (server-side filtered)
    const results = await Promise.all(
      [...matchingSeries].map((seriesTicker) =>
        fetchAllPages<KalshiEvent>(
          '/events',
          { status: 'open', series_ticker: seriesTicker },
          'events',
          50
        )
      )
    );

    const seen = new Set<string>();
    const eventTickers: string[] = [];
    for (const events of results) {
      for (const e of events) {
        if (!seen.has(e.event_ticker)) {
          seen.add(e.event_ticker);
          eventTickers.push(e.event_ticker);
        }
      }
    }

    return eventTickers;
  }
}
