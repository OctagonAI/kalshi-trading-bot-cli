import type { Database } from 'bun:sqlite';

/** Thrown when the history API requires a paid subscription. */
export class SubscriptionRequiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SubscriptionRequiredError';
  }
}

/** Narrow snapshot type representing what we actually store and use from history. */
export interface OutcomeProbability {
  market_ticker: string;
  outcome_name?: string;
  model_probability: number;   // percentage 0-100
  market_probability: number;  // percentage 0-100
}

export interface HistorySnapshot {
  history_id: number;
  event_ticker: string;
  captured_at: string;
  name: string | null;
  series_category: string | null;
  confidence_score: number | null;
  model_probability: number;   // percentage 0-100
  market_probability: number;  // percentage 0-100
  edge_pp: number | null;
  close_time: string | null;
  outcome_probabilities?: OutcomeProbability[] | null;
  outcome_probabilities_json?: string | null; // raw JSON from DB cache
}

interface HistoryPage {
  event_ticker: string;
  data: HistorySnapshot[];
  next_cursor: string | null;
  has_more: boolean;
}

const EVENTS_API_BASE = 'https://api.octagonai.co/v1';
const PAGE_LIMIT = 200;
const TIMEOUT_MS = 60_000;

/**
 * Fetch all history snapshots for an event from the Octagon API.
 * Supports optional time window filtering via captured_from/captured_to.
 */
export async function fetchEventHistory(
  eventTicker: string,
  opts?: { capturedFrom?: string; capturedTo?: string },
): Promise<HistorySnapshot[]> {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) throw new Error('OCTAGON_API_KEY not set');

  const all: HistorySnapshot[] = [];
  let cursor: string | null = null;

  do {
    const params = new URLSearchParams({ limit: String(PAGE_LIMIT) });
    if (cursor) params.set('cursor', cursor);
    if (opts?.capturedFrom) params.set('captured_from', opts.capturedFrom);
    if (opts?.capturedTo) params.set('captured_to', opts.capturedTo);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

    let resp: Response;
    try {
      resp = await fetch(
        `${EVENTS_API_BASE}/prediction-markets/events/${encodeURIComponent(eventTicker)}/history?${params}`,
        { headers: { Authorization: `Bearer ${apiKey}` }, signal: controller.signal },
      );
    } finally {
      clearTimeout(timer);
    }

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      if (resp.status === 403 || resp.status === 402) {
        throw new SubscriptionRequiredError(
          'The Octagon history API requires a paid subscription. ' +
          'The unresolved edge scanner (--unresolved) uses the free events API. ' +
          'Upgrade at https://app.octagonai.co to unlock resolved market backtesting.',
        );
      }
      throw new Error(`Octagon history API ${resp.status} for ${eventTicker}: ${body.slice(0, 200)}`);
    }

    const raw = (await resp.json()) as unknown;
    if (!raw || typeof raw !== 'object') {
      throw new Error(`Octagon history API returned invalid response for ${eventTicker}`);
    }
    const page = raw as Record<string, unknown>;
    if (!Array.isArray(page.data)) {
      throw new Error(`Octagon history API response missing data array for ${eventTicker}`);
    }
    const hasMore = typeof page.has_more === 'boolean' ? page.has_more : false;
    if (hasMore && !page.next_cursor) {
      throw new Error(`Octagon history API has_more=true but next_cursor missing for ${eventTicker}`);
    }
    all.push(...(page.data as HistorySnapshot[]));
    cursor = hasMore ? (page.next_cursor as string) : null;
  } while (cursor);

  return all;
}

/**
 * Fetch event history and cache it in the local octagon_history table.
 * Only uses the cache for full-history requests (no time window).
 * When capturedFrom/capturedTo are provided, always fetches fresh from the API.
 */
export async function fetchAndCacheHistory(
  db: Database,
  eventTicker: string,
  opts?: { capturedFrom?: string; capturedTo?: string },
): Promise<HistorySnapshot[]> {
  const hasWindow = !!(opts?.capturedFrom || opts?.capturedTo);

  // Only use cache for full-history requests (no time window filter)
  if (!hasWindow) {
    const cached = db.query(
      'SELECT COUNT(*) as cnt FROM octagon_history WHERE event_ticker = $et',
    ).get({ $et: eventTicker }) as { cnt: number };

    if (cached.cnt > 0) {
      const rows = db.query(
        `SELECT history_id, event_ticker, captured_at, name, series_category,
                confidence_score, model_probability, market_probability, edge_pp, close_time,
                outcome_probabilities_json
         FROM octagon_history WHERE event_ticker = $et ORDER BY captured_at ASC`,
      ).all({ $et: eventTicker }) as HistorySnapshot[];
      // Parse outcome_probabilities from cached JSON
      for (const r of rows) {
        if (r.outcome_probabilities_json) {
          try { r.outcome_probabilities = JSON.parse(r.outcome_probabilities_json); } catch { /* skip */ }
        }
      }
      return rows;
    }
  }

  // Fetch from API
  const snapshots = await fetchEventHistory(eventTicker, opts);

  // Cache in DB (only for full-history requests to avoid partial cache)
  if (!hasWindow) {
    const insert = db.prepare(`
      INSERT OR IGNORE INTO octagon_history
        (history_id, event_ticker, captured_at, model_probability, market_probability,
         edge_pp, confidence_score, series_category, close_time, name, outcome_probabilities_json)
      VALUES ($history_id, $event_ticker, $captured_at, $model_probability, $market_probability,
              $edge_pp, $confidence_score, $series_category, $close_time, $name, $opj)
    `);

    db.transaction(() => {
      for (const s of snapshots) {
        insert.run({
          $history_id: s.history_id,
          $event_ticker: s.event_ticker,
          $captured_at: s.captured_at,
          $model_probability: s.model_probability,
          $market_probability: s.market_probability,
          $edge_pp: s.edge_pp,
          $confidence_score: s.confidence_score,
          $series_category: s.series_category ?? null,
          $close_time: s.close_time ?? null,
          $name: s.name ?? null,
          $opj: s.outcome_probabilities ? JSON.stringify(s.outcome_probabilities) : null,
        });
      }
    })();
  }

  return snapshots;
}

/**
 * Select the snapshot closest to a target date (N days ago).
 * Returns the last snapshot captured on or before the target date.
 * Probabilities in the returned snapshot are percentages (0-100).
 */
export function selectSnapshotByDate(
  snapshots: HistorySnapshot[],
  targetDate: Date,
): HistorySnapshot | null {
  const targetEpoch = targetDate.getTime();

  let best: HistorySnapshot | null = null;
  let bestEpoch = -Infinity;
  for (const s of snapshots) {
    const capturedEpoch = new Date(s.captured_at).getTime();
    if (capturedEpoch <= targetEpoch && capturedEpoch > bestEpoch) {
      best = s;
      bestEpoch = capturedEpoch;
    }
  }
  return best;
}
