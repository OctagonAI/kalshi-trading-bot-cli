import type { Database } from 'bun:sqlite';
import { fetchAllOctagonEvents, type OctagonEventEntry } from './octagon-events-api.js';
import { insertReport, getLatestReport, getTtlForCloseTime } from '../db/octagon-cache.js';
import { insertEdge } from '../db/edge.js';

const PREFETCH_COOLDOWN_MS = 60 * 60 * 1000; // 1 hour
const META_KEY = 'octagon_prefetch_at';

/**
 * Check if a prefetch is needed (more than 1h since last one).
 */
function shouldPrefetch(db: Database): boolean {
  const row = db.query("SELECT value FROM event_index_meta WHERE key = $key").get({
    $key: META_KEY,
  }) as { value: string } | null;
  if (!row) return true;
  const lastPrefetch = parseInt(row.value, 10);
  if (!Number.isFinite(lastPrefetch)) return true;
  return Date.now() - lastPrefetch > PREFETCH_COOLDOWN_MS;
}

function markPrefetchDone(db: Database): void {
  db.query("INSERT OR REPLACE INTO event_index_meta (key, value) VALUES ($key, $value)").run({
    $key: META_KEY,
    $value: String(Date.now()),
  });
}

/**
 * Infer a mispricing signal from the edge.
 */
function inferSignal(modelProb: number, marketProb: number): string {
  const edge = modelProb - marketProb;
  if (Math.abs(edge) < 0.03) return 'fair_value';
  return edge > 0 ? 'underpriced' : 'overpriced';
}

/**
 * Classify confidence from absolute edge.
 */
function classifyConfidence(absEdge: number): string {
  if (absEdge >= 0.10) return 'very_high';
  if (absEdge >= 0.05) return 'high';
  if (absEdge >= 0.02) return 'moderate';
  return 'low';
}

/**
 * Convert an Octagon event entry to local DB records and persist.
 * Returns true if a new record was inserted, false if skipped.
 */
function persistEvent(db: Database, event: OctagonEventEntry): boolean {
  const capturedDate = new Date(event.captured_at);
  const closeDate = new Date(event.close_time);
  if (isNaN(capturedDate.getTime()) || isNaN(closeDate.getTime())) return false;
  const capturedAt = Math.floor(capturedDate.getTime() / 1000);
  const closeTime = Math.floor(closeDate.getTime() / 1000);

  // Probabilities from the events API are percentages (0-100)
  const modelProb = event.model_probability / 100;
  const marketProb = event.market_probability / 100;

  // Skip if we already have a fresher report for this event
  const existing = getLatestReport(db, event.event_ticker);
  if (existing && existing.fetched_at >= capturedAt) return false;

  const ttl = getTtlForCloseTime(Math.max(0, closeTime - capturedAt));
  const reportId = `events-api-${event.event_ticker}-${capturedAt}`;

  insertReport(db, {
    report_id: reportId,
    ticker: event.event_ticker,
    event_ticker: event.event_ticker,
    model_prob: modelProb,
    market_prob: marketProb,
    mispricing_signal: inferSignal(modelProb, marketProb),
    drivers_json: JSON.stringify([{
      claim: event.key_takeaway || event.name,
      category: (event.series_category || 'other').toLowerCase(),
      impact: 'medium',
    }]),
    catalysts_json: null,
    sources_json: null,
    resolution_history_json: null,
    contract_snapshot_json: null,
    raw_response: null,
    variant_used: 'events-api',
    fetched_at: capturedAt,
    expires_at: capturedAt + ttl,
  });

  // Also persist to edge_history
  const edge = modelProb - marketProb;
  try {
    insertEdge(db, {
      ticker: event.event_ticker,
      event_ticker: event.event_ticker,
      timestamp: capturedAt,
      model_prob: modelProb,
      market_prob: marketProb,
      edge,
      octagon_report_id: reportId,
      drivers_json: null,
      sources_json: null,
      catalysts_json: null,
      cache_hit: 1,
      cache_miss: 0,
      confidence: classifyConfidence(Math.abs(edge)),
    });
  } catch (err) {
    // Only swallow UNIQUE constraint violations (duplicate ticker+timestamp)
    const msg = err instanceof Error ? err.message : String(err);
    if (/UNIQUE constraint failed/i.test(msg)) {
      // Expected — edge already exists for this ticker+timestamp
    } else {
      throw err;
    }
  }

  return true;
}

/**
 * Fetch all Octagon events via the REST API and persist them locally.
 * Runs only if the last prefetch was more than 1h ago.
 */
export async function prefetchOctagonEvents(db: Database): Promise<{ inserted: number; skipped: number }> {
  if (!shouldPrefetch(db)) {
    return { inserted: 0, skipped: 0 };
  }

  const events = await fetchAllOctagonEvents();
  let inserted = 0;
  let skipped = 0;

  for (const event of events) {
    if (persistEvent(db, event)) {
      inserted++;
    } else {
      skipped++;
    }
  }

  // Mark has_history, mutually_exclusive, series_category from the response
  const markMeta = db.prepare(
    `UPDATE octagon_reports SET has_history = $hh, mutually_exclusive = $me, series_category = $sc, confidence_score = $cs, outcome_probabilities_json = $opj
     WHERE event_ticker = $et AND variant_used = 'events-api'`,
  );
  db.transaction(() => {
    for (const event of events) {
      markMeta.run({
        $et: event.event_ticker,
        $hh: event.has_history ? 1 : 0,
        $me: event.mutually_exclusive ? 1 : 0,
        $sc: event.series_category ?? null,
        $cs: event.confidence_score ?? null,
        $opj: event.outcome_probabilities ? JSON.stringify(event.outcome_probabilities) : null,
      });
    }
  })();

  markPrefetchDone(db);
  return { inserted, skipped };
}
