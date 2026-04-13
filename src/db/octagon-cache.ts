import type { Database } from 'bun:sqlite';

export interface OctagonReport {
  report_id: string;
  ticker: string;
  event_ticker: string;
  model_prob: number;
  market_prob?: number | null;
  mispricing_signal?: string | null;
  drivers_json?: string | null;
  catalysts_json?: string | null;
  sources_json?: string | null;
  resolution_history_json?: string | null;
  contract_snapshot_json?: string | null;
  raw_response?: string | null;
  model_accuracy?: number | null;
  variant_used?: string | null;
  fetched_at: number;
  expires_at: number;
}

export function insertReport(db: Database, report: OctagonReport): void {
  // Preserve has_history, mutually_exclusive, series_category from the existing row
  // since INSERT OR REPLACE would reset them to defaults.
  const existing = db.query(
    'SELECT has_history, mutually_exclusive, series_category FROM octagon_reports WHERE report_id = $id',
  ).get({ $id: report.report_id }) as { has_history: number; mutually_exclusive: number; series_category: string | null } | null;

  db.prepare(`
    INSERT OR REPLACE INTO octagon_reports
      (report_id, ticker, event_ticker, model_prob, market_prob, mispricing_signal,
       drivers_json, catalysts_json, sources_json, resolution_history_json,
       contract_snapshot_json, raw_response, model_accuracy, variant_used, fetched_at, expires_at,
       has_history, mutually_exclusive, series_category)
    VALUES
      ($report_id, $ticker, $event_ticker, $model_prob, $market_prob, $mispricing_signal,
       $drivers_json, $catalysts_json, $sources_json, $resolution_history_json,
       $contract_snapshot_json, $raw_response, $model_accuracy, $variant_used, $fetched_at, $expires_at,
       $has_history, $mutually_exclusive, $series_category)
  `).run({
    $report_id: report.report_id,
    $ticker: report.ticker,
    $event_ticker: report.event_ticker,
    $model_prob: report.model_prob,
    $market_prob: report.market_prob ?? null,
    $mispricing_signal: report.mispricing_signal ?? null,
    $drivers_json: report.drivers_json ?? null,
    $catalysts_json: report.catalysts_json ?? null,
    $sources_json: report.sources_json ?? null,
    $resolution_history_json: report.resolution_history_json ?? null,
    $contract_snapshot_json: report.contract_snapshot_json ?? null,
    $raw_response: report.raw_response ?? null,
    $model_accuracy: report.model_accuracy ?? null,
    $variant_used: report.variant_used ?? null,
    $fetched_at: report.fetched_at,
    $expires_at: report.expires_at,
    $has_history: existing?.has_history ?? 0,
    $mutually_exclusive: existing?.mutually_exclusive ?? 0,
    $series_category: existing?.series_category ?? null,
  });
}

export function getReport(db: Database, reportId: string): OctagonReport | null {
  return db.query('SELECT * FROM octagon_reports WHERE report_id = $id').get({
    $id: reportId,
  }) as OctagonReport | null;
}

export function getLatestReport(db: Database, ticker: string): OctagonReport | null {
  return db.query(
    'SELECT * FROM octagon_reports WHERE ticker = $ticker ORDER BY fetched_at DESC LIMIT 1'
  ).get({ $ticker: ticker }) as OctagonReport | null;
}

export function updateReportModelProb(db: Database, reportId: string, modelProb: number): void {
  db.prepare(
    `UPDATE octagon_reports SET model_prob = $model_prob WHERE report_id = $report_id`,
  ).run({ $report_id: reportId, $model_prob: modelProb });
}

/**
 * Returns the cache TTL (in seconds) based on how far away the market close time is.
 *
 * | Time to close | TTL   |
 * |---------------|-------|
 * | <24h          | 1h    |
 * | 1–7d          | 6h    |
 * | 7–30d         | 24h   |
 * | 30d+          | 48h   |
 * | Already closed| 1h    |
 */
export function getTtlForCloseTime(secondsUntilClose: number): number {
  if (secondsUntilClose <= 0) return 3600;           // already closed → 1h
  if (secondsUntilClose < 86400) return 3600;        // <24h → 1h
  if (secondsUntilClose < 7 * 86400) return 21600;   // 1–7d → 6h
  if (secondsUntilClose < 30 * 86400) return 86400;  // 7–30d → 24h
  return 172800;                                      // 30d+ → 48h
}

/**
 * Returns true if no report exists for this ticker, or the latest is older than the TTL.
 * When closeTimeEpoch is provided, uses tiered TTL based on market close proximity.
 * Otherwise falls back to 24h.
 */
export function isStale(db: Database, ticker: string, nowSeconds?: number, closeTimeEpoch?: number): boolean {
  const now = nowSeconds ?? Math.floor(Date.now() / 1000);
  const report = getLatestReport(db, ticker);
  if (!report) return true;
  const ttl = closeTimeEpoch != null
    ? getTtlForCloseTime(closeTimeEpoch - now)
    : 86400;
  return report.fetched_at + ttl < now;
}
