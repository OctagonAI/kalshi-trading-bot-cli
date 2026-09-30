/**
 * Octagon Prediction Markets Reports API.
 *
 * Direct REST access to report versions, bodies, and asynchronous generation —
 * replaces pulling reports through the Prediction Markets Agent's legacy
 * `:cache` / `:refresh` model variants.
 *
 *   GET  /predictions/reports/kalshi/{event_ticker}            versions (free)
 *   GET  /predictions/reports/kalshi/{event_ticker}?version=…  + markdown body
 *   GET  /predictions/reports/kalshi/{event_ticker}/trust      Trust Index scorecard
 *   GET  /predictions/reports/status/{run_id}                  run status (free)
 *   POST /predictions/reports/kalshi/{event_ticker}            fresh run (3 credits, 202)
 *
 * Docs: /guide/rest-api/prediction-markets-reports
 */
import { logger } from '../utils/logger.js';
import { fetchWithDeadline } from '../utils/http.js';

const REPORTS_API_BASE = 'https://api.octagonai.co/v1';
const REQUEST_TIMEOUT_MS = 60_000;
const GET_RETRY_STATUS = [502, 503, 504, 522, 524];
const GET_MAX_RETRIES = 3;
const GET_RETRY_DELAYS = [5_000, 15_000, 30_000];

/** One entry of `versions`: that run's headline only. The event is named on the response itself. */
export interface ReportVersion {
  run_id: string;
  captured_at: string;
  analysis_last_updated: string;
  market_probability: number;
  model_probability: number;
  confidence_score: number;
  total_volume: number;
  key_takeaway: string;
}

export interface ReportVersionsResponse {
  event_ticker: string;
  venue: 'kalshi' | 'polymarket';
  /** The pinned version's event name. */
  name: string | null;
  requested_url: string | null;
  versions: ReportVersion[];
  /** Populated only when a `version` was requested and resolved. */
  markdown_report: string | null;
  /** The run `markdown_report` corresponds to; null when no body requested. */
  run_id: string | null;
  /**
   * The pinned version's per-outcome rows, as a JSON string: market_ticker, outcome_name,
   * model_probability / market_probability (0-100), model_probability_source, evidence_grade, …
   */
  outcome_probabilities_json: string | null;
}

export interface ReportRunStatus {
  run_id: string;
  status: 'processing' | 'completed' | 'failed';
  venue: 'kalshi' | 'polymarket';
  event_ticker: string | null;
  requested_url: string | null;
}

export interface ReportGenerationAccepted {
  run_id: string;
  status: 'processing';
  event_ticker: string;
  venue: 'kalshi' | 'polymarket';
}

// ─── Trust Index (GET /predictions/reports/{venue}/{event}/trust) ───────────
// Every score is an integer in [0, 100], higher is better; null means unscored.

export type TrustLabel = 'Strong' | 'Good' | 'Caution' | 'High Risk' | 'Avoid';
export type IntegrityPillarKey = 'manipulation_resistance' | 'information_fairness' | 'settlement_reliability';

export interface TrustEvidence {
  text: string;
  window?: string | null;
}

/** One integrity pillar. Always all three, in a fixed order. */
export interface TrustPillar {
  key: IntegrityPillarKey;
  name: string;
  score: number | null;
  label: TrustLabel | null;
  basis: 'measured' | 'structural_prior' | null;
  summary: string | null;
  factors: string[];
}

/** One trade-quality row. Always all three; one without a reading has null score and label. */
export interface TrustComponent {
  key: 'liquidity' | 'move_quality' | 'rule_clarity';
  name: string;
  score: number | null;
  label: string | null;
}

/** One integrity detector's reading; returned only with expand=integrity. */
export interface TrustScreen {
  key: string;
  state: 'run' | 'not_applicable' | 'awaiting_data';
  score: number | null;
  label: string | null;
  confidence: string | null;
  drivers: string[];
  evidence: TrustEvidence[];
  warning?: string | null;
}

export interface TrustMarketScore {
  score: number | null;
  label: string | null;
  confidence: string | null;
  not_applicable: boolean;
  drivers: string[];
  evidence: TrustEvidence[];
  warning?: string | null;
  checks?: Record<string, string> | null;
}

/** Per-market card; returned only with expand=trade_quality. */
export interface TrustMarket {
  market_ticker: string;
  title: string | null;
  is_primary: boolean;
  last_trade_cents: number | null;
  scores: {
    market_quality: TrustMarketScore | null;
    liquidity: TrustMarketScore | null;
    move_quality: TrustMarketScore | null;
    resolution_clarity: TrustMarketScore | null;
  };
}

export interface TrustCap {
  key: IntegrityPillarKey | 'market_quality' | 'severe_activity_flag';
  name: string;
  floor: number | null;
  ceiling: number | null;
}

/** A pillar under its safety floor whose cap lowered nothing: the score was already under it. */
export interface TrustFloorBreach {
  key: IntegrityPillarKey;
  floor: number;
  score: number;
}

export interface TrustIndex {
  score: number;
  label: TrustLabel;
  computed_at: string;
  version: string;
  caps: TrustCap[];
  uncapped_score?: number | null;
  floors_breached: TrustFloorBreach[];
  profile: {
    integrity: {
      key: 'integrity';
      name: string;
      score: number | null;
      label: TrustLabel | null;
      risk: { score: number; label: string } | null;
      breakdown: TrustPillar[];
      screen_counts: { run: number; not_applicable: number; awaiting_data: number } | null;
      screens?: TrustScreen[] | null;
      structure_classes?: string[] | null;
    };
    trade_quality: {
      key: 'trade_quality';
      name: string;
      score: number | null;
      label: TrustLabel | null;
      basis: 'measured' | 'structural_prior' | null;
      summary: string | null;
      factors: string[];
      breakdown: TrustComponent[];
      markets?: TrustMarket[] | null;
      /** Markets left out of the trade-quality read, by reason. */
      exclusions?: {
        total: number;
        terminal_lifecycle: number;
        below_volume_floor: number;
        no_ticker: number;
      } | null;
    };
  };
}

export interface TrustIndexResponse {
  event_ticker: string;
  /** The event's name. Not yet served by the API; rendered once it is. */
  title?: string | null;
  venue: 'kalshi' | 'polymarket';
  run_id: string;
  trust_index: TrustIndex;
}

/** Axes the trust endpoint can return in depth. */
export type TrustExpand = 'integrity' | 'trade_quality';

export class OctagonReportsApiError extends Error {
  constructor(
    public statusCode: number,
    public code: string | null,
    message: string,
  ) {
    super(message);
    this.name = 'OctagonReportsApiError';
  }
}

function requireApiKey(): string {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) throw new Error('OCTAGON_API_KEY not set. Get one at https://app.octagonai.co');
  return apiKey;
}

function baseUrl(): string {
  return process.env.OCTAGON_BASE_URL ?? REPORTS_API_BASE;
}

function toApiError(status: number, body: string): OctagonReportsApiError {
  let code: string | null = null;
  let message = body.slice(0, 300);
  try {
    const parsed = JSON.parse(body) as { error?: { code?: string; message?: string } };
    code = parsed.error?.code ?? null;
    message = parsed.error?.message ?? message;
  } catch {
    // non-JSON body — keep the raw slice
  }
  return new OctagonReportsApiError(status, code, `Octagon reports API ${status}${code ? ` (${code})` : ''}: ${message}`);
}

async function requestJson<T>(
  method: 'GET' | 'POST',
  path: string,
  opts?: { retry?: boolean },
): Promise<T> {
  const apiKey = requireApiKey();
  const maxRetries = opts?.retry ? GET_MAX_RETRIES : 0;

  let lastError: Error | null = null;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (attempt > 0) {
      const delay = GET_RETRY_DELAYS[attempt - 1];
      logger.info(`[reports-api] retrying in ${delay / 1000}s (attempt ${attempt + 1}/${maxRetries + 1})`);
      await new Promise((r) => setTimeout(r, delay));
    }
    // The deadline stays armed through the body read, so read the body inside
    // this try too. An abort there has to become the same "timed out" error:
    // GETs retry on it, and isAmbiguousGenerationFailure only recognises a POST
    // that may have started a run by that message.
    let resp: Response;
    let body: string;
    try {
      resp = await fetchWithDeadline(`${baseUrl()}${path}`, {
        method,
        headers: { Authorization: `Bearer ${apiKey}` },
      }, REQUEST_TIMEOUT_MS);
      body = await resp.text();
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        lastError = new Error(`Octagon reports API timed out after ${REQUEST_TIMEOUT_MS / 1000}s (${method} ${path})`);
        if (attempt < maxRetries) continue;
        throw lastError;
      }
      throw err;
    }

    if (resp.ok) return JSON.parse(body) as T;

    if (GET_RETRY_STATUS.includes(resp.status) && attempt < maxRetries) {
      lastError = toApiError(resp.status, body);
      continue;
    }
    throw toApiError(resp.status, body);
  }
  throw lastError ?? new Error('Octagon reports API request failed');
}

/**
 * List report versions for an event; pass `version` ('latest' or a run_id)
 * to also receive `markdown_report`.
 */
export async function fetchReportVersions(
  eventTicker: string,
  opts?: { version?: string },
): Promise<ReportVersionsResponse> {
  const qs = opts?.version ? `?version=${encodeURIComponent(opts.version)}` : '';
  return requestJson<ReportVersionsResponse>(
    'GET',
    `/predictions/reports/kalshi/${encodeURIComponent(eventTicker)}${qs}`,
    { retry: true },
  );
}

/** Fetch the latest cached report body, or null when none has been generated. */
export async function fetchLatestReportMarkdown(eventTicker: string): Promise<{
  markdown: string | null;
  runId: string | null;
  versions: ReportVersion[];
}> {
  const res = await fetchReportVersions(eventTicker, { version: 'latest' });
  return { markdown: res.markdown_report, runId: res.run_id, versions: res.versions };
}

/**
 * The Trust Index scorecard of the latest report version. 404s carry a code
 * naming what is missing: report_not_found or trust_index_not_found.
 */
export async function fetchTrustIndex(
  eventTicker: string,
  opts?: { expand?: TrustExpand[] },
): Promise<TrustIndexResponse> {
  const qs = opts?.expand?.length ? `?expand=${opts.expand.join(',')}` : '';
  return requestJson<TrustIndexResponse>(
    'GET',
    `/predictions/reports/kalshi/${encodeURIComponent(eventTicker)}/trust${qs}`,
    { retry: true },
  );
}

export async function fetchReportRunStatus(runId: string): Promise<ReportRunStatus> {
  return requestJson<ReportRunStatus>('GET', `/predictions/reports/status/${encodeURIComponent(runId)}`, { retry: true });
}

/**
 * Trigger fresh generation (3 credits, charged on 202; refunded on failure).
 * Not retried: a retry after an ambiguous failure could double-charge.
 */
export async function triggerReportGeneration(eventTicker: string): Promise<ReportGenerationAccepted> {
  return requestJson<ReportGenerationAccepted>('POST', `/predictions/reports/kalshi/${encodeURIComponent(eventTicker)}`);
}

/**
 * Trigger fresh generation and poll until the run completes, then return the
 * markdown pinned to that run. Docs recommend polling every 30-60s; fresh
 * generation typically takes several minutes.
 */
export async function generateReportAndWait(
  eventTicker: string,
  opts?: {
    pollIntervalMs?: number;
    timeoutMs?: number;
    onProgress?: (msg: string) => void;
  },
): Promise<{ markdown: string; runId: string; envelope: ReportVersionsResponse }> {
  const pollInterval = sanitizeMs(opts?.pollIntervalMs, 30_000);
  const timeoutMs = sanitizeMs(opts?.timeoutMs, 600_000);

  // Snapshot the current latest run BEFORE triggering: if the POST fails
  // ambiguously (gateway 502/504/524 or a client-side timeout), the run has
  // often started server-side anyway — the report "lands" as a new version.
  // Knowing the pre-POST latest run_id lets us recover by watching for a
  // version we haven't seen instead of surfacing a false-negative error.
  let baselineRunId: string | null = null;
  let baselineKnown = false;
  try {
    const before = await fetchReportVersions(eventTicker);
    baselineRunId = before.versions[0]?.run_id ?? null;
    baselineKnown = true;
  } catch {
    // Baseline unknown: recovery must not run, because without knowing the
    // pre-POST latest run we could hand back an unchanged cached report as
    // if it were the fresh one.
  }

  let accepted: ReportGenerationAccepted;
  try {
    accepted = await triggerReportGeneration(eventTicker);
  } catch (err) {
    if (!isAmbiguousGenerationFailure(err) || !baselineKnown) throw err;
    opts?.onProgress?.(
      `Generation POST failed ambiguously (${err instanceof Error ? err.message.slice(0, 80) : err}); ` +
      `watching ?version=latest for the run to land anyway...`,
    );
    return recoverFromLatest(eventTicker, baselineRunId, pollInterval, timeoutMs, opts?.onProgress);
  }
  opts?.onProgress?.(`Generation started (run ${accepted.run_id}). Polling every ${Math.round(pollInterval / 1000)}s...`);

  const deadline = Date.now() + timeoutMs;
  for (;;) {
    await sleepUntil(pollInterval, deadline);
    const status = await fetchReportRunStatus(accepted.run_id);
    if (status.status === 'completed') break;
    if (status.status === 'failed') {
      throw new Error(`Octagon report generation failed for ${eventTicker} (run ${accepted.run_id}). Credits are refunded automatically.`);
    }
    if (Date.now() > deadline) {
      throw new Error(
        `Octagon report generation timed out after ${Math.round(timeoutMs / 1000)}s (run ${accepted.run_id}, still processing). ` +
        `Check later with the status endpoint or fetch ?version=latest.`,
      );
    }
    opts?.onProgress?.(`Still processing (run ${accepted.run_id})...`);
  }

  const res = await fetchReportVersions(accepted.event_ticker || eventTicker, { version: accepted.run_id });
  if (!res.markdown_report) {
    throw new Error(`Report run ${accepted.run_id} completed but no markdown was returned for ${eventTicker}.`);
  }
  return { markdown: res.markdown_report, runId: accepted.run_id, envelope: res };
}

/** Positive finite ms value or the default. */
function sanitizeMs(value: number | undefined, fallback: number): number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : fallback;
}

/** Sleep `intervalMs`, but never past `deadline`. */
async function sleepUntil(intervalMs: number, deadline: number): Promise<void> {
  const delay = Math.max(0, Math.min(intervalMs, deadline - Date.now()));
  if (delay > 0) await new Promise((r) => setTimeout(r, delay));
}

/**
 * A generation POST failure is "ambiguous" when the request may have reached
 * the service even though we got no usable answer: gateway errors (502/504,
 * Cloudflare 522/524), 503, or a client-side timeout. Definite rejections
 * (400/401/403/404/409/429) are never recovered from.
 */
export function isAmbiguousGenerationFailure(err: unknown): boolean {
  if (err instanceof OctagonReportsApiError) {
    return [502, 503, 504, 522, 524].includes(err.statusCode);
  }
  return err instanceof Error && /timed out/i.test(err.message);
}

/** Poll ?version=latest until a run different from `baselineRunId` lands. */
async function recoverFromLatest(
  eventTicker: string,
  baselineRunId: string | null,
  pollInterval: number,
  timeoutMs: number,
  onProgress?: (msg: string) => void,
): Promise<{ markdown: string; runId: string; envelope: ReportVersionsResponse }> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    await sleepUntil(pollInterval, deadline);
    let latest: ReportVersionsResponse | null = null;
    try {
      latest = await fetchReportVersions(eventTicker, { version: 'latest' });
    } catch (err) {
      // Transient gateway/timeout failures keep polling; definitive API
      // answers (401/403/404, malformed keys) will not improve with time.
      if (err instanceof OctagonReportsApiError && !GET_RETRY_STATUS.includes(err.statusCode)) {
        throw err;
      }
    }
    const newRun = latest?.versions[0]?.run_id;
    if (latest?.markdown_report && newRun && newRun !== baselineRunId) {
      onProgress?.(`Recovered: fresh report landed as run ${newRun}.`);
      return { markdown: latest.markdown_report, runId: newRun, envelope: latest };
    }
    if (Date.now() > deadline) {
      throw new Error(
        `Octagon report generation for ${eventTicker} failed and no new version landed within ` +
        `${Math.round(timeoutMs / 1000)}s. If credits were charged for a failed run they are refunded automatically.`,
      );
    }
    onProgress?.('No new version yet; still watching...');
  }
}
