/**
 * A single event entry from the Octagon Prediction Markets Events API.
 * Probabilities are percentages (0-100).
 */
export interface OctagonEventEntry {
  history_id: number;
  run_id: string;
  captured_at: string;
  event_ticker: string;
  name: string;
  slug: string;
  image_url?: string;
  series_category: string;
  available_on_brokers: boolean;
  mutually_exclusive: boolean;
  analysis_last_updated: string;
  confidence_score: number;
  model_probability: number;
  market_probability: number;
  edge_pp: number;
  expected_return: number;
  r_score: number;
  total_volume: number;
  total_open_interest: number;
  close_time: string;
  key_takeaway: string;
  has_history?: boolean;
  outcome_probabilities?: Array<{
    market_ticker: string;
    outcome_name?: string;
    model_probability: number;
    market_probability: number;
    volume?: number | null;
    volume_24h?: number | null;
  }> | null;
  current_state_summary_richtext?: string;
  short_answer_richtext?: string;
  executive_summary_richtext?: string;
  /**
   * Trader Trust scorecard fields (added in calculation_version v1.0+).
   * Null on reports generated before this shipped — callers must guard.
   */
  trader_trust_subtitle?: string | null;
  /** Pre-rendered HTML; the CLI ignores this and reads trader_trust_json. */
  trader_trust_richtext?: string | null;
  /** JSON-encoded string. See TraderTrustCard in src/commands/trust.ts. */
  trader_trust_json?: string | null;
}

interface EventsPage {
  data: OctagonEventEntry[];
  next_cursor: string | null;
  has_more: boolean;
}

const EVENTS_API_BASE = 'https://api.octagonai.co/v1';
const PAGE_LIMIT = 200;
const TIMEOUT_MS = 60_000;

/**
 * Fetch a single page of events with optional filters. Useful for CLI commands
 * that don't need the full universe — e.g. `events list --limit 50`.
 */
export async function fetchOctagonEventsPage(opts?: {
  limit?: number;
  cursor?: string | null;
  hasHistory?: boolean;
}): Promise<{ data: OctagonEventEntry[]; next_cursor: string | null; has_more: boolean }> {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) throw new Error('OCTAGON_API_KEY not set');

  const params = new URLSearchParams({ limit: String(opts?.limit ?? PAGE_LIMIT) });
  if (opts?.hasHistory) params.set('has_history', 'true');
  if (opts?.cursor) params.set('cursor', opts.cursor);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let resp: Response;
  try {
    resp = await fetch(`${EVENTS_API_BASE}/prediction-markets/events?${params}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`Octagon events API ${resp.status}: ${body.slice(0, 200)}`);
  }
  const page = (await resp.json()) as { data?: OctagonEventEntry[]; next_cursor?: string | null; has_more?: boolean };
  return {
    data: Array.isArray(page.data) ? page.data : [],
    next_cursor: page.next_cursor ?? null,
    has_more: !!page.has_more,
  };
}

/**
 * Look up a single event by ticker via the dedicated endpoint
 * GET /v1/prediction-markets/events/{event_ticker}. Returns null on 404.
 * Cheaper than `fetchOctagonEventByTicker` which scans paginated pages.
 */
export async function fetchOctagonEventDirect(eventTicker: string): Promise<OctagonEventEntry | null> {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) throw new Error('OCTAGON_API_KEY not set');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let resp: Response;
  try {
    resp = await fetch(`${EVENTS_API_BASE}/prediction-markets/events/${encodeURIComponent(eventTicker)}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
  if (resp.status === 404) return null;
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`Octagon event lookup ${resp.status}: ${body.slice(0, 200)}`);
  }
  return (await resp.json()) as OctagonEventEntry;
}

/**
 * Look up a single event by ticker. Scans pages until found (universe is small).
 * Returns null if not found.
 */
export async function fetchOctagonEventByTicker(eventTicker: string): Promise<OctagonEventEntry | null> {
  let cursor: string | null = null;
  do {
    const page: { data: OctagonEventEntry[]; next_cursor: string | null; has_more: boolean } =
      await fetchOctagonEventsPage({ cursor });
    const hit = page.data.find((e) => e.event_ticker === eventTicker);
    if (hit) return hit;
    cursor = page.has_more ? page.next_cursor : null;
  } while (cursor);
  return null;
}

/**
 * Fetch all events from the Octagon Prediction Markets Events API,
 * paginating through all pages.
 * @param opts.hasHistory - When true, only return events with multiple historical snapshots.
 *   Note: The events list endpoint now returns `has_history` per event, so this filter
 *   is only needed if you want to reduce response size.
 */
export async function fetchAllOctagonEvents(opts?: { hasHistory?: boolean }): Promise<OctagonEventEntry[]> {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) throw new Error('OCTAGON_API_KEY not set');

  const all: OctagonEventEntry[] = [];
  let cursor: string | null = null;

  do {
    const params = new URLSearchParams({ limit: String(PAGE_LIMIT) });
    if (opts?.hasHistory) params.set('has_history', 'true');
    if (cursor) params.set('cursor', cursor);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

    let resp: Response;
    try {
      resp = await fetch(`${EVENTS_API_BASE}/prediction-markets/events?${params}`, {
        headers: { Authorization: `Bearer ${apiKey}` },
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      throw new Error(`Octagon events API ${resp.status}: ${body.slice(0, 200)}`);
    }

    const page = (await resp.json()) as unknown;
    if (!page || typeof page !== 'object') {
      throw new Error('Octagon events API returned invalid response shape');
    }
    const p = page as Record<string, unknown>;
    if (!Array.isArray(p.data)) {
      throw new Error('Octagon events API response missing data array');
    }
    const hasMore = typeof p.has_more === 'boolean' ? p.has_more : false;
    if (hasMore && !p.next_cursor) {
      throw new Error('Octagon events API has_more=true but next_cursor is missing');
    }
    all.push(...(p.data as OctagonEventEntry[]));
    cursor = hasMore ? (p.next_cursor as string) : null;
  } while (cursor);

  return all;
}
