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
  }> | null;
  current_state_summary_richtext?: string;
  short_answer_richtext?: string;
  executive_summary_richtext?: string;
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
  let pageNum = 0;

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
    pageNum++;
  } while (cursor);

  return all;
}
