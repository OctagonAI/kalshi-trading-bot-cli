import { callKalshiApi, KalshiApiError } from '../tools/kalshi/api.js';
import { logger } from '../utils/logger.js';
import { fetchWithDeadline } from '../utils/http.js';
import type { OctagonInvoker, OctagonVariant } from './types.js';
import { fetchReportVersions, generateReportAndWait, OctagonReportsApiError } from './octagon-reports-api.js';
import { looksLikeTicker } from '../commands/similar.js';

/**
 * Slugify a title for Kalshi website URL paths.
 */
function slugify(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

/** Cache series slug lookups to avoid redundant API calls */
const seriesSlugCache = new Map<string, string>(); // series_ticker → slug

/**
 * Build a Kalshi market URL that Octagon can resolve.
 * Kalshi website URLs use the format: /markets/{series_ticker}/{series_title_slug}/{event_ticker}
 * Octagon needs this exact format — it cannot follow client-side redirects.
 */
async function buildKalshiMarketUrl(ticker: string): Promise<string> {
  let market: unknown;
  try {
    market = await callKalshiApi('GET', `/markets/${ticker}`);
  } catch (err) {
    if (err instanceof KalshiApiError && err.statusCode === 404) {
      throw new Error(`Market ticker '${ticker}' not found on Kalshi. Use kalshi_search to find valid tickers.`);
    }
    throw err;
  }
  const data = ((market as any).market ?? market) as Record<string, unknown>;
  const eventTicker = data.event_ticker as string | undefined;
  if (!eventTicker) throw new Error(`No event_ticker found for market ${ticker}`);

  // Get series info (series_ticker + title for slug)
  const eventRes = await callKalshiApi('GET', `/events/${eventTicker}`);
  const ev = ((eventRes as any).event ?? eventRes) as Record<string, unknown>;
  const seriesTicker = ev.series_ticker as string | undefined;
  if (!seriesTicker) throw new Error(`No series_ticker found for event ${eventTicker}`);

  // Check slug cache
  let slug = seriesSlugCache.get(seriesTicker);
  if (!slug) {
    const seriesRes = await callKalshiApi('GET', `/series/${seriesTicker}`);
    const ser = ((seriesRes as any).series ?? seriesRes) as Record<string, unknown>;
    const seriesTitle = ser.title as string | undefined;
    if (!seriesTitle) throw new Error(`No title found for series ${seriesTicker}`);
    slug = slugify(seriesTitle);
    seriesSlugCache.set(seriesTicker, slug);
  }

  return `https://kalshi.com/markets/${seriesTicker.toLowerCase()}/${slug}/${eventTicker.toLowerCase()}`;
}

/**
 * Extract text content from an OpenAI-compatible responses API result.
 */
export function extractTextFromResponse(data: unknown): string {
  if (!data || typeof data !== 'object') return String(data);

  const obj = data as Record<string, unknown>;

  // OpenAI responses format: { output: [{ type: "message", content: [{ type: "output_text", text: "..." }] }] }
  if (Array.isArray(obj.output)) {
    for (const item of obj.output) {
      if (item && typeof item === 'object') {
        const entry = item as Record<string, unknown>;
        if (Array.isArray(entry.content)) {
          for (const block of entry.content) {
            if (block && typeof block === 'object') {
              const b = block as Record<string, unknown>;
              if (b.type === 'output_text' && typeof b.text === 'string') {
                return b.text;
              }
            }
          }
        }
        // Direct text field
        if (typeof entry.text === 'string') return entry.text;
      }
    }
  }

  // Chat completions format: { choices: [{ message: { content: "..." } }] }
  if (Array.isArray(obj.choices)) {
    const first = obj.choices[0] as Record<string, unknown> | undefined;
    if (first?.message && typeof first.message === 'object') {
      const msg = first.message as Record<string, unknown>;
      if (typeof msg.content === 'string') return msg.content;
    }
  }

  // Direct output_text field
  if (typeof obj.output_text === 'string') return obj.output_text;

  // Fallback
  return JSON.stringify(data);
}

/**
 * Resolve any accepted input (kalshi.com URL, market ticker, or event ticker)
 * to the event ticker the Reports API is addressed by.
 */
export async function resolveEventTicker(input: string): Promise<string> {
  let candidate = input;
  if (input.startsWith('https://kalshi.com/')) {
    // URL formats end in an event ticker, a market ticker (tool-built URLs),
    // or a series slug — extract the last segment and resolve it below like
    // any bare ticker, so market-ticker URLs land on the parent event.
    const last = input.split('?')[0].split('/').filter(Boolean).pop();
    if (!last) throw new Error(`Could not extract an event ticker from URL: ${input}`);
    candidate = last;
  }
  const ticker = candidate.toUpperCase();
  try {
    const market = await callKalshiApi('GET', `/markets/${ticker}`);
    const data = ((market as any).market ?? market) as Record<string, unknown>;
    if (typeof data.event_ticker === 'string') return data.event_ticker;
  } catch (err) {
    if (!(err instanceof KalshiApiError && err.statusCode === 404)) throw err;
    // Not a market ticker — assume it's already an event ticker.
  }
  return ticker;
}

/**
 * Call Octagon for a report or a conversational query.
 *
 * - 'cache' and 'refresh' use the Reports API (/predictions/reports/kalshi):
 *   cache pulls ?version=latest, refresh POSTs a generation run and polls it.
 *   A cache miss returns the JSON envelope '{"versions": []}' so downstream
 *   cache-miss detection keeps working.
 * - 'default' sends the input to the conversational Prediction Markets Agent
 *   over the OpenAI-compatible /responses endpoint.
 */
export async function callOctagon(input: string, variant: OctagonVariant): Promise<string> {
  if (variant === 'cache' || variant === 'refresh') {
    const eventTicker = await resolveEventTicker(input);
    // Return the full Reports API envelope (versions metadata + markdown_report)
    // rather than the bare markdown: the structured model_probability /
    // outcome_probabilities are what downstream parseReport extracts real
    // probabilities from — markdown regex extraction is the fallback that
    // produced 0.5-placeholder edges.
    if (variant === 'cache') {
      try {
        const res = await fetchReportVersions(eventTicker, { version: 'latest' });
        if (res.markdown_report) return JSON.stringify(res);
        return JSON.stringify({ versions: res.versions ?? [] });
      } catch (err) {
        // An unknown event is a cache MISS, not an error — the legacy :cache
        // variant guaranteed the empty-versions envelope, and scan/analyze
        // key their miss-triggered refresh logic off it.
        if (err instanceof OctagonReportsApiError && err.statusCode === 404) {
          return JSON.stringify({ versions: [] });
        }
        throw err;
      }
    }
    const { envelope } = await generateReportAndWait(eventTicker, {
      onProgress: (msg) => logger.info(`[octagon] ${msg}`),
    });
    return JSON.stringify(envelope);
  }

  const apiKey = process.env.OCTAGON_API_KEY;
  const baseUrl = process.env.OCTAGON_BASE_URL ?? 'https://api.octagonai.co/v1';

  if (!apiKey) throw new Error('OCTAGON_API_KEY not set. Get one at https://app.octagonai.co');

  const model = 'octagon-prediction-markets-agent';

  // The conversational agent accepts URLs, tickers, and natural language —
  // pass free text through, but canonicalize bare market tickers to URLs.
  // Uses the strict ticker shape (hyphenated, e.g. KXFED-26SEP-T3) so a
  // single conversational word like "Explain" is never treated as a ticker.
  const marketUrl = input.startsWith('https://kalshi.com/') || !looksLikeTicker(input.trim())
    ? input
    : await buildKalshiMarketUrl(input.trim());

  const timeoutMs = 600_000;
  const reqBody = JSON.stringify({ model, input: marketUrl });
  const MAX_RETRIES = 3;
  const RETRY_DELAYS = [15_000, 30_000, 60_000]; // 15s, 30s, 60s

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_DELAYS[attempt - 1];
      logger.info(`[octagon] Returned ${lastError?.message?.match(/\d{3}/)?.[0] ?? '5xx'}, retrying in ${delay / 1000}s (attempt ${attempt + 1}/${MAX_RETRIES + 1})`);
      await new Promise((r) => setTimeout(r, delay));
    }

    // The deadline stays armed through the body read, so read the body inside
    // this try too: an abort there is the same timeout, not a raw AbortError.
    let resp: Response;
    let body: string;
    try {
      resp = await fetchWithDeadline(`${baseUrl}/responses`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: reqBody,
      }, timeoutMs);
      body = await resp.text();
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        const secs = Math.round(timeoutMs / 1000);
        throw new Error(
          `Octagon API timed out after ${secs}s. The agent is taking longer than expected. Try again later.`
        );
      }
      throw err;
    }

    if (resp.ok) {
      const data = JSON.parse(body);
      return extractTextFromResponse(data);
    }

    // Retry on 502/503/504 gateway errors
    if ([502, 503, 504].includes(resp.status) && attempt < MAX_RETRIES) {
      const isHtml = body.trimStart().startsWith('<');
      const detail = isHtml ? '' : body.slice(0, 200);
      lastError = new Error(`${resp.status} ${resp.statusText}${detail ? ` — ${detail}` : ''}`);
      continue;
    }

    // Non-retryable error or retries exhausted
    const isHtml = body.trimStart().startsWith('<');
    const detail = isHtml ? '' : body.slice(0, 200);
    const maskedKey = apiKey!.length > 4 ? '...' + apiKey!.slice(-4) : '****';
    const curl = `curl -X POST '${baseUrl}/responses' \\\n  -H 'Authorization: Bearer ${maskedKey}' \\\n  -H 'Content-Type: application/json' \\\n  -d '${reqBody}'`;
    throw new Error(
      `Octagon API error: ${resp.status} ${resp.statusText}${detail ? ` — ${detail}` : ''}\n\nReproduce with:\n${curl}`
    );
  }

  // Should not reach here, but satisfy TypeScript
  throw lastError ?? new Error('Octagon API request failed');
}

/**
 * Factory for the OctagonInvoker used by ScanLoop.
 * Calls the Octagon Prediction Markets Agent API (OpenAI-compatible).
 */
export function createOctagonInvoker(): OctagonInvoker {
  return async (ticker: string, variant: OctagonVariant): Promise<string> => {
    return callOctagon(ticker, variant);
  };
}
