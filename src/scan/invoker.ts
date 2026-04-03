import type { OctagonInvoker, OctagonVariant } from './types.js';
import { callKalshiApi, KalshiApiError } from '../tools/kalshi/api.js';
import { logger } from '../utils/logger.js';

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
function extractTextFromResponse(data: unknown): string {
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
 * Call the Octagon API with a Kalshi market URL or ticker.
 * Octagon only accepts full Kalshi URLs (e.g. https://kalshi.com/markets/series/event/ticker).
 * If a ticker is passed, it will be resolved to a URL via the Kalshi API.
 */
export async function callOctagon(input: string, variant: OctagonVariant): Promise<string> {
  const apiKey = process.env.OCTAGON_API_KEY;
  const baseUrl = process.env.OCTAGON_BASE_URL ?? 'https://api-gateway.octagonagents.com/v1';

  if (!apiKey) throw new Error('OCTAGON_API_KEY not set. Get one at https://app.octagonai.co');

  const model = variant === 'default'
    ? 'octagon-prediction-markets-agent'
    : `octagon-prediction-markets-agent:${variant}`;

  // Octagon requires a full Kalshi URL — resolve tickers to URLs
  const marketUrl = input.startsWith('https://kalshi.com/')
    ? input
    : await buildKalshiMarketUrl(input);

  // Refresh reports can take several minutes to generate; cache is fast
  const timeoutMs = variant === 'cache' ? 60_000 : 600_000;
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

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    let resp: Response;
    try {
      resp = await fetch(`${baseUrl}/responses`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: reqBody,
        signal: controller.signal,
      });
    } catch (err) {
      clearTimeout(timer);
      if (err instanceof DOMException && err.name === 'AbortError') {
        const secs = Math.round(timeoutMs / 1000);
        throw new Error(
          `Octagon API timed out after ${secs}s. The ${variant} report is taking longer than expected. ` +
          `Try again later or use cached data (omit --refresh).`
        );
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }

    if (resp.ok) {
      const data = await resp.json();
      return extractTextFromResponse(data);
    }

    // Retry on 502/503/504 gateway errors
    if ([502, 503, 504].includes(resp.status) && attempt < MAX_RETRIES) {
      const body = await resp.text().catch(() => '');
      const isHtml = body.trimStart().startsWith('<');
      const detail = isHtml ? '' : body.slice(0, 200);
      lastError = new Error(`${resp.status} ${resp.statusText}${detail ? ` — ${detail}` : ''}`);
      continue;
    }

    // Non-retryable error or retries exhausted
    const body = await resp.text().catch(() => '');
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
