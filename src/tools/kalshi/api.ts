import { createSign, constants } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { logger } from '../../utils/logger.js';
import { auditTrail } from '../../audit/index.js';
import { fetchWithDeadline } from '../../utils/http.js';
import { dlqWriter } from './dlq.js';
import type { KalshiMarket } from './types.js';

const PROD_BASE_URL = 'https://api.elections.kalshi.com/trade-api/v2';
const DEMO_BASE_URL = 'https://demo-api.kalshi.co/trade-api/v2';

function getBaseUrl(): string {
  return process.env.KALSHI_USE_DEMO === 'true' ? DEMO_BASE_URL : PROD_BASE_URL;
}

function getPrivateKey(): string {
  if (process.env.KALSHI_PRIVATE_KEY) {
    return process.env.KALSHI_PRIVATE_KEY;
  }
  if (process.env.KALSHI_PRIVATE_KEY_FILE) {
    return readFileSync(process.env.KALSHI_PRIVATE_KEY_FILE, 'utf-8');
  }
  throw new Error('Kalshi private key not configured. Set KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_FILE.');
}

function getApiKey(): string {
  const key = process.env.KALSHI_API_KEY;
  if (!key) throw new Error('KALSHI_API_KEY not set');
  return key;
}

function buildSignature(method: string, path: string): { timestamp: string; signature: string } {
  const timestamp = Date.now().toString();
  const message = timestamp + method.toUpperCase() + path;

  const privateKey = getPrivateKey();
  const sign = createSign('SHA256');
  sign.update(message);
  sign.end();

  const signature = sign.sign(
    {
      key: privateKey,
      padding: constants.RSA_PKCS1_PSS_PADDING,
      saltLength: 32,
    },
    'base64'
  );

  return { timestamp, signature };
}

// --- Error class ---

export class KalshiApiError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly statusText: string,
    public readonly body: string
  ) {
    super(`Kalshi API error: ${statusCode} ${statusText}${body ? ` — ${body}` : ''}`);
    this.name = 'KalshiApiError';
  }
}

// --- Dollar conversion utilities ---

export function toDollarString(cents: number): string {
  return (cents / 100).toFixed(2);
}

export function fromDollarString(dollar: string): number {
  return Math.round(parseFloat(dollar) * 100);
}

export function supportsFractional(market: KalshiMarket): boolean {
  return market.supports_fractional === true || market.tick_size < 1;
}

// --- Retry logic ---

interface RetryContext {
  method: string;
  path: string;
  body?: Record<string, unknown>;
}

const MAX_RETRIES = 5;
const REQUEST_TIMEOUT_MS = 30_000;
const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 120_000;
const JITTER_FACTOR = 0.2;

function isRetryable(error: unknown): boolean {
  if (!(error instanceof KalshiApiError)) return false;
  if (error.statusCode === 429) return true;
  if (error.statusCode >= 500) return true;
  return false;
}

function computeDelay(attempt: number): number {
  const base = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
  const jitter = base * JITTER_FACTOR * (2 * Math.random() - 1);
  return Math.max(0, base + jitter);
}

async function withRetry<T>(fn: () => Promise<T>, context: RetryContext): Promise<T> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (!isRetryable(error) || attempt === MAX_RETRIES) {
        if (attempt > 0 && error instanceof KalshiApiError) {
          // Exhausted retries — write to DLQ
          dlqWriter.append({
            method: context.method,
            path: context.path,
            body: context.body,
            error: error.message,
            attempts: attempt + 1,
          });
          auditTrail.log({
            type: 'DLQ_ENTRY',
            method: context.method,
            path: context.path,
            error: error.message,
            attempts: attempt + 1,
          });
        }
        throw error;
      }

      const apiError = error as KalshiApiError;
      const delay = computeDelay(attempt);

      auditTrail.log({
        type: 'API_RETRY',
        method: context.method,
        path: context.path,
        attempt: attempt + 1,
        max_retries: MAX_RETRIES,
        status_code: apiError.statusCode,
        delay_ms: Math.round(delay),
      });

      logger.warn(
        `[Kalshi API] ${apiError.statusCode} on ${context.method} ${context.path}, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${MAX_RETRIES})`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  throw new Error('Max retries exceeded');
}

// --- Public API ---

export interface KalshiApiResponse {
  [key: string]: unknown;
}

export async function callKalshiApi(
  method: 'GET' | 'POST' | 'PUT' | 'DELETE',
  path: string,
  options?: {
    params?: Record<string, string | number | boolean | string[] | undefined>;
    body?: Record<string, unknown>;
  }
): Promise<KalshiApiResponse> {
  return withRetry(
    async () => {
      const baseUrl = getBaseUrl();

      // Build URL with query params
      const url = new URL(`${baseUrl}${path}`);
      if (options?.params) {
        for (const [key, value] of Object.entries(options.params)) {
          if (value !== undefined && value !== null) {
            if (Array.isArray(value)) {
              value.forEach((v) => url.searchParams.append(key, String(v)));
            } else {
              url.searchParams.append(key, String(value));
            }
          }
        }
      }

      // Sign the request using only the path (no query string)
      // Kalshi signature covers: timestamp + method + path (without query params)
      const pathWithQuery = url.pathname;
      const { timestamp, signature } = buildSignature(method, pathWithQuery);

      const headers: Record<string, string> = {
        'KALSHI-ACCESS-KEY': getApiKey(),
        'KALSHI-ACCESS-SIGNATURE': signature,
        'KALSHI-ACCESS-TIMESTAMP': timestamp,
        'Content-Type': 'application/json',
      };

      const fetchOptions: RequestInit = { method, headers };

      if (options?.body && (method === 'POST' || method === 'PUT' || method === 'DELETE')) {
        fetchOptions.body = JSON.stringify(options.body);
      }

      // Deadline is required: without one a half-open connection hangs forever
      // and the retry ladder above never sees a rejection.
      const response = await fetchWithDeadline(url.toString(), fetchOptions, REQUEST_TIMEOUT_MS);

      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new KalshiApiError(response.status, response.statusText, text);
      }

      // 204 No Content — DELETE operations often return this
      if (response.status === 204) {
        return {};
      }

      return response.json();
    },
    { method, path, body: options?.body }
  );
}

/**
 * Fetch all pages of a cursor-paginated endpoint.
 */
export async function fetchAllPages<T>(
  path: string,
  params: Record<string, string | number | boolean | undefined>,
  dataKey: string,
  maxPages = 10,
  onProgress?: (info: { fetchedItems: number; page: number; maxPages: number }) => void
): Promise<T[]> {
  const results: T[] = [];
  let cursor: string | undefined;
  let page = 0;

  while (page < maxPages) {
    const response = await callKalshiApi('GET', path, {
      params: cursor ? { ...params, cursor } : params,
    });

    const data = response[dataKey] as T[] | undefined;
    if (!data || data.length === 0) break;

    results.push(...data);
    cursor = response.cursor as string | undefined;
    page++;
    onProgress?.({ fetchedItems: results.length, page, maxPages });
    if (!cursor) break;
  }

  // Hitting the cap with a cursor still open means the result set is truncated.
  // Say so: callers that sum these rows (e.g. open exposure) would otherwise
  // understate the total silently.
  if (cursor && page >= maxPages) {
    logger.warn(
      `[Kalshi API] ${path} truncated at the ${maxPages}-page cap with more pages available (${results.length} ${dataKey} so far)`
    );
  }

  return results;
}

/**
 * Page through a cursor-paginated endpoint, handing each page to `onPage`
 * instead of accumulating it.
 *
 * Separate from fetchAllPages rather than a flag on it, for two reasons: the
 * caller usually wants the rows written as they land (so the index is usable
 * before the last page arrives), and the full open-events set with nested
 * markets is ~190 MB of JSON — holding all of it to return one array is what
 * the 20-page cap was quietly protecting against.
 *
 * Returns the number of rows handled, and whether the cap cut the walk short.
 */
export async function streamAllPages<T>(
  path: string,
  params: Record<string, string | number | boolean | undefined>,
  dataKey: string,
  maxPages: number,
  onPage: (rows: T[]) => void | Promise<void>,
  onProgress?: (info: { fetchedItems: number; page: number; maxPages: number }) => void
): Promise<{ count: number; truncated: boolean }> {
  let cursor: string | undefined;
  let page = 0;
  let count = 0;

  while (page < maxPages) {
    const response = await callKalshiApi('GET', path, {
      params: cursor ? { ...params, cursor } : params,
    });

    const data = response[dataKey] as T[] | undefined;
    if (!data || data.length === 0) break;

    await onPage(data);
    count += data.length;
    cursor = response.cursor as string | undefined;
    page++;
    onProgress?.({ fetchedItems: count, page, maxPages });
    if (!cursor) break;
  }

  const truncated = Boolean(cursor) && page >= maxPages;
  if (truncated) {
    logger.warn(
      `[Kalshi API] ${path} truncated at the ${maxPages}-page cap with more pages available (${count} ${dataKey} so far)`
    );
  }

  return { count, truncated };
}
