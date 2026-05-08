#!/usr/bin/env bun
/**
 * One-off backfill: push the top-N highest-volume open Kalshi events
 * (in priority categories, not already covered by Octagon) through the
 * Octagon prediction-markets agent, sequentially.
 *
 * Run: bun scripts/backfill-octagon.ts [--limit 500] [--dry-run] [--refresh] [--retry-failed] [--categories Crypto,Politics,...]
 *
 * Output: one JSONL file per run under scripts/backfill-octagon-out/.
 *   - default runs write to backfill-<stamp>.jsonl
 *   - --refresh runs write to refresh-<stamp>.jsonl
 *   - --retry-failed runs write to retry-<stamp>.jsonl
 * Resumable: default runs skip events present in any prior backfill-*.jsonl,
 * refresh-*.jsonl, or retry-*.jsonl; refresh runs only skip events present in
 * prior refresh-*.jsonl/retry-*.jsonl (so old cached results don't block a
 * fresh refresh).
 * --retry-failed re-reads all JSONL output files and re-attempts every
 * event_ticker whose latest row is an error (i.e. it has no success record
 * yet). Reuses the original variant from that row.
 */
import 'dotenv/config';
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { fetchAllOctagonEvents } from '../src/scan/octagon-events-api.js';
import { callKalshiApi } from '../src/tools/kalshi/api.js';
import { callOctagon } from '../src/scan/invoker.js';
import type { KalshiEvent, KalshiMarket } from '../src/tools/kalshi/types.js';

// Cache-miss agent calls can take minutes — `api.octagonai.co/v1` (the
// public/events host) hits a Cloudflare 524 after ~100s, so we default to
// the gateway host that the rest of this codebase uses for agent calls.
// Override with `OCTAGON_BASE_URL=...` if needed.
process.env.OCTAGON_BASE_URL ||= 'https://api-gateway.octagonagents.com/v1';

const DEFAULT_CATEGORIES = [
  'Crypto',
  'Politics',
  'Elections',
  'Economics',
  'Financials',
  'Companies',
  'Mentions',
];

const OUT_DIR = join(import.meta.dir, 'backfill-octagon-out');

interface Args {
  limit: number;
  dryRun: boolean;
  refresh: boolean;
  retryFailed: boolean;
  categories: Set<string>;
}

function parseArgs(argv: string[]): Args {
  const out: Args = {
    limit: 500,
    dryRun: false,
    refresh: false,
    retryFailed: false,
    categories: new Set(DEFAULT_CATEGORIES),
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dry-run') out.dryRun = true;
    else if (a === '--refresh') out.refresh = true;
    else if (a === '--retry-failed') out.retryFailed = true;
    else if (a === '--limit') out.limit = Number(argv[++i]);
    else if (a === '--categories') out.categories = new Set(argv[++i].split(',').map((s) => s.trim()).filter(Boolean));
    else if (a === '--help' || a === '-h') {
      console.log(
        'Usage: bun scripts/backfill-octagon.ts [--limit N] [--dry-run] [--refresh] [--retry-failed] [--categories a,b,c]',
      );
      process.exit(0);
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }
  if (!Number.isFinite(out.limit) || out.limit <= 0) throw new Error('--limit must be a positive number');
  return out;
}

function requireEnv(): void {
  const missing: string[] = [];
  if (!process.env.OCTAGON_API_KEY) missing.push('OCTAGON_API_KEY');
  if (!process.env.KALSHI_API_KEY) missing.push('KALSHI_API_KEY');
  if (!process.env.KALSHI_PRIVATE_KEY && !process.env.KALSHI_PRIVATE_KEY_FILE) {
    missing.push('KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_FILE');
  }
  if (missing.length) {
    console.error(`Missing required env vars: ${missing.join(', ')}`);
    process.exit(1);
  }
}

function slugify(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

interface JsonlRow {
  event_ticker?: string;
  series_ticker?: string;
  title?: string;
  category?: string;
  total_volume_24h?: number;
  kalshi_url?: string;
  variant?: 'default' | 'cache' | 'refresh';
  fetched_at?: string;
  octagon_response?: string;
  error?: string;
}

/** Read every line from every *.jsonl in OUT_DIR, optionally filtering by filename prefix. */
function readAllJsonl(prefixes?: string[]): JsonlRow[] {
  const rows: JsonlRow[] = [];
  if (!existsSync(OUT_DIR)) return rows;
  for (const name of readdirSync(OUT_DIR)) {
    if (!name.endsWith('.jsonl')) continue;
    if (prefixes && !prefixes.some((p) => name.startsWith(p))) continue;
    const path = join(OUT_DIR, name);
    const content = readFileSync(path, 'utf-8');
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        rows.push(JSON.parse(line) as JsonlRow);
      } catch {
        // skip malformed lines
      }
    }
  }
  return rows;
}

/**
 * Return event_tickers already processed (any row counts as "done").
 * In default mode we consider all output files (refresh + default + retry runs
 * all count as "covered"). In refresh mode we only consider refresh-*.jsonl /
 * retry-*.jsonl so a re-refresh isn't blocked by stale default-mode results.
 */
function loadCompletedTickers(refreshMode: boolean): Set<string> {
  const prefixes = refreshMode ? ['refresh-', 'retry-'] : undefined;
  const rows = readAllJsonl(prefixes);
  const set = new Set<string>();
  for (const row of rows) {
    if (row.event_ticker) set.add(row.event_ticker);
  }
  return set;
}

/**
 * Find event_tickers whose latest row in any JSONL output is an error AND no
 * success row exists for that ticker. Returns one entry per ticker with the
 * metadata needed to re-call Octagon.
 */
function loadFailedItems(): ProcessItem[] {
  const rows = readAllJsonl();
  // Track per-ticker: any success seen, latest error row.
  const seen = new Map<
    string,
    { hasSuccess: boolean; latestError?: JsonlRow & { fetched_at: string } }
  >();
  for (const row of rows) {
    if (!row.event_ticker) continue;
    const entry = seen.get(row.event_ticker) ?? { hasSuccess: false };
    if (row.octagon_response && !row.error) {
      entry.hasSuccess = true;
    } else if (row.error) {
      const stamp = row.fetched_at ?? '';
      if (!entry.latestError || stamp > entry.latestError.fetched_at) {
        entry.latestError = { ...row, fetched_at: stamp };
      }
    }
    seen.set(row.event_ticker, entry);
  }
  const items: ProcessItem[] = [];
  for (const [ticker, entry] of seen) {
    if (entry.hasSuccess || !entry.latestError) continue;
    const e = entry.latestError;
    if (!e.kalshi_url || !e.series_ticker || !e.title || !e.category) {
      console.warn(`[retry] skipping ${ticker}: prior error row missing url/metadata`);
      continue;
    }
    items.push({
      event_ticker: ticker,
      series_ticker: e.series_ticker,
      title: e.title,
      category: e.category,
      total_volume_24h: typeof e.total_volume_24h === 'number' ? e.total_volume_24h : 0,
      url: e.kalshi_url,
      variant: e.variant ?? 'default',
    });
  }
  return items;
}

interface ProcessItem {
  event_ticker: string;
  series_ticker: string;
  title: string;
  category: string;
  total_volume_24h: number;
  url: string;
  variant: 'default' | 'cache' | 'refresh';
}

/** Fetch every open Kalshi event with nested markets (paginated). */
async function fetchAllOpenKalshiEvents(): Promise<KalshiEvent[]> {
  const events: KalshiEvent[] = [];
  let cursor: string | undefined;
  let page = 0;
  while (true) {
    const params: Record<string, string | number | boolean | undefined> = {
      status: 'open',
      with_nested_markets: true,
      limit: 200,
    };
    if (cursor) params.cursor = cursor;
    const resp = await callKalshiApi('GET', '/events', { params });
    const batch = resp.events as KalshiEvent[] | undefined;
    if (!batch || batch.length === 0) break;
    events.push(...batch);
    page++;
    process.stdout.write(`\r[kalshi] fetched ${events.length} events across ${page} pages...`);
    cursor = resp.cursor as string | undefined;
    if (!cursor) break;
  }
  process.stdout.write('\n');
  return events;
}

/**
 * Sum 24h volume across an event's nested markets.
 * Kalshi's current API returns volume as a fixed-point string `volume_24h_fp`
 * (e.g. "1914.34") rather than the legacy numeric `volume_24h`. Handle both.
 */
function totalVolume24h(ev: KalshiEvent): number {
  if (!ev.markets || ev.markets.length === 0) return 0;
  let sum = 0;
  for (const m of ev.markets as KalshiMarket[]) {
    if (typeof m.volume_24h_fp === 'string') {
      const n = parseFloat(m.volume_24h_fp);
      if (Number.isFinite(n)) sum += n;
    } else if (typeof m.volume_24h === 'number' && Number.isFinite(m.volume_24h)) {
      sum += m.volume_24h;
    }
  }
  return sum;
}

/** Cache series_ticker → slug to avoid repeated /series/{ticker} calls. */
const seriesSlugCache = new Map<string, string>();

async function getSeriesSlug(seriesTicker: string): Promise<string> {
  const cached = seriesSlugCache.get(seriesTicker);
  if (cached) return cached;
  const resp = await callKalshiApi('GET', `/series/${seriesTicker}`);
  const ser = ((resp as Record<string, unknown>).series ?? resp) as Record<string, unknown>;
  const title = ser.title as string | undefined;
  if (!title) throw new Error(`No title for series ${seriesTicker}`);
  const slug = slugify(title);
  seriesSlugCache.set(seriesTicker, slug);
  return slug;
}

async function buildKalshiUrl(ev: KalshiEvent): Promise<string> {
  const slug = await getSeriesSlug(ev.series_ticker);
  return `https://kalshi.com/markets/${ev.series_ticker.toLowerCase()}/${slug}/${ev.event_ticker.toLowerCase()}`;
}

/**
 * Decide whether an error from callOctagon is worth retrying at the script level.
 * callOctagon already retries 502/503/504 a few times internally with short
 * backoffs (15/30/60s). We add an outer retry layer for two reasons:
 *  - Bun's fetch raises a raw "The operation timed out." after ~5min when the
 *    Octagon agent endpoint holds the connection idle; this is *not* caught
 *    by callOctagon's AbortError branch.
 *  - 502s from the agent often mean the remote pipeline is briefly overloaded;
 *    waiting longer than the internal 60s ceiling and retrying tends to work.
 */
function isRetryableOctagonError(msg: string): boolean {
  return (
    /operation timed out/i.test(msg) ||
    /timed? ?out/i.test(msg) ||
    /\b502\b|bad gateway/i.test(msg) ||
    /\b503\b|service unavailable/i.test(msg) ||
    /\b504\b|gateway timeout/i.test(msg) ||
    /server_error/i.test(msg) ||
    /Prediction markets refresh failed/i.test(msg)
  );
}

async function callOctagonWithRetry(
  url: string,
  variant: 'default' | 'cache' | 'refresh',
  attempts = 3,
  backoffsMs = [60_000, 180_000],
): Promise<string> {
  let lastErr: Error | null = null;
  for (let i = 0; i < attempts; i++) {
    try {
      return await callOctagon(url, variant);
    } catch (err) {
      lastErr = err as Error;
      const msg = lastErr.message ?? '';
      const isLast = i === attempts - 1;
      if (isLast || !isRetryableOctagonError(msg)) throw lastErr;
      const wait = backoffsMs[i] ?? backoffsMs[backoffsMs.length - 1] ?? 60_000;
      process.stdout.write(
        `\n  ↳ retryable error (attempt ${i + 1}/${attempts}): ${msg.split('\n')[0].slice(0, 140)}\n  ↳ waiting ${Math.round(wait / 1000)}s before retry...`,
      );
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  throw lastErr ?? new Error('callOctagonWithRetry: unreachable');
}

async function discoverFromKalshi(args: Args): Promise<ProcessItem[]> {
  // 1. Octagon dedupe set — skipped in refresh mode (we want to refresh those too)
  let octagonTickers = new Set<string>();
  if (args.refresh) {
    console.log('[octagon] refresh mode — skipping events-API dedupe');
  } else {
    console.log('[octagon] fetching already-cached event tickers...');
    const octagonEvents = await fetchAllOctagonEvents();
    octagonTickers = new Set(octagonEvents.map((e) => e.event_ticker));
    console.log(`[octagon] already covers ${octagonTickers.size} events`);
  }

  // 2. Local resume set (events already in our JSONL output)
  const completedTickers = loadCompletedTickers(args.refresh);
  if (completedTickers.size > 0) {
    const scope = args.refresh ? 'refresh-/retry-*.jsonl' : 'all *.jsonl';
    console.log(`[backfill] resuming — ${completedTickers.size} events already in ${scope}`);
  }

  // 3. Fetch all open Kalshi events with nested markets
  const allEvents = await fetchAllOpenKalshiEvents();
  console.log(`[kalshi] total open events: ${allEvents.length}`);

  // 4. Filter + sort
  type Ranked = { ev: KalshiEvent; volume: number };
  const ranked: Ranked[] = [];
  let droppedCategory = 0;
  let droppedOctagon = 0;
  let droppedCompleted = 0;
  let droppedNoVolume = 0;
  for (const ev of allEvents) {
    if (!args.categories.has(ev.category)) {
      droppedCategory++;
      continue;
    }
    if (octagonTickers.has(ev.event_ticker)) {
      droppedOctagon++;
      continue;
    }
    if (completedTickers.has(ev.event_ticker)) {
      droppedCompleted++;
      continue;
    }
    const volume = totalVolume24h(ev);
    if (volume <= 0) {
      droppedNoVolume++;
      continue;
    }
    ranked.push({ ev, volume });
  }
  ranked.sort((a, b) => b.volume - a.volume);
  const top = ranked.slice(0, args.limit);

  console.log(
    `[backfill] filtered: kept=${ranked.length} (dropped: category=${droppedCategory}, octagon=${droppedOctagon}, completed=${droppedCompleted}, no_volume=${droppedNoVolume})`,
  );
  console.log(`[backfill] selected top ${top.length} by 24h volume`);

  // 5. Build URLs
  console.log('[backfill] resolving Kalshi URLs (series slug lookups)...');
  const variant: ProcessItem['variant'] = args.refresh ? 'refresh' : 'default';
  const items: ProcessItem[] = [];
  for (const { ev, volume } of top) {
    try {
      const url = await buildKalshiUrl(ev);
      items.push({
        event_ticker: ev.event_ticker,
        series_ticker: ev.series_ticker,
        title: ev.title,
        category: ev.category,
        total_volume_24h: volume,
        url,
        variant,
      });
    } catch (err) {
      console.error(`[backfill] skipping ${ev.event_ticker}: failed to build URL: ${(err as Error).message}`);
    }
  }
  console.log(`[backfill] ${items.length} URLs resolved (${seriesSlugCache.size} unique series)`);
  return items;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  requireEnv();

  if (args.refresh && args.retryFailed) {
    throw new Error('--refresh and --retry-failed are mutually exclusive (retry preserves the variant from the failed row)');
  }

  console.log(
    `[backfill] limit=${args.limit} dryRun=${args.dryRun} refresh=${args.refresh} retryFailed=${args.retryFailed} categories=${[...args.categories].join(',')}`,
  );
  console.log(`[backfill] OCTAGON_BASE_URL=${process.env.OCTAGON_BASE_URL}`);

  // Discovery: Kalshi top-N (default/refresh) or failed-only (retry)
  let items: ProcessItem[];
  let prefix: string;
  if (args.retryFailed) {
    const failed = loadFailedItems();
    console.log(`[retry] found ${failed.length} events with no successful row`);
    items = failed.slice(0, args.limit);
    prefix = 'retry';
  } else {
    items = await discoverFromKalshi(args);
    prefix = args.refresh ? 'refresh' : 'backfill';
  }

  if (items.length === 0) {
    console.log('[backfill] nothing to do.');
    return;
  }

  if (args.dryRun) {
    console.log('\n=== DRY RUN — would process the following events ===');
    for (let i = 0; i < items.length; i++) {
      const it = items[i];
      console.log(
        `${String(i + 1).padStart(3)}. [${it.category}] vol24h=${it.total_volume_24h.toLocaleString()} variant=${it.variant} ${it.event_ticker}`,
      );
      console.log(`     ${it.title}`);
      console.log(`     ${it.url}`);
    }
    return;
  }

  // Sequential Octagon calls, streaming JSONL output
  mkdirSync(OUT_DIR, { recursive: true });
  const runStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const outPath = join(OUT_DIR, `${prefix}-${runStamp}.jsonl`);
  console.log(`[backfill] writing to ${outPath}`);

  let succeeded = 0;
  let failed = 0;
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const start = Date.now();
    const tag = `[${i + 1}/${items.length}] ${it.event_ticker}`;
    process.stdout.write(`${tag} — calling Octagon (${it.variant})...`);
    try {
      const response = await callOctagonWithRetry(it.url, it.variant);
      const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
      const row = {
        event_ticker: it.event_ticker,
        series_ticker: it.series_ticker,
        title: it.title,
        category: it.category,
        total_volume_24h: it.total_volume_24h,
        kalshi_url: it.url,
        variant: it.variant,
        fetched_at: new Date().toISOString(),
        elapsed_sec: Number(elapsedSec),
        octagon_response: response,
      };
      appendFileSync(outPath, JSON.stringify(row) + '\n');
      succeeded++;
      process.stdout.write(` ok (${elapsedSec}s, ${response.length} chars)\n`);
    } catch (err) {
      failed++;
      const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
      const msg = (err as Error).message;
      process.stdout.write(` FAILED (${elapsedSec}s): ${msg.split('\n')[0]}\n`);
      const errRow = {
        event_ticker: it.event_ticker,
        series_ticker: it.series_ticker,
        title: it.title,
        category: it.category,
        total_volume_24h: it.total_volume_24h,
        kalshi_url: it.url,
        variant: it.variant,
        fetched_at: new Date().toISOString(),
        elapsed_sec: Number(elapsedSec),
        error: msg,
      };
      appendFileSync(outPath, JSON.stringify(errRow) + '\n');
    }
  }

  console.log(`\n[backfill] done. succeeded=${succeeded} failed=${failed} output=${outPath}`);
}

main().catch((err) => {
  console.error('[backfill] fatal:', err);
  process.exit(1);
});
