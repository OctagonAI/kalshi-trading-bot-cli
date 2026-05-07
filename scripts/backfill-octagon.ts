#!/usr/bin/env bun
/**
 * One-off backfill: push the top-N highest-volume open Kalshi events
 * (in priority categories, not already covered by Octagon) through the
 * Octagon prediction-markets agent, sequentially.
 *
 * Run: bun scripts/backfill-octagon.ts [--limit 500] [--dry-run] [--refresh] [--categories Crypto,Politics,...]
 *
 * Output: one JSONL file per run under scripts/backfill-octagon-out/.
 *   - default runs write to backfill-<stamp>.jsonl
 *   - --refresh runs write to refresh-<stamp>.jsonl
 * Resumable: default runs skip events present in any prior backfill-*.jsonl
 * or refresh-*.jsonl; refresh runs only skip events present in prior
 * refresh-*.jsonl (so old cached results don't block a fresh refresh).
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
  categories: Set<string>;
}

function parseArgs(argv: string[]): Args {
  const out: Args = { limit: 500, dryRun: false, refresh: false, categories: new Set(DEFAULT_CATEGORIES) };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dry-run') out.dryRun = true;
    else if (a === '--refresh') out.refresh = true;
    else if (a === '--limit') out.limit = Number(argv[++i]);
    else if (a === '--categories') out.categories = new Set(argv[++i].split(',').map((s) => s.trim()).filter(Boolean));
    else if (a === '--help' || a === '-h') {
      console.log('Usage: bun scripts/backfill-octagon.ts [--limit N] [--dry-run] [--refresh] [--categories a,b,c]');
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

/**
 * Read prior JSONL outputs and return the set of event_tickers already processed.
 * In default mode we consider all output files (refresh + default runs both
 * count as "covered"). In refresh mode we only consider prior refresh-*.jsonl
 * so a re-refresh isn't blocked by stale default-mode results.
 */
function loadCompletedTickers(refreshMode: boolean): Set<string> {
  const set = new Set<string>();
  if (!existsSync(OUT_DIR)) return set;
  for (const name of readdirSync(OUT_DIR)) {
    if (!name.endsWith('.jsonl')) continue;
    if (refreshMode && !name.startsWith('refresh-')) continue;
    const path = join(OUT_DIR, name);
    const content = readFileSync(path, 'utf-8');
    for (const line of content.split('\n')) {
      if (!line.trim()) continue;
      try {
        const row = JSON.parse(line) as { event_ticker?: string };
        if (row.event_ticker) set.add(row.event_ticker);
      } catch {
        // skip malformed lines
      }
    }
  }
  return set;
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

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  requireEnv();

  console.log(
    `[backfill] limit=${args.limit} dryRun=${args.dryRun} refresh=${args.refresh} categories=${[...args.categories].join(',')}`
  );
  console.log(`[backfill] OCTAGON_BASE_URL=${process.env.OCTAGON_BASE_URL}`);

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
    const scope = args.refresh ? 'refresh-*.jsonl' : 'all *.jsonl';
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
    `[backfill] filtered: kept=${ranked.length} (dropped: category=${droppedCategory}, octagon=${droppedOctagon}, completed=${droppedCompleted}, no_volume=${droppedNoVolume})`
  );
  console.log(`[backfill] selected top ${top.length} by 24h volume`);

  if (top.length === 0) {
    console.log('[backfill] nothing to do.');
    return;
  }

  // 5. Build URLs
  console.log('[backfill] resolving Kalshi URLs (series slug lookups)...');
  const enriched: Array<Ranked & { url: string }> = [];
  for (const r of top) {
    try {
      const url = await buildKalshiUrl(r.ev);
      enriched.push({ ...r, url });
    } catch (err) {
      console.error(`[backfill] skipping ${r.ev.event_ticker}: failed to build URL: ${(err as Error).message}`);
    }
  }
  console.log(`[backfill] ${enriched.length} URLs resolved (${seriesSlugCache.size} unique series)`);

  if (args.dryRun) {
    console.log('\n=== DRY RUN — would process the following events ===');
    for (let i = 0; i < enriched.length; i++) {
      const { ev, volume, url } = enriched[i];
      console.log(`${String(i + 1).padStart(3)}. [${ev.category}] vol24h=${volume.toLocaleString()} ${ev.event_ticker}`);
      console.log(`     ${ev.title}`);
      console.log(`     ${url}`);
    }
    return;
  }

  // 6. Sequential Octagon calls, streaming JSONL output
  mkdirSync(OUT_DIR, { recursive: true });
  const runStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const prefix = args.refresh ? 'refresh' : 'backfill';
  const outPath = join(OUT_DIR, `${prefix}-${runStamp}.jsonl`);
  const variant = args.refresh ? 'refresh' : 'default';
  console.log(`[backfill] variant=${variant} writing to ${outPath}`);

  let succeeded = 0;
  let failed = 0;
  for (let i = 0; i < enriched.length; i++) {
    const { ev, volume, url } = enriched[i];
    const start = Date.now();
    const tag = `[${i + 1}/${enriched.length}] ${ev.event_ticker}`;
    process.stdout.write(`${tag} — calling Octagon...`);
    try {
      const response = await callOctagon(url, variant);
      const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
      const row = {
        event_ticker: ev.event_ticker,
        series_ticker: ev.series_ticker,
        title: ev.title,
        category: ev.category,
        total_volume_24h: volume,
        kalshi_url: url,
        variant,
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
      // Record the failure too so resume skips it (and we have a record)
      const errRow = {
        event_ticker: ev.event_ticker,
        series_ticker: ev.series_ticker,
        title: ev.title,
        category: ev.category,
        total_volume_24h: volume,
        kalshi_url: url,
        variant,
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
