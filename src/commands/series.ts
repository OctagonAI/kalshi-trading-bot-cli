/**
 * Series-level rollup over Octagon's /kalshi/markets endpoint.
 *
 * "Series" is the Kalshi grouping above markets — KXBTCD is a series, while
 * KXBTCD-26DEC31-T100000 is a market inside it. Octagon doesn't expose a
 * dedicated series endpoint, so we paginate /kalshi/markets and reduce by
 * series_ticker client-side. The open universe is ~few thousand markets so
 * this is cheap (2-3 paginated calls).
 */
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  searchKalshiMarkets,
  getBasketCandles,
  type KalshiMarketRow,
  type BasketCandlesResponse,
} from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

const UNIVERSE_PAGE_LIMIT = 200;
const MAX_PAGES = 25; // safety cap; universe is small

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtVol(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return v.toFixed(0);
}

function fmtMoney(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  return `$${v.toFixed(2)}`;
}

// ─── Rollup logic ───────────────────────────────────────────────────────────

export interface SeriesRollup {
  series_ticker: string;
  market_count: number;
  active_count: number;
  total_volume_24h: number;
  total_open_interest: number;
  dominant_category: string | null;
  sample_titles: string[];
  earliest_close: string | null;
  latest_close: string | null;
}

function rollupBySeries(markets: KalshiMarketRow[]): SeriesRollup[] {
  const map = new Map<string, {
    market_count: number;
    active_count: number;
    total_volume_24h: number;
    total_open_interest: number;
    category_counts: Map<string, number>;
    sample_titles: string[];
    earliest_close: string | null;
    latest_close: string | null;
  }>();

  for (const m of markets) {
    // Use series_ticker if present, else derive from market_ticker prefix
    // (Kalshi convention: SERIES-EVENT-OUTCOME)
    const series = m.series_ticker ?? m.market_ticker.split('-')[0];
    if (!series) continue;

    let entry = map.get(series);
    if (!entry) {
      entry = {
        market_count: 0,
        active_count: 0,
        total_volume_24h: 0,
        total_open_interest: 0,
        category_counts: new Map(),
        sample_titles: [],
        earliest_close: null,
        latest_close: null,
      };
      map.set(series, entry);
    }

    entry.market_count += 1;
    if (m.status === 'active' || m.status === 'open') entry.active_count += 1;
    entry.total_volume_24h += m.volume_24h ?? 0;
    entry.total_open_interest += m.open_interest ?? 0;

    const cat = m.category ?? '';
    if (cat) entry.category_counts.set(cat, (entry.category_counts.get(cat) ?? 0) + 1);

    if (entry.sample_titles.length < 3 && m.title) entry.sample_titles.push(m.title);

    if (m.close_time) {
      if (!entry.earliest_close || m.close_time < entry.earliest_close) entry.earliest_close = m.close_time;
      if (!entry.latest_close || m.close_time > entry.latest_close) entry.latest_close = m.close_time;
    }
  }

  return Array.from(map.entries()).map(([series_ticker, agg]) => {
    let dominant_category: string | null = null;
    let dominantCount = 0;
    for (const [cat, count] of agg.category_counts.entries()) {
      if (count > dominantCount) {
        dominant_category = cat;
        dominantCount = count;
      }
    }
    return {
      series_ticker,
      market_count: agg.market_count,
      active_count: agg.active_count,
      total_volume_24h: agg.total_volume_24h,
      total_open_interest: agg.total_open_interest,
      dominant_category,
      sample_titles: agg.sample_titles,
      earliest_close: agg.earliest_close,
      latest_close: agg.latest_close,
    };
  });
}

async function fetchUniverse(opts: {
  q?: string;
  category?: string;
  series_ticker?: string;
  min_volume_24h?: number;
  close_before?: string;
  maxMarkets?: number;
}): Promise<KalshiMarketRow[]> {
  const all: KalshiMarketRow[] = [];
  let cursor: string | undefined;
  const cap = opts.maxMarkets ?? 5000;
  for (let i = 0; i < MAX_PAGES; i++) {
    const page = await searchKalshiMarkets({
      q: opts.q,
      category: opts.category,
      series_ticker: opts.series_ticker,
      min_volume_24h: opts.min_volume_24h,
      close_before: opts.close_before,
      limit: UNIVERSE_PAGE_LIMIT,
      cursor,
    });
    all.push(...page.data);
    if (all.length >= cap || !page.has_more || !page.next_cursor) break;
    cursor = page.next_cursor;
  }
  return all;
}

// ─── Handlers ───────────────────────────────────────────────────────────────

export type SeriesResult =
  | { kind: 'list'; data: SeriesRollup[]; total_markets: number }
  | { kind: 'detail'; series_ticker: string; rollup: SeriesRollup; markets: KalshiMarketRow[] }
  | { kind: 'candles'; series_ticker: string; tickers_used: string[]; data: BasketCandlesResponse };

export async function handleSeries(args: ParsedArgs): Promise<CLIResponse<SeriesResult>> {
  try {
    const positional = args.positionalArgs[0]?.toLowerCase();

    // series candles <ticker> [--timeframe]
    if (positional === 'candles') {
      const seriesTicker = (args.positionalArgs[1] ?? args.seriesTicker)?.toUpperCase();
      if (!seriesTicker) {
        return wrapError('series', 'MISSING_SERIES', 'Usage: series candles <series_ticker> [--timeframe 1y]');
      }
      // Octagon's series_ticker field is null on every row, so we filter the
      // universe by market_ticker prefix client-side instead.
      const all = await fetchUniverse({});
      const markets = all.filter((m) => m.market_ticker.toUpperCase().startsWith(seriesTicker + '-') || m.market_ticker.toUpperCase() === seriesTicker);
      if (markets.length === 0) {
        return wrapError('series', 'EMPTY_SERIES', `No markets found for series ${seriesTicker}`);
      }
      // Cap to top-N by volume to keep request size sane (basket API limits)
      const topN = args.topK ?? 20;
      const tickers = markets
        .slice()
        .sort((a, b) => (b.volume_24h ?? 0) - (a.volume_24h ?? 0))
        .slice(0, topN)
        .map((m) => m.market_ticker);
      const data = await getBasketCandles({ market_tickers: tickers, timeframe: args.timeframe });
      return wrapSuccess('series', { kind: 'candles', series_ticker: seriesTicker, tickers_used: tickers, data });
    }

    // series search <query> [--min-volume N]
    if (positional === 'search') {
      const q = args.positionalArgs.slice(1).join(' ');
      if (!q) {
        return wrapError('series', 'MISSING_QUERY', 'Usage: series search <query>');
      }
      const markets = await fetchUniverse({ q, min_volume_24h: args.minVolume });
      const rollups = rollupBySeries(markets).sort((a, b) => b.total_volume_24h - a.total_volume_24h);
      const limited = rollups.slice(0, args.limit ?? 30);
      return wrapSuccess('series', { kind: 'list', data: limited, total_markets: markets.length });
    }

    // series <SERIES_TICKER> — drill into one series
    if (args.positionalArgs[0] && args.positionalArgs[0].toUpperCase().startsWith('KX')) {
      const seriesTicker = args.positionalArgs[0].toUpperCase();
      const all = await fetchUniverse({});
      const markets = all.filter(
        (m) => m.market_ticker.toUpperCase().startsWith(seriesTicker + '-') || m.market_ticker.toUpperCase() === seriesTicker,
      );
      if (markets.length === 0) {
        return wrapError('series', 'EMPTY_SERIES', `No markets found for series ${seriesTicker}`);
      }
      const rollup = rollupBySeries(markets)[0];
      markets.sort((a, b) => (b.volume_24h ?? 0) - (a.volume_24h ?? 0));
      return wrapSuccess('series', {
        kind: 'detail',
        series_ticker: seriesTicker,
        rollup,
        markets: markets.slice(0, args.limit ?? 30),
      });
    }

    // series list [--category C] [--min-volume N] [--limit N]
    const markets = await fetchUniverse({
      category: args.category,
      min_volume_24h: args.minVolume,
    });
    const rollups = rollupBySeries(markets).sort((a, b) => b.total_volume_24h - a.total_volume_24h);
    const limited = rollups.slice(0, args.limit ?? 50);
    return wrapSuccess('series', { kind: 'list', data: limited, total_markets: markets.length });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('series', 'OCTAGON_ERROR', message);
  }
}

// ─── Formatters ─────────────────────────────────────────────────────────────

export function formatSeriesHuman(result: SeriesResult): string {
  if (result.kind === 'list') return formatSeriesList(result.data, result.total_markets);
  if (result.kind === 'detail') return formatSeriesDetail(result.series_ticker, result.rollup, result.markets);
  return formatSeriesCandles(result.series_ticker, result.tickers_used, result.data);
}

function formatSeriesList(rollups: SeriesRollup[], totalMarkets: number): string {
  const lines: string[] = [];
  lines.push(`Series rollup — ${rollups.length} series across ${totalMarkets} markets, sorted by 24h volume`);
  lines.push('');
  if (rollups.length === 0) {
    lines.push('No series match.');
    return lines.join('\n');
  }
  const rows: string[][] = rollups.map((r) => [
    r.series_ticker,
    truncate(r.sample_titles[0] ?? '-', 40),
    String(r.active_count),
    String(r.market_count),
    fmtVol(r.total_volume_24h),
    fmtVol(r.total_open_interest),
    r.dominant_category ?? '-',
    (r.earliest_close ?? '').slice(0, 10),
  ]);
  lines.push(formatTable(
    ['Series', 'Sample title', 'Active', 'Total', '24h Vol', 'OI', 'Category', 'Earliest close'],
    rows,
  ));
  lines.push('');
  lines.push('Use "series <SERIES>" to drill in, "series candles <SERIES>" for theme NAV.');
  return lines.join('\n');
}

function formatSeriesDetail(seriesTicker: string, rollup: SeriesRollup, markets: KalshiMarketRow[]): string {
  const lines: string[] = [];
  lines.push(`Series ${seriesTicker} — ${rollup.active_count}/${rollup.market_count} active, $${rollup.total_volume_24h.toFixed(0)} 24h vol`);
  lines.push(`  Category    ${rollup.dominant_category ?? '-'}`);
  lines.push(`  Close range ${(rollup.earliest_close ?? '-').slice(0, 10)}  →  ${(rollup.latest_close ?? '-').slice(0, 10)}`);
  lines.push('');
  if (markets.length === 0) {
    lines.push('No sub-markets.');
    return lines.join('\n');
  }
  const rows: string[][] = markets.map((m) => [
    m.market_ticker,
    truncate(m.title, 38),
    m.status,
    fmtMoney(m.last_price ?? m.yes_ask),
    fmtVol(m.volume_24h),
    (m.close_time ?? '').slice(0, 10),
  ]);
  lines.push(formatTable(['Market', 'Title', 'Status', 'Last', '24h Vol', 'Closes'], rows));
  return lines.join('\n');
}

function formatSeriesCandles(seriesTicker: string, tickers: string[], data: BasketCandlesResponse): string {
  const lines: string[] = [];
  lines.push(`Series ${seriesTicker} NAV — ${data.timeframe} window, ${data.candles.length} bins, basket of top ${tickers.length} sub-markets`);
  if (data.missing.length > 0) {
    lines.push(`  Excluded (no candle history): ${data.missing.length}`);
  }
  lines.push('');
  if (data.candles.length === 0) {
    lines.push('No candles available.');
    return lines.join('\n');
  }
  const recent = data.candles.slice(-10);
  const rows: string[][] = recent.map((c) => [
    new Date(c.time * 1000).toISOString().slice(0, 16).replace('T', ' '),
    c.open.toFixed(3),
    c.high.toFixed(3),
    c.low.toFixed(3),
    c.close.toFixed(3),
  ]);
  lines.push(formatTable(['Time (UTC)', 'Open', 'High', 'Low', 'Close'], rows));
  if (data.candles.length > recent.length) {
    lines.push('');
    lines.push(`(showing last ${recent.length} of ${data.candles.length} bins — use --json for all)`);
  }
  return lines.join('\n');
}

// Exposed so theme commands can reuse the rollup + universe fetcher
export { fetchUniverse, rollupBySeries };
