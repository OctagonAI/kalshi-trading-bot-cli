import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  buildBasket,
  backtestBasket,
  getBasketSize,
  getBasketCandles,
  type BasketBuildResponse,
  type BasketBacktestResponse,
  type BasketSizeResponse,
  type BasketCandlesResponse,
  type BasketBuildBody,
  type BasketSizeBody,
  type BasketCandlesBody,
} from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtPct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function parseProbabilities(raw: string | undefined): Record<string, number> | undefined {
  if (!raw) return undefined;
  const map: Record<string, number> = {};
  for (const pair of raw.split(',')) {
    const [tickerRaw, probRaw] = pair.split(':');
    if (!tickerRaw || !probRaw) continue;
    const p = Number(probRaw);
    if (!Number.isFinite(p)) continue;
    map[tickerRaw.trim().toUpperCase()] = p;
  }
  return Object.keys(map).length > 0 ? map : undefined;
}

function parseLegs(raw: string | undefined, sideDefault: 'yes' | 'no'): { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[] {
  if (!raw) return [];
  const legs: { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[] = [];
  for (const pair of raw.split(',')) {
    const [tickerRaw, probRaw] = pair.split(':');
    if (!tickerRaw || !probRaw) continue;
    const p = Number(probRaw);
    if (!Number.isFinite(p)) continue;
    legs.push({ market_ticker: tickerRaw.trim().toUpperCase(), side: sideDefault, model_probability: p });
  }
  return legs;
}

function collectTickers(args: ParsedArgs): string[] {
  const set = new Set<string>();
  if (args.tickers) {
    for (const t of args.tickers.split(',')) {
      const upper = t.trim().toUpperCase();
      if (upper) set.add(upper);
    }
  }
  // Skip positionalArgs[0] which is the basket subcommand (build/backtest/size/candles).
  for (let i = 1; i < args.positionalArgs.length; i++) {
    const upper = args.positionalArgs[i].toUpperCase();
    if (upper) set.add(upper);
  }
  return Array.from(set);
}

// ─── build ──────────────────────────────────────────────────────────────────

export async function handleBasketBuild(args: ParsedArgs): Promise<CLIResponse<BasketBuildResponse>> {
  const probs = parseProbabilities(args.probabilities);
  const wantsKelly = args.bankroll !== undefined || args.kellyMultiplier !== undefined || probs !== undefined;

  if (wantsKelly && args.bankroll === undefined) {
    return wrapError('basket', 'MISSING_BANKROLL', 'Kelly sizing requires --bankroll (e.g., --bankroll 1000).');
  }

  const labelContainsAny = args.labelContains
    ? args.labelContains.split(',').map((s) => s.trim()).filter(Boolean)
    : undefined;

  const body: BasketBuildBody = {
    universe: {
      q: args.query,
      anchor_ticker: args.ticker,
      category: args.category,
      series_ticker: args.seriesTicker,
      min_volume_24h: args.minVolume,
      close_before: args.closeBefore,
      label_contains_any: labelContainsAny,
    },
    n: args.n ?? 5,
    max_per_cluster: args.maxPerCluster,
    max_pairwise_correlation: args.maxCorrelation,
    candidate_pool_size: args.limit,
    correlation_window_days: args.windowDays,
    sizing: wantsKelly
      ? {
          strategy: 'kelly',
          bankroll_usd: args.bankroll,
          kelly_multiplier: args.kellyMultiplier ?? 0.25,
          leg_probabilities: probs,
        }
      : { strategy: 'equal' },
  };

  try {
    const data = await buildBasket(body);
    return wrapSuccess('basket', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('basket', 'OCTAGON_ERROR', message);
  }
}

export function formatBasketBuildHuman(data: BasketBuildResponse): string {
  const lines: string[] = [];
  const corrStr = data.realized_max_pairwise_correlation == null
    ? 'n/a (no overlapping history)'
    : data.realized_max_pairwise_correlation.toFixed(2);
  lines.push(`Basket — ${data.legs.length} legs, ${data.universe_size} candidates considered, realized max pairwise correlation ${corrStr}`);
  lines.push('');

  if (data.legs.length === 0) {
    lines.push('No legs selected.');
  } else {
    const num = (v: number | null | undefined, decimals: number, prefix = '', suffix = '') =>
      v == null ? '-' : `${prefix}${v.toFixed(decimals)}${suffix}`;
    const rows: string[][] = data.legs.map((l) => [
      l.market_ticker,
      truncate(l.title, 35),
      l.side.toUpperCase(),
      num(l.price, 2),
      num(l.model_probability != null ? l.model_probability * 100 : null, 1, '', '%'),
      num(l.kelly_fraction, 3),
      num(l.weight, 3),
      num(l.notional_usd, 2, '$'),
      l.cluster_label ? truncate(l.cluster_label, 18) : '-',
    ]);
    lines.push(formatTable(
      ['Ticker', 'Title', 'Side', 'Price', 'Model%', 'Kelly', 'Weight', 'Notional', 'Cluster'],
      rows,
    ));
  }

  if (data.dropped.length > 0) {
    lines.push('');
    lines.push(`Dropped ${data.dropped.length} candidate(s) during selection (top 5):`);
    for (const d of data.dropped.slice(0, 5)) {
      lines.push(`  ${d.market_ticker} — ${d.reason}`);
    }
  }
  return lines.join('\n');
}

// ─── backtest ───────────────────────────────────────────────────────────────

export async function handleBasketBacktest(args: ParsedArgs): Promise<CLIResponse<BasketBacktestResponse>> {
  const tickers = collectTickers(args);
  if (tickers.length < 1) {
    return wrapError('basket', 'MISSING_TICKERS', 'Usage: basket backtest --tickers KX-A,KX-B [--weights 0.6,0.4] [--timeframe 1y]');
  }
  if (args.weights && args.weights.length !== tickers.length) {
    return wrapError('basket', 'WEIGHTS_MISMATCH', `Got ${tickers.length} tickers but ${args.weights.length} weights.`);
  }
  const body: BasketCandlesBody = {
    market_tickers: tickers,
    weights: args.weights,
    timeframe: args.timeframe,
  };
  try {
    const data = await backtestBasket(body);
    return wrapSuccess('basket', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('basket', 'OCTAGON_ERROR', message);
  }
}

export function formatBasketBacktestHuman(data: BasketBacktestResponse): string {
  const lines: string[] = [];
  lines.push(`Basket backtest — ${data.timeframe} window, ${data.candles.length} bins (interval ${data.interval_source})`);
  if (data.missing.length > 0) {
    lines.push(`  Excluded (no candle data): ${data.missing.join(', ')}`);
  }
  lines.push('');
  const s = data.summary;
  const rows: string[][] = [
    ['Total return',       fmtPct(s.total_return)],
    ['Annualized return',  fmtPct(s.annualized_return)],
    ['Sharpe',             s.sharpe != null ? s.sharpe.toFixed(2) : '-'],
    ['Max drawdown',       fmtPct(s.max_drawdown)],
    ['Win rate',           fmtPct(s.win_rate)],
    ['First NAV',          s.first_nav.toFixed(3)],
    ['Final NAV',          s.final_nav.toFixed(3)],
    ['Observations',       String(s.observation_count)],
  ];
  lines.push(formatTable(['Metric', 'Value'], rows));
  return lines.join('\n');
}

// ─── candles ────────────────────────────────────────────────────────────────

export async function handleBasketCandles(args: ParsedArgs): Promise<CLIResponse<BasketCandlesResponse>> {
  const tickers = collectTickers(args);
  if (tickers.length < 1) {
    return wrapError('basket', 'MISSING_TICKERS', 'Usage: basket candles --tickers KX-A,KX-B [--weights 0.6,0.4] [--timeframe 1y]');
  }
  if (args.weights && args.weights.length !== tickers.length) {
    return wrapError('basket', 'WEIGHTS_MISMATCH', `Got ${tickers.length} tickers but ${args.weights.length} weights.`);
  }
  const body: BasketCandlesBody = {
    market_tickers: tickers,
    weights: args.weights,
    timeframe: args.timeframe,
  };
  try {
    const data = await getBasketCandles(body);
    return wrapSuccess('basket', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('basket', 'OCTAGON_ERROR', message);
  }
}

export function formatBasketCandlesHuman(data: BasketCandlesResponse): string {
  const lines: string[] = [];
  lines.push(`Basket NAV — ${data.timeframe} window, ${data.candles.length} bins (interval ${data.interval_source})`);
  if (data.missing.length > 0) {
    lines.push(`  Excluded (no candle data): ${data.missing.join(', ')}`);
  }
  lines.push('');
  if (data.candles.length === 0) {
    lines.push('No candles in window.');
    return lines.join('\n');
  }
  const shown = data.candles.slice(-10);
  const rows: string[][] = shown.map((c) => [
    new Date(c.time * 1000).toISOString().slice(0, 16).replace('T', ' '),
    c.open.toFixed(3),
    c.high.toFixed(3),
    c.low.toFixed(3),
    c.close.toFixed(3),
  ]);
  lines.push(formatTable(['Time (UTC)', 'Open', 'High', 'Low', 'Close'], rows));
  if (data.candles.length > shown.length) {
    lines.push('');
    lines.push(`(showing last ${shown.length} of ${data.candles.length} bins — use --json for all)`);
  }
  return lines.join('\n');
}

// ─── size ───────────────────────────────────────────────────────────────────

export async function handleBasketSize(args: ParsedArgs): Promise<CLIResponse<BasketSizeResponse>> {
  if (args.bankroll === undefined || args.bankroll <= 0) {
    return wrapError('basket', 'MISSING_BANKROLL', 'Usage: basket size --bankroll 1000 --kelly 0.25 --probs KX-A:0.62,KX-B:0.55 [--side yes|no]');
  }
  const sideDefault = args.side ?? 'yes';
  const legs = parseLegs(args.probabilities, sideDefault);
  if (legs.length === 0) {
    return wrapError('basket', 'MISSING_PROBS', 'Pass --probs TICKER:prob,TICKER:prob,... with at least one leg.');
  }
  const body: BasketSizeBody = {
    bankroll_usd: args.bankroll,
    kelly_multiplier: args.kellyMultiplier ?? 0.25,
    legs,
  };
  try {
    const data = await getBasketSize(body);
    return wrapSuccess('basket', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('basket', 'OCTAGON_ERROR', message);
  }
}

export function formatBasketSizeHuman(data: BasketSizeResponse): string {
  const lines: string[] = [];
  lines.push(`Kelly sizing — $${data.bankroll_usd.toFixed(2)} bankroll, ${(data.kelly_multiplier * 100).toFixed(0)}% Kelly cap, total notional $${data.total_notional.toFixed(2)}`);
  lines.push('');
  const rows: string[][] = data.legs.map((l) => [
    l.market_ticker,
    l.side.toUpperCase(),
    l.price.toFixed(2),
    `${(l.model_probability * 100).toFixed(1)}%`,
    `${l.edge_pp >= 0 ? '+' : ''}${l.edge_pp.toFixed(1)}pp`,
    l.kelly_fraction.toFixed(3),
    l.weight.toFixed(3),
    `$${l.notional_usd.toFixed(2)}`,
  ]);
  lines.push(formatTable(['Ticker', 'Side', 'Price', 'Model%', 'Edge', 'Kelly', 'Weight', 'Notional'], rows));
  return lines.join('\n');
}

// ─── Dispatcher ─────────────────────────────────────────────────────────────

export type BasketResult =
  | { sub: 'build'; data: BasketBuildResponse }
  | { sub: 'backtest'; data: BasketBacktestResponse }
  | { sub: 'size'; data: BasketSizeResponse }
  | { sub: 'candles'; data: BasketCandlesResponse };

export async function handleBasket(args: ParsedArgs): Promise<CLIResponse<BasketResult>> {
  const sub = args.positionalArgs[0]?.toLowerCase();
  if (sub === 'build') {
    const resp = await handleBasketBuild(args);
    return liftBasket(resp, 'build');
  }
  if (sub === 'backtest') {
    const resp = await handleBasketBacktest(args);
    return liftBasket(resp, 'backtest');
  }
  if (sub === 'size') {
    const resp = await handleBasketSize(args);
    return liftBasket(resp, 'size');
  }
  if (sub === 'candles') {
    const resp = await handleBasketCandles(args);
    return liftBasket(resp, 'candles');
  }
  return wrapError('basket', 'MISSING_SUBCOMMAND', 'Usage: basket <build|backtest|size|candles> [...]');
}

function liftBasket<T>(resp: CLIResponse<T>, sub: BasketResult['sub']): CLIResponse<BasketResult> {
  if (!resp.ok) return resp as unknown as CLIResponse<BasketResult>;
  return {
    ok: true,
    command: 'basket',
    timestamp: resp.timestamp,
    data: { sub, data: resp.data } as BasketResult,
  };
}

export function formatBasketHuman(result: BasketResult): string {
  if (result.sub === 'build') return formatBasketBuildHuman(result.data);
  if (result.sub === 'backtest') return formatBasketBacktestHuman(result.data);
  if (result.sub === 'size') return formatBasketSizeHuman(result.data);
  return formatBasketCandlesHuman(result.data);
}
