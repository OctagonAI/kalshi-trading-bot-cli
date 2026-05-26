import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  buildBasket,
  backtestBasket,
  getBasketSize,
  getBasketCandles,
  searchKalshiMarkets,
  validateBasket,
  getMarketsEdge,
  type BasketBuildResponse,
  type BasketBacktestResponse,
  type BasketSizeResponse,
  type BasketCandlesResponse,
  type BasketBuildBody,
  type BasketSizeBody,
  type BasketCandlesBody,
  type BasketValidateResponse,
  type ValidateBasketBody,
  type ValidateBasketLeg,
} from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';
import { getDb } from '../db/index.js';
import { getEditorialTheme } from '../db/editorial-themes.js';

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
    const ticker = tickerRaw?.trim().toUpperCase();
    if (!ticker || !probRaw) continue;
    const p = Number(probRaw);
    if (!Number.isFinite(p) || p < 0 || p > 1) continue;
    map[ticker] = p;
  }
  return Object.keys(map).length > 0 ? map : undefined;
}

function parseLegs(raw: string | undefined, sideDefault: 'yes' | 'no'): { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[] {
  if (!raw) return [];
  const legs: { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[] = [];
  for (const pair of raw.split(',')) {
    const [tickerRaw, probRaw] = pair.split(':');
    const ticker = tickerRaw?.trim().toUpperCase();
    if (!ticker || !probRaw) continue;
    const p = Number(probRaw);
    if (!Number.isFinite(p) || p < 0 || p > 1) continue;
    legs.push({ market_ticker: ticker, side: sideDefault, model_probability: p });
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

/**
 * Resolve an editorial theme name to a flat list of market tickers: for each
 * series in the theme, pull the live universe, filter by market_ticker prefix
 * (Octagon's series_ticker field is currently null), and pick the top market
 * by 24h volume. Limit total candidates with --top-k.
 */
async function tickersFromTheme(themeName: string, topPerSeries: number, maxTotal: number): Promise<string[]> {
  const db = getDb();
  const theme = getEditorialTheme(db, themeName);
  if (!theme) throw new Error(`No editorial theme named "${themeName}". Try \`themes list\` or \`themes import\`.`);
  if (theme.series.length === 0) throw new Error(`Theme "${themeName}" has no mapped series.`);
  // Pull the universe once and bucket by series prefix.
  const universe = await searchKalshiMarkets({ limit: 200 });
  const all = [...universe.data];
  let cursor = universe.next_cursor;
  let pages = 1;
  while (cursor && universe.has_more && pages < 25) {
    const page = await searchKalshiMarkets({ limit: 200, cursor });
    all.push(...page.data);
    if (!page.has_more || !page.next_cursor) break;
    cursor = page.next_cursor;
    pages += 1;
  }
  const out: string[] = [];
  for (const seriesTicker of theme.series) {
    const prefix = seriesTicker.toUpperCase() + '-';
    const sub = all
      .filter((m) => m.market_ticker.toUpperCase().startsWith(prefix))
      .sort((a, b) => (b.volume_24h ?? 0) - (a.volume_24h ?? 0))
      .slice(0, topPerSeries)
      .map((m) => m.market_ticker);
    out.push(...sub);
    if (out.length >= maxTotal) break;
  }
  return out.slice(0, maxTotal);
}

// ─── build ──────────────────────────────────────────────────────────────────

export async function handleBasketBuild(args: ParsedArgs): Promise<CLIResponse<BasketBuildResponse>> {
  let probs = parseProbabilities(args.probabilities);
  const wantsKelly = args.bankroll !== undefined || args.kellyMultiplier !== undefined || probs !== undefined;

  if (wantsKelly && args.bankroll === undefined) {
    return wrapError('basket', 'MISSING_BANKROLL', 'Kelly sizing requires --bankroll (e.g., --bankroll 1000).');
  }

  const labelContainsAny = args.labelContains
    ? args.labelContains.split(',').map((s) => s.trim()).filter(Boolean)
    : undefined;

  // --theme <name> or --tickers KX-A,KX-B: pass explicit candidate pool via the
  // new universe.market_tickers field. Server-side resolver respects this when
  // wired up; falls back to the default search universe otherwise.
  let marketTickers: string[] | undefined;
  if (args.theme) {
    try {
      marketTickers = await tickersFromTheme(args.theme, args.topK ?? 1, 200);
    } catch (err) {
      return wrapError('basket', 'THEME_RESOLVE', err instanceof Error ? err.message : String(err));
    }
    if (marketTickers.length === 0) {
      return wrapError('basket', 'THEME_EMPTY', `Theme "${args.theme}" resolved to 0 markets.`);
    }
  } else if (args.tickers) {
    marketTickers = args.tickers.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
  }

  // --auto-probs: fetch model probabilities for the candidate pool so Kelly
  // sizing isn't reliant on manual --probs entry.
  if (args.autoProbs && marketTickers && marketTickers.length > 0 && !probs) {
    try {
      const edgeResp = await getMarketsEdge({ tickers: marketTickers });
      const scored = edgeResp.data.filter((r) => r.status === 'scored' && r.model_probability != null);
      if (scored.length > 0) {
        probs = {};
        for (const row of scored) {
          const key = row.market_ticker ?? row.input_ticker;
          probs[key.toUpperCase()] = row.model_probability!;
        }
      }
    } catch {
      // Best-effort — fall through to manual probs / equal sizing.
    }
  }

  const body: BasketBuildBody = {
    universe: {
      q: args.query,
      anchor_ticker: args.ticker,
      market_tickers: marketTickers,
      category: args.category,
      series_ticker: args.seriesTicker,
      min_volume_24h: args.minVolume,
      close_before: args.closeBefore,
      label_contains_any: labelContainsAny,
    },
    n: args.n ?? 5,
    max_per_cluster: args.maxPerCluster,
    max_pairwise_correlation: args.maxCorrelation,
    candidate_pool_size: args.limit ?? (marketTickers ? Math.max(marketTickers.length, 50) : undefined),
    correlation_window_days: args.windowDays,
    sizing: (wantsKelly || probs)
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
  let tickers = collectTickers(args);
  // --theme <name>: resolve registry to top market per series, equal-weight basket.
  if (args.theme && tickers.length === 0) {
    try {
      tickers = await tickersFromTheme(args.theme, args.topK ?? 1, 50);
    } catch (err) {
      return wrapError('basket', 'THEME_RESOLVE', err instanceof Error ? err.message : String(err));
    }
    if (tickers.length === 0) {
      return wrapError('basket', 'THEME_EMPTY', `Theme "${args.theme}" resolved to 0 tradeable markets.`);
    }
  }
  if (tickers.length < 1) {
    return wrapError('basket', 'MISSING_TICKERS', 'Usage: basket backtest --tickers KX-A,KX-B [--weights 0.6,0.4] [--timeframe 1y]  OR  basket backtest --theme "Iran Escalation"');
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
  let tickers = collectTickers(args);
  if (args.theme && tickers.length === 0) {
    try {
      tickers = await tickersFromTheme(args.theme, args.topK ?? 1, 50);
    } catch (err) {
      return wrapError('basket', 'THEME_RESOLVE', err instanceof Error ? err.message : String(err));
    }
    if (tickers.length === 0) {
      return wrapError('basket', 'THEME_EMPTY', `Theme "${args.theme}" resolved to 0 tradeable markets.`);
    }
  }
  if (tickers.length < 1) {
    return wrapError('basket', 'MISSING_TICKERS', 'Usage: basket candles --tickers KX-A,KX-B [--weights 0.6,0.4]  OR  basket candles --theme "Iran Escalation"');
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
    return wrapError('basket', 'MISSING_BANKROLL', 'Usage: basket size --bankroll 1000 --kelly 0.25 --probs KX-A:0.62,KX-B:0.55 [--side yes|no]  OR  --auto-probs --theme "AI Race Milestones"  OR  --auto-probs --tickers KX-A,KX-B');
  }
  const sideDefault = args.side ?? 'yes';

  // Source the leg list. Priority:
  //   1) --probs (explicit): manual probabilities
  //   2) --theme: resolve to top market per series, then fetch probs via markets/edge
  //   3) --tickers + --auto-probs: explicit tickers, fetched probs
  let legs: { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[] = [];

  if (args.probabilities && /[:]/.test(args.probabilities) && !args.autoProbs) {
    legs = parseLegs(args.probabilities, sideDefault);
  } else if (args.theme || (args.autoProbs && args.tickers)) {
    let tickers: string[];
    if (args.theme) {
      try {
        tickers = await tickersFromTheme(args.theme, args.topK ?? 1, 30);
      } catch (err) {
        return wrapError('basket', 'THEME_RESOLVE', err instanceof Error ? err.message : String(err));
      }
    } else {
      tickers = (args.tickers ?? '').split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
    }
    if (tickers.length === 0) {
      return wrapError('basket', 'NO_TICKERS', 'No tickers to size.');
    }
    let edgeRows;
    try {
      const edgeResp = await getMarketsEdge({ tickers });
      edgeRows = edgeResp.data;
    } catch (err) {
      return wrapError('basket', 'OCTAGON_EDGE', err instanceof Error ? err.message : String(err));
    }
    const scored = edgeRows.filter((r) => r.status === 'scored' && r.model_probability != null);
    if (scored.length === 0) {
      const unscored = edgeRows.map((r) => r.input_ticker).join(', ');
      return wrapError('basket', 'NO_SCORED_LEGS',
        `Octagon has no model coverage for any of these tickers in the current run. Unscored: ${unscored}. Try --probs to supply your own priors, or pick events Octagon scored (see kalshi search edge).`);
    }
    legs = scored.map((r) => ({
      market_ticker: r.market_ticker ?? r.input_ticker,
      side: sideDefault,
      model_probability: r.model_probability!,
    }));
  }

  if (legs.length === 0) {
    return wrapError('basket', 'MISSING_PROBS', 'Pass --probs TICKER:prob,TICKER:prob OR --auto-probs --theme "..." OR --auto-probs --tickers KX-A,KX-B.');
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

// ─── validate ───────────────────────────────────────────────────────────────

/**
 * Parse --legs "KX-A:yes:170,KX-B:no:160" into ValidateBasketLeg[].
 * Each leg is ticker[:side[:stake]] — side defaults to yes, stake defaults to
 * an equal split of bankroll (or 100 if no bankroll).
 */
function parseValidateLegs(args: ParsedArgs): { legs: ValidateBasketLeg[]; error?: string } {
  // Two input modes: --legs "csv" with side+stake, OR --tickers + --probs/--side.
  if (args.tickers) {
    const tickers = args.tickers.split(',').map((t) => t.trim().toUpperCase()).filter(Boolean);
    if (tickers.length === 0) return { legs: [], error: 'No tickers supplied.' };
    const sideDefault = args.side ?? 'yes';
    const totalStake = args.bankroll ?? 1000;
    const perLeg = totalStake / tickers.length;
    return {
      legs: tickers.map((t) => ({ market_ticker: t, side: sideDefault, stake_usd: perLeg })),
    };
  }
  // --legs csv: ticker:side[:stake]
  if (args.probabilities && /[:]/.test(args.probabilities)) {
    const legs: ValidateBasketLeg[] = [];
    for (const piece of args.probabilities.split(',')) {
      const parts = piece.trim().split(':');
      if (parts.length < 1 || !parts[0]) continue;
      const ticker = parts[0].toUpperCase();
      const sideRaw = (parts[1] ?? 'yes').toLowerCase();
      const side: 'yes' | 'no' = sideRaw === 'no' ? 'no' : 'yes';
      const stake = parts[2] ? Number(parts[2]) : NaN;
      legs.push({
        market_ticker: ticker,
        side,
        stake_usd: Number.isFinite(stake) ? stake : (args.bankroll ?? 1000) / Math.max(1, args.probabilities!.split(',').length),
      });
    }
    return { legs };
  }
  return { legs: [], error: 'Provide --tickers KX-A,KX-B OR --probs KX-A:yes:170,KX-B:no:160 to specify legs.' };
}

export async function handleBasketValidate(args: ParsedArgs): Promise<CLIResponse<BasketValidateResponse>> {
  // --theme support: resolve to legs
  let legs: ValidateBasketLeg[] = [];
  if (args.theme) {
    try {
      const tickers = await tickersFromTheme(args.theme, args.topK ?? 1, 30);
      if (tickers.length === 0) {
        return wrapError('basket', 'THEME_EMPTY', `Theme "${args.theme}" resolved to 0 tickers.`);
      }
      const totalStake = args.bankroll ?? 1000;
      const perLeg = totalStake / tickers.length;
      const sideDefault = args.side ?? 'yes';
      legs = tickers.map((t) => ({ market_ticker: t, side: sideDefault, stake_usd: perLeg }));
    } catch (err) {
      return wrapError('basket', 'THEME_RESOLVE', err instanceof Error ? err.message : String(err));
    }
  } else {
    const parsed = parseValidateLegs(args);
    if (parsed.error) return wrapError('basket', 'MISSING_LEGS', parsed.error);
    legs = parsed.legs;
  }
  if (legs.length === 0) {
    return wrapError('basket', 'MISSING_LEGS', 'No legs to validate.');
  }
  const body: ValidateBasketBody = {
    legs,
    bankroll_usd: args.bankroll,
    correlation_window_days: args.windowDays ?? 30,
    correlation_interval: args.correlationInterval,
    max_pairwise_correlation: args.maxCorrelation,
    calendar_clash_window_days: 7,
  };
  try {
    const data = await validateBasket(body);
    return wrapSuccess('basket', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('basket', 'OCTAGON_ERROR', message);
  }
}

export function formatBasketValidateHuman(data: BasketValidateResponse): string {
  const lines: string[] = [];
  const bankroll = data.bankroll_usd != null ? `$${data.bankroll_usd.toFixed(0)}` : 'n/a';
  lines.push(`Basket validation — total stake $${data.total_stake_usd.toFixed(0)}, bankroll ${bankroll}, max leg ${(data.max_leg_pct * 100).toFixed(1)}%`);
  if (data.max_pairwise_correlation != null) {
    lines.push(`  Max pairwise correlation: ${data.max_pairwise_correlation.toFixed(2)}`);
  } else {
    lines.push('  Max pairwise correlation: n/a (no overlapping history)');
  }
  lines.push('');

  // Cluster breakdown — flag any cluster with ≥2 legs
  const thematic = Object.entries(data.cluster_breakdown_thematic);
  if (thematic.length > 0) {
    lines.push('Thematic cluster breakdown:');
    for (const [clusterId, tickers] of thematic) {
      const warn = tickers.length >= 2 ? ' ⚠' : '';
      lines.push(`  Cluster ${clusterId}${warn}  ${tickers.join(', ')}`);
    }
    lines.push('');
  }

  if (data.unassigned_market_tickers.length > 0) {
    lines.push(`Unassigned (no cluster): ${data.unassigned_market_tickers.join(', ')}`);
    lines.push('');
  }

  // Pairwise correlations
  if (data.pairwise_correlations.length > 0) {
    lines.push('Pairwise correlations (top 10 by |corr|):');
    const top = data.pairwise_correlations
      .slice()
      .sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation))
      .slice(0, 10);
    const rows: string[][] = top.map((p) => [
      p.ticker_a.length > 22 ? p.ticker_a.slice(0, 21) + '…' : p.ticker_a,
      p.ticker_b.length > 22 ? p.ticker_b.slice(0, 21) + '…' : p.ticker_b,
      p.correlation.toFixed(3),
    ]);
    lines.push(formatTable(['Ticker A', 'Ticker B', 'Corr'], rows));
    lines.push('');
  }

  if (data.calendar_clashes.length > 0) {
    lines.push(`Calendar clashes (${data.calendar_clashes.length} weeks):`);
    for (const c of data.calendar_clashes) {
      lines.push(`  ${c.window_start.slice(0, 10)} → ${c.window_end.slice(0, 10)}: ${c.market_tickers.join(', ')}`);
    }
    lines.push('');
  }

  if (data.duplicate_underliers.length > 0) {
    lines.push(`Duplicate underliers (same event):`);
    for (const d of data.duplicate_underliers) {
      lines.push(`  ${d.event_ticker}: ${d.market_tickers.join(', ')}`);
    }
    lines.push('');
  }

  if (data.warnings.length > 0) {
    lines.push('⚠ Warnings:');
    for (const w of data.warnings) lines.push(`  • ${w}`);
  } else {
    lines.push('✓ No warnings.');
  }
  return lines.join('\n');
}

// Re-export for use by handleBasket below
export { getMarketsEdge };

// ─── Dispatcher ─────────────────────────────────────────────────────────────

export type BasketResult =
  | { sub: 'build'; data: BasketBuildResponse }
  | { sub: 'backtest'; data: BasketBacktestResponse }
  | { sub: 'size'; data: BasketSizeResponse }
  | { sub: 'candles'; data: BasketCandlesResponse }
  | { sub: 'validate'; data: BasketValidateResponse };

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
  if (sub === 'validate') {
    const resp = await handleBasketValidate(args);
    return liftBasket(resp, 'validate');
  }
  return wrapError('basket', 'MISSING_SUBCOMMAND', 'Usage: basket <build|backtest|size|candles|validate> [...]');
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
  if (result.sub === 'validate') return formatBasketValidateHuman(result.data);
  return formatBasketCandlesHuman(result.data);
}
