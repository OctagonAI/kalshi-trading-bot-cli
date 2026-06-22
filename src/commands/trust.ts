/**
 * Trader Trust scorecard.
 *
 * Surfaces Octagon's per-market market-integrity score from the new
 * `trader_trust_json` field on /v1/prediction-markets/events/{event_ticker}.
 *
 * Two views:
 *   - kalshi trust <event-ticker>                  → table across all markets
 *   - kalshi trust <event-ticker> --market <mkt>   → single-market detail card
 *
 * Six per-market scores, each in [0, 100]. Their direction-of-good differs:
 *
 *   HIGHER IS GOOD (green at top, red at bottom):
 *     - trader_trust         (overall composite)
 *     - liquidity_quality
 *     - move_quality
 *     - resolution_risk      (despite the name, the score itself is
 *                             "resolution clarity" — higher = better; the
 *                             label string explains the value)
 *
 *   HIGHER IS BAD (red at top, green at bottom):
 *     - market_avoid
 *     - quote_risk
 *
 * trader_trust_json is null on reports generated before this calculation
 * shipped; the handler returns a clear "no scorecard yet" error rather than
 * crashing.
 */
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import { fetchOctagonEventDirect } from '../scan/octagon-events-api.js';
import { formatTable } from './scan-formatters.js';
import { theme } from '../theme.js';

/** A single contributor to a score; the "why" behind the number. */
export interface TrustDriver {
  name: string;
  sub_score: number;
  points: number;
}

/** A raw metric backing the score; shown with --verbose. */
export interface TrustEvidence {
  metric: string;
  value: unknown;
}

export interface TrustScore {
  value: number;        // 0-100
  label: string;
  drivers: TrustDriver[];
  evidence: TrustEvidence[];
  confidence: 'low' | 'medium' | 'high';
  data_freshness: 'current' | 'point_in_time';
}

export interface TrustMarket {
  market_ticker: string;
  title: string;
  is_primary: boolean;
  scores: {
    trader_trust: TrustScore;
    liquidity_quality: TrustScore;
    market_avoid: TrustScore;
    move_quality: TrustScore;
    quote_risk: TrustScore;
    resolution_risk: TrustScore;
  };
}

export interface TraderTrustCard {
  calculation_version: string;
  computed_at: string;
  event_ticker: string;
  rollup: {
    median_trader_trust: number;
    min_trader_trust: number;
    markets_scored: number;
  };
  markets: TrustMarket[];
}

/** Set of scores where higher is bad (risk metrics). Used by the colorizer. */
const HIGHER_IS_BAD: ReadonlySet<keyof TrustMarket['scores']> = new Set([
  'market_avoid',
  'quote_risk',
]);

/** Color a 0-100 score according to its semantic direction. */
function colorScore(value: number, key: keyof TrustMarket['scores']): string {
  // Normalize so high = good for the color comparison.
  const goodness = HIGHER_IS_BAD.has(key) ? 100 - value : value;
  const str = value.toFixed(0).padStart(3);
  if (goodness >= 70) return theme.success(str);
  if (goodness >= 40) return theme.warning(str);
  return theme.error(str);
}

/** Output shape for both table and detail views (machine-readable). */
export type TrustResult =
  | { kind: 'table'; card: TraderTrustCard; event_name: string | null }
  | { kind: 'detail'; card: TraderTrustCard; market: TrustMarket; verbose: boolean };

export async function handleTrust(args: ParsedArgs): Promise<CLIResponse<TrustResult>> {
  const eventTicker = args.positionalArgs[0]?.toUpperCase();
  if (!eventTicker) {
    return wrapError('trust', 'MISSING_EVENT', 'Usage: trust <event_ticker> [--market <market_ticker>] [--verbose]');
  }

  let event;
  try {
    event = await fetchOctagonEventDirect(eventTicker);
  } catch (err) {
    return wrapError('trust', 'OCTAGON_ERROR', err instanceof Error ? err.message : String(err));
  }
  if (!event) {
    return wrapError('trust', 'EVENT_NOT_FOUND', `No Octagon record for event ${eventTicker}.`);
  }
  if (!event.trader_trust_json) {
    return wrapError(
      'trust',
      'NO_SCORECARD',
      `No trust scorecard for ${eventTicker} yet. The Trader Trust calculation may not have run for this event — try again after the next Octagon refresh.`,
    );
  }

  let card: TraderTrustCard;
  try {
    card = JSON.parse(event.trader_trust_json) as TraderTrustCard;
  } catch (err) {
    return wrapError(
      'trust',
      'PARSE_ERROR',
      `Octagon returned malformed trader_trust_json for ${eventTicker}: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!Array.isArray(card.markets) || card.markets.length === 0) {
    return wrapError('trust', 'EMPTY_SCORECARD', `Trust scorecard for ${eventTicker} has no markets.`);
  }

  // Single-market detail view
  if (args.market) {
    const wanted = args.market.toUpperCase();
    const market = card.markets.find((m) => m.market_ticker.toUpperCase() === wanted);
    if (!market) {
      return wrapError(
        'trust',
        'MARKET_NOT_IN_SCORECARD',
        `Market ${wanted} is not in the trust scorecard for ${eventTicker}. Run \`trust ${eventTicker}\` to see the available markets.`,
      );
    }
    return wrapSuccess('trust', { kind: 'detail', card, market, verbose: args.verbose });
  }

  return wrapSuccess('trust', { kind: 'table', card, event_name: event.name ?? null });
}

export function formatTrustHuman(result: TrustResult): string {
  if (result.kind === 'table') return formatTrustTable(result.card, result.event_name);
  return formatTrustDetail(result.card, result.market, result.verbose);
}

const SCORE_KEYS: Array<keyof TrustMarket['scores']> = [
  'trader_trust',
  'liquidity_quality',
  'move_quality',
  'market_avoid',
  'quote_risk',
  'resolution_risk',
];

const SCORE_HEADER_LABELS: Record<keyof TrustMarket['scores'], string> = {
  trader_trust: 'Trust',
  liquidity_quality: 'Liquidity',
  move_quality: 'Move',
  market_avoid: 'Avoid↑',
  quote_risk: 'Quote↑',
  resolution_risk: 'Resol.',
};

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function formatTrustTable(card: TraderTrustCard, eventName: string | null): string {
  const lines: string[] = [];
  const title = eventName ? ` — ${eventName}` : '';
  lines.push(`Trader Trust scorecard for ${card.event_ticker}${title}`);
  lines.push(
    `  Median trust ${card.rollup.median_trader_trust}  ·  Min ${card.rollup.min_trader_trust}  ·  ${card.rollup.markets_scored} markets scored`,
  );
  lines.push(`  Calculation ${card.calculation_version}  ·  Computed ${card.computed_at.slice(0, 16).replace('T', ' ')} UTC`);
  lines.push('');

  // Sort by liquidity_quality desc; the most active markets surface first.
  const sorted = card.markets.slice().sort((a, b) => b.scores.liquidity_quality.value - a.scores.liquidity_quality.value);

  const headers = ['', 'Market', 'Title', ...SCORE_KEYS.map((k) => SCORE_HEADER_LABELS[k])];
  const rows: string[][] = sorted.map((m) => [
    m.is_primary ? '*' : ' ',
    m.market_ticker,
    truncate(m.title, 30),
    ...SCORE_KEYS.map((k) => colorScore(m.scores[k].value, k)),
  ]);
  lines.push(formatTable(headers, rows));
  lines.push('');
  lines.push(theme.muted('  * = primary outcome.  Higher is better for Trust/Liquidity/Move/Resol.; Avoid↑ and Quote↑ are risk metrics (higher = worse).'));
  lines.push(theme.muted(`  Drill into one market: trust ${card.event_ticker} --market <market_ticker> [--verbose]`));
  return lines.join('\n');
}

function formatTrustDetail(card: TraderTrustCard, market: TrustMarket, verbose: boolean): string {
  const lines: string[] = [];
  const primaryMark = market.is_primary ? ' (primary)' : '';
  lines.push(`Trader Trust — ${market.market_ticker}${primaryMark}`);
  lines.push(`  ${market.title}`);
  lines.push(`  Event ${card.event_ticker}  ·  Calculation ${card.calculation_version}  ·  Computed ${card.computed_at.slice(0, 16).replace('T', ' ')} UTC`);
  lines.push('');

  for (const key of SCORE_KEYS) {
    const score = market.scores[key];
    const valueStr = colorScore(score.value, key);
    const risk = HIGHER_IS_BAD.has(key) ? ' (risk metric — higher is worse)' : '';
    const freshness = score.data_freshness === 'point_in_time' ? ' (as of report time)' : '';
    lines.push(`  ${SCORE_HEADER_LABELS[key].padEnd(10)}  ${valueStr}/100  ${theme.muted(score.label)}${risk}${freshness}`);
    const topDrivers = score.drivers.slice(0, 3);
    for (const d of topDrivers) {
      const sign = d.points >= 0 ? '+' : '';
      lines.push(`      • ${d.name}  ${theme.muted(`(sub ${d.sub_score.toFixed(0)}, ${sign}${d.points.toFixed(1)} pts)`)}`);
    }
    if (verbose && score.evidence.length > 0) {
      lines.push(theme.muted(`      Evidence:`));
      for (const e of score.evidence) {
        lines.push(theme.muted(`        ${e.metric}: ${formatEvidenceValue(e.value)}`));
      }
      lines.push(theme.muted(`      Confidence: ${score.confidence}  ·  Freshness: ${score.data_freshness}`));
    }
    lines.push('');
  }
  return lines.join('\n');
}

function formatEvidenceValue(v: unknown): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return v.toString();
  if (typeof v === 'string') return v;
  try { return JSON.stringify(v); } catch { return String(v); }
}
