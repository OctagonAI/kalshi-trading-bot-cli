/**
 * Trader Trust scorecard.
 *
 * Surfaces Octagon's Trust Index from the `trader_trust_json` field on
 * /v1/predictions/events/{event_ticker}. Card shape: trader_dashboard_lean_v1.14.
 *
 * Views:
 *   - kalshi trust <event-ticker>                  → Trust Index (the `underwriting` block)
 *   - kalshi trust <event-ticker> --verbose        → …plus per-contract market quality
 *   - kalshi trust <event-ticker> --market <mkt>   → single-market detail card
 *
 * The Trust Index mirrors the Octagon UI: an overall score
 * (underwriting.underwriting_score) blended from an Integrity axis and a Trade
 * quality axis, then a profile of the three integrity pillars and the three
 * event-level trade-quality components.
 *
 * Four per-market scores, each in [0, 100] and all HIGHER IS BETTER:
 *   - market_quality       (overall composite)
 *   - liquidity
 *   - move_quality
 *   - resolution_clarity
 *
 * Every score value is nullable — null means not applicable (e.g. no trades to
 * judge a move by) or suppressed for insufficient data. A null renders as "—",
 * never as 0. Prices inside the card are in cents.
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

/** A raw fact backing the score; shown with --verbose. */
export interface TrustEvidence {
  text: string;
  metric?: string;
  value?: unknown;
  window?: string;
  comparison?: string;
}

export interface TrustScore {
  value: number | null; // 0-100, null when not applicable or suppressed
  label: string;
  confidence: 'low' | 'medium' | 'high' | 'insufficient';
  suppressed: boolean;
  not_applicable: boolean;
  /** Pre-rendered "why" sentences, most important first. */
  drivers: string[];
  evidence?: TrustEvidence[];
  warning?: string;
}

export interface TrustMarket {
  market_ticker: string;
  title: string;
  is_primary: boolean;
  lifecycle_status?: string;
  fair_cents?: number | null;
  best_bid_cents?: number | null;
  best_ask_cents?: number | null;
  spread_cents?: number | null;
  scores: {
    market_quality: TrustScore;
    liquidity: TrustScore;
    move_quality: TrustScore;
    resolution_clarity: TrustScore;
  };
}

/** One pillar of the underwriting profile (e.g. information_fairness). */
export interface UnderwritingPillar {
  score: number | null;
  label: string;
  summary: string;
  factors: string[];
}

export interface Underwriting {
  calculation_version: string;
  profile_version: string;
  underwriting_score: number | null;
  underwriting_label: string;
  manipulation_resistance?: UnderwritingPillar;
  information_fairness?: UnderwritingPillar;
  settlement_reliability?: UnderwritingPillar;
  market_quality?: UnderwritingPillar;
  integrity_axis_score: number | null;
  integrity_axis_label: string;
  caps_detail?: unknown[];
}

export interface TraderTrustCard {
  calculation_version: string;
  computed_at: string;
  event_ticker: string;
  venue?: string;
  event?: {
    components?: {
      event_liquidity?: number | null;
      event_liquidity_label?: string;
      event_move_quality?: number | null;
      event_move_quality_label?: string;
      event_rule_clarity?: number | null;
      event_resolution?: string;
    };
  };
  integrity?: {
    structure?: { evidence?: Array<{ text: string }> };
    /** Integrity screens (detectors), keyed by name. */
    scores?: Record<string, TrustScore>;
  };
  underwriting?: Underwriting;
  markets: TrustMarket[];
}

/**
 * Axis weights by underwriting profile. They are not in the payload — the
 * Octagon UI hard-codes them — but for v0 they reproduce underwriting_blend
 * (0.8 × integrity + 0.2 × trade quality). Unknown profiles show no weights
 * rather than stale ones.
 */
const PROFILE_WEIGHTS: Record<string, { integrity: number; trade: number }> = {
  underwriting_profile_v0: { integrity: 80, trade: 20 },
};

/** Color a 0-100 score (higher = better); null renders as a muted dash. */
function colorScore(value: number | null | undefined): string {
  if (value === null || value === undefined) return theme.muted('  —');
  const str = value.toFixed(0).padStart(3);
  if (value >= 70) return theme.success(str);
  if (value >= 40) return theme.warning(str);
  return theme.error(str);
}

// Underwriting thresholds differ per axis (40 is "High Risk" for one pillar and
// "Mixed" for another), so the label, not the number, decides the color.
const GOOD_LABELS = new Set(['Strong', 'Clear', 'Tradeable', 'Confirmed']);
const BAD_LABELS = new Set(['High Risk', 'Very thin', 'Very weak']);

function colorByLabel(text: string, label: string): string {
  if (GOOD_LABELS.has(label)) return theme.success(text);
  if (BAD_LABELS.has(label)) return theme.error(text);
  return theme.warning(text);
}

/** "57  ● Caution" — bold number, colored status dot, label. */
function scoreCell(value: number | null | undefined, label = '', labelWidth = 0): string {
  const n = value === null || value === undefined ? '—' : String(value);
  return `${theme.bold(n.padStart(3))}  ${colorByLabel('●', label)} ${label.padEnd(labelWidth)}`;
}

/** Output shape for both views (machine-readable). */
export type TrustResult =
  | { kind: 'table'; card: TraderTrustCard; event_name: string | null; verbose: boolean }
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

  return wrapSuccess('trust', { kind: 'table', card, event_name: event.name ?? null, verbose: args.verbose });
}

export function formatTrustHuman(result: TrustResult): string {
  if (result.kind === 'table') return formatTrustIndex(result.card, result.event_name, result.verbose);
  return formatTrustDetail(result.card, result.market, result.verbose);
}

const SCORE_KEYS: Array<keyof TrustMarket['scores']> = [
  'market_quality',
  'liquidity',
  'move_quality',
  'resolution_clarity',
];

const SCORE_HEADER_LABELS: Record<keyof TrustMarket['scores'], string> = {
  market_quality: 'Quality',
  liquidity: 'Liquidity',
  move_quality: 'Move',
  resolution_clarity: 'Resol.',
};

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtCents(v: number | null | undefined): string {
  return v === null || v === undefined ? '—' : `${Number(v.toFixed(1))}¢`;
}

function formatTrustIndex(card: TraderTrustCard, eventName: string | null, verbose: boolean): string {
  const lines: string[] = [];
  const title = eventName ? ` · ${eventName}` : '';
  lines.push(theme.bold(`Trust Index — ${card.event_ticker}${title}`));
  lines.push(theme.muted('Trust Index combines Integrity and Trade quality.'));
  lines.push('');

  const uw = card.underwriting;
  if (uw) {
    lines.push(...formatUnderwriting(card, uw));
  } else {
    lines.push(`  No Trust Index in the scorecard for ${card.event_ticker} yet.`);
  }
  lines.push('');

  if (verbose) {
    lines.push(...formatPerContract(card));
    lines.push('');
  }

  const version = uw?.calculation_version ?? card.calculation_version;
  lines.push(theme.muted(`  Calculation ${version}  ·  Computed ${card.computed_at.slice(0, 16).replace('T', ' ')} UTC`));
  if (!verbose) lines.push(theme.muted(`  Per-contract market quality: trust ${card.event_ticker} --verbose`));
  lines.push(theme.muted(`  Drill into one market: trust ${card.event_ticker} --market <market_ticker> [--verbose]`));
  return lines.join('\n');
}

function formatUnderwriting(card: TraderTrustCard, uw: Underwriting): string[] {
  const lines: string[] = [];

  // Headline score, with a bar standing in for the UI's gauge
  const venue = card.venue ? ` · ${card.venue.toUpperCase()}` : '';
  lines.push(`  ${gauge(uw.underwriting_score, uw.underwriting_label)}  ${scoreCell(uw.underwriting_score, uw.underwriting_label)}`);
  lines.push(theme.muted(`  Octagon Trust Index${venue}`));

  const headline = card.integrity?.structure?.evidence?.[0]?.text;
  if (headline) {
    lines.push('');
    lines.push(`  ${headline}`);
    lines.push(`  ${theme.muted('Integrity risk ·')} Information exposure`);
  }

  // How it adds up
  const weights = PROFILE_WEIGHTS[uw.profile_version];
  const weight = (pct: number | undefined) => theme.muted((pct === undefined ? '' : `${pct}% of score`).padEnd(14));
  lines.push('');
  lines.push(theme.muted('  HOW IT ADDS UP'));
  lines.push(`  ${'Integrity'.padEnd(16)}${weight(weights?.integrity)}${scoreCell(uw.integrity_axis_score, uw.integrity_axis_label)}`);
  lines.push(`  ${'Trade quality'.padEnd(16)}${weight(weights?.trade)}${scoreCell(uw.market_quality?.score, uw.market_quality?.label)}`);
  const cost = tradeCostSentence(uw.market_quality?.factors ?? []);
  if (cost) lines.push(`    ${cost}`);
  lines.push(theme.muted(`  ${'─'.repeat(46)}`));
  lines.push(`  ${'= Trust score'.padEnd(30)}${scoreCell(uw.underwriting_score, uw.underwriting_label)}`);
  if (uw.caps_detail && uw.caps_detail.length > 0) {
    lines.push(`  ${theme.warning('Caps applied:')} ${uw.caps_detail.map(formatEvidenceValue).join('; ')}`);
  }
  lines.push(theme.muted('  Weighted blend with hard caps — a critically weak safety pillar, or a severe'));
  lines.push(theme.muted('  trading anomaly, caps the total regardless of the rest.'));

  // Trust profile
  lines.push('');
  lines.push(theme.muted('  TRUST PROFILE'));
  const counts = integrityCounts(card);
  lines.push(`  ${theme.bold('Integrity')}${counts ? `   ${theme.muted(counts)}` : ''}`);
  lines.push(pillarRow('Market integrity', uw.manipulation_resistance));
  lines.push(pillarRow('Info fairness', uw.information_fairness));
  lines.push(pillarRow('Resolution quality', uw.settlement_reliability));
  const c = card.event?.components;
  lines.push(`  ${theme.bold('Trade quality')}`);
  lines.push(profileRow('Liquidity', c?.event_liquidity, c?.event_liquidity_label));
  lines.push(profileRow('Move quality', c?.event_move_quality, c?.event_move_quality_label));
  lines.push(profileRow('Rule clarity', c?.event_rule_clarity, c?.event_resolution));
  return lines;
}

function gauge(value: number | null, label: string): string {
  const width = 30;
  const filled = value === null ? 0 : Math.round((value / 100) * width);
  return colorByLabel('█'.repeat(filled), label) + theme.muted('░'.repeat(width - filled));
}

function profileRow(name: string, value: number | null | undefined, label: string | undefined, summary?: string): string {
  const tail = summary ? `  ${theme.muted(summary)}` : '';
  return `    ${name.padEnd(20)}${scoreCell(value, label ?? '', 10)}${tail}`.trimEnd();
}

function pillarRow(name: string, pillar: UnderwritingPillar | undefined): string {
  return profileRow(name, pillar?.score, pillar?.label, pillar?.summary);
}

/** "4 screens run · 3 don't apply · 3 awaiting data" over the integrity detectors. */
function integrityCounts(card: TraderTrustCard): string | null {
  const screens = Object.values(card.integrity?.scores ?? {});
  if (screens.length === 0) return null;
  const run = screens.filter((s) => s.value !== null).length;
  const na = screens.filter((s) => s.not_applicable).length;
  const waiting = screens.filter((s) => s.suppressed).length;
  return `${run} screens run · ${na} don't apply · ${waiting} awaiting data`;
}

/** Turn the "$1,000 order: …" trade-quality factor into the UI's sentence. */
function tradeCostSentence(factors: string[]): string | null {
  const prefix = '$1,000 order:';
  const factor = factors.find((f) => f.startsWith(prefix));
  if (!factor) return null;
  const rest = factor.slice(prefix.length).trim();
  return rest === 'book too thin to fill'
    ? "Includes the cost to trade: a $1,000 order can't be filled here because the order book is too thin."
    : `Includes the cost to trade: a $1,000 order costs ${rest}.`;
}

function formatPerContract(card: TraderTrustCard): string[] {
  // Sort by market quality desc (unscored last); the best markets surface first.
  const quality = (m: TrustMarket) => m.scores?.market_quality?.value ?? -1;
  const sorted = card.markets.slice().sort((a, b) => quality(b) - quality(a));
  const rows: string[][] = sorted.map((m) => [
    m.is_primary ? '*' : ' ',
    m.market_ticker,
    truncate(m.title, 30),
    colorScore(m.scores?.market_quality?.value),
    theme.muted(m.scores?.market_quality?.label ?? ''),
  ]);
  return [
    theme.muted('  PER-CONTRACT MARKET QUALITY'),
    formatTable(['', 'Market', 'Title', 'Quality', 'Label'], rows),
    theme.muted('  * = primary outcome.  Higher is better; — = not scored (not applicable or insufficient data).'),
  ];
}

function formatTrustDetail(card: TraderTrustCard, market: TrustMarket, verbose: boolean): string {
  const lines: string[] = [];
  const primaryMark = market.is_primary ? ' (primary)' : '';
  lines.push(`Trader Trust — ${market.market_ticker}${primaryMark}`);
  lines.push(`  ${market.title}`);
  lines.push(`  Event ${card.event_ticker}  ·  Calculation ${card.calculation_version}  ·  Computed ${card.computed_at.slice(0, 16).replace('T', ' ')} UTC`);
  lines.push(`  Fair ${fmtCents(market.fair_cents)}  ·  Bid ${fmtCents(market.best_bid_cents)} / Ask ${fmtCents(market.best_ask_cents)}  ·  Spread ${fmtCents(market.spread_cents)}`);
  lines.push('');

  for (const key of SCORE_KEYS) {
    const score = market.scores?.[key];
    const label = SCORE_HEADER_LABELS[key].padEnd(10);
    if (!score) {
      lines.push(`  ${label}  ${colorScore(null)}      ${theme.muted('not reported')}`);
      lines.push('');
      continue;
    }
    const valueStr = score.value === null ? `${colorScore(null)}    ` : `${colorScore(score.value)}/100`;
    const why = score.value !== null ? ''
      : score.not_applicable ? ' (not applicable)'
      : score.suppressed ? ' (insufficient data)' : '';
    lines.push(`  ${label}  ${valueStr}  ${theme.muted(score.label)}${why}`);
    if (score.warning) lines.push(`      ${theme.warning(score.warning)}`);
    for (const d of score.drivers.slice(0, 3)) {
      lines.push(`      • ${d}`);
    }
    if (verbose) {
      const evidence = score.evidence ?? [];
      if (evidence.length > 0) {
        lines.push(theme.muted(`      Evidence:`));
        for (const e of evidence) lines.push(theme.muted(`        ${formatEvidence(e)}`));
      }
      lines.push(theme.muted(`      Confidence: ${score.confidence}`));
    }
    lines.push('');
  }
  return lines.join('\n');
}

function formatEvidence(e: TrustEvidence): string {
  if (!e.metric) return e.text;
  const window = e.window ? ` (${e.window})` : '';
  return `${e.metric}: ${formatEvidenceValue(e.value)}${window}`;
}

function formatEvidenceValue(v: unknown): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return v.toString();
  if (typeof v === 'string') return v;
  try { return JSON.stringify(v); } catch { return String(v); }
}
