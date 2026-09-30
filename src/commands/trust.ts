/**
 * Trader Trust scorecard.
 *
 * Surfaces Octagon's Trust Index from the Reports API's trust endpoint,
 * /v1/predictions/reports/kalshi/{event_ticker}/trust.
 *
 * Views:
 *   - kalshi trust <event-ticker>                  → Trust Index (score + profile)
 *   - kalshi trust <event-ticker> --verbose        → …plus per-contract market quality
 *   - kalshi trust <event-ticker> --market <mkt>   → single-market detail card
 *
 * The Trust Index mirrors the Octagon UI: an overall score blended from an
 * Integrity axis and a Trade quality axis, then a profile of the three
 * integrity pillars and the three event-level trade-quality components.
 *
 * Four per-market scores, each in [0, 100] and all HIGHER IS BETTER:
 *   - market_quality       (overall composite)
 *   - liquidity
 *   - move_quality
 *   - resolution_clarity
 *
 * Any score can be null — not applicable (no recent move, no book) or too thin
 * to publish. Nulls render as "—" rather than as a zero, which would read as a
 * damning score rather than an absent one. Prices inside the card are in cents.
 *
 * The per-market cards come back only with expand=trade_quality, so they are
 * requested only for the views that show them.
 */
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  fetchTrustIndex,
  OctagonReportsApiError,
  type TrustIndex,
  type TrustIndexResponse,
  type TrustMarket,
  type TrustPillar,
} from '../scan/octagon-reports-api.js';
import { formatTable } from './scan-formatters.js';
import { theme } from '../theme.js';

/** Color a 0-100 score. Every score in this card is higher-is-better. */
function colorScore(value: number | null | undefined): string {
  if (value === null || value === undefined) return theme.muted('  —');
  const str = value.toFixed(0).padStart(3);
  if (value >= 70) return theme.success(str);
  if (value >= 40) return theme.warning(str);
  return theme.error(str);
}

// Thresholds differ per axis (40 is "High Risk" for one pillar and "Mixed" for
// another), so the label, not the number, decides the color.
const GOOD_LABELS = new Set(['Strong', 'Good', 'Clear', 'Tradeable', 'Confirmed']);
const BAD_LABELS = new Set(['High Risk', 'Avoid', 'Very thin', 'Very weak']);

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
  | { kind: 'table'; trust: TrustIndexResponse; verbose: boolean }
  | { kind: 'detail'; trust: TrustIndexResponse; market: TrustMarket; verbose: boolean };

export async function handleTrust(args: ParsedArgs): Promise<CLIResponse<TrustResult>> {
  const eventTicker = args.positionalArgs[0]?.toUpperCase();
  if (!eventTicker) {
    return wrapError('trust', 'MISSING_EVENT', 'Usage: trust <event_ticker> [--market <market_ticker>] [--verbose]');
  }
  const needsMarkets = args.verbose || Boolean(args.market);

  let trust: TrustIndexResponse;
  try {
    trust = await fetchTrustIndex(eventTicker, { expand: needsMarkets ? ['trade_quality'] : [] });
  } catch (err) {
    if (err instanceof OctagonReportsApiError && err.statusCode === 404) {
      if (err.code === 'trust_index_not_found') {
        return wrapError(
          'trust',
          'NO_SCORECARD',
          `No trust scorecard for ${eventTicker} yet. The Trust Index may not have run for this event's latest report — try again after the next Octagon refresh.`,
        );
      }
      return wrapError('trust', 'EVENT_NOT_FOUND', `No Octagon report for event ${eventTicker}.`);
    }
    return wrapError('trust', 'OCTAGON_ERROR', err instanceof Error ? err.message : String(err));
  }

  if (!needsMarkets) return wrapSuccess('trust', { kind: 'table', trust, verbose: false });

  const markets = trust.trust_index.profile.trade_quality.markets ?? [];
  if (markets.length === 0) {
    return wrapError('trust', 'EMPTY_SCORECARD', `Trust scorecard for ${eventTicker} has no scored markets.`);
  }

  // Single-market detail view
  if (args.market) {
    const wanted = args.market.toUpperCase();
    const market = markets.find((m) => m.market_ticker.toUpperCase() === wanted);
    if (!market) {
      return wrapError(
        'trust',
        'MARKET_NOT_IN_SCORECARD',
        `Market ${wanted} is not in the trust scorecard for ${eventTicker}. Run \`trust ${eventTicker} --verbose\` to see the available markets.`,
      );
    }
    return wrapSuccess('trust', { kind: 'detail', trust, market, verbose: args.verbose });
  }

  return wrapSuccess('trust', { kind: 'table', trust, verbose: true });
}

export function formatTrustHuman(result: TrustResult): string {
  if (result.kind === 'table') return formatTrustIndex(result.trust, result.verbose);
  return formatTrustDetail(result.trust, result.market, result.verbose);
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

function fmtCents(v: number | null | undefined): string {
  return v === null || v === undefined ? '—' : `${Number(v.toFixed(1))}¢`;
}

function fmtComputedAt(ti: TrustIndex): string {
  return `${ti.computed_at.slice(0, 16).replace('T', ' ')} UTC`;
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function formatTrustIndex(trust: TrustIndexResponse, verbose: boolean): string {
  const ti = trust.trust_index;
  const lines: string[] = [];
  const title = trust.title ? ` · ${trust.title}` : '';
  lines.push(theme.bold(`Octagon Trust Index — ${trust.event_ticker}${title}`));
  lines.push(theme.muted('Trust Index combines Integrity and Trade quality.'));
  lines.push('');
  lines.push(...formatScorecard(trust));
  lines.push('');

  if (verbose) {
    lines.push(...formatPerContract(trust));
    lines.push('');
  }

  lines.push(theme.muted(`  Calculation ${ti.version}  ·  Computed ${fmtComputedAt(ti)}`));
  if (!verbose) lines.push(theme.muted(`  Per-contract market quality: trust ${trust.event_ticker} --verbose`));
  lines.push(theme.muted(`  Drill into one market: trust ${trust.event_ticker} --market <market_ticker> [--verbose]`));
  return lines.join('\n');
}

function formatScorecard(trust: TrustIndexResponse): string[] {
  const ti = trust.trust_index;
  const { integrity, trade_quality: trade } = ti.profile;
  const lines: string[] = [];

  // Headline score, with a bar standing in for the UI's gauge
  lines.push(`  ${gauge(ti.score, ti.label)}  ${scoreCell(ti.score, ti.label)}`);

  if (integrity.risk) {
    lines.push('');
    lines.push(`  ${theme.muted('Integrity risk ·')} ${integrity.risk.label}`);
  }

  // How it adds up
  lines.push('');
  lines.push(theme.muted('  HOW IT ADDS UP'));
  lines.push(`  ${'Integrity'.padEnd(30)}${scoreCell(integrity.score, integrity.label ?? '')}`);
  lines.push(`  ${'Trade quality'.padEnd(30)}${scoreCell(trade.score, trade.label ?? '')}`);
  const cost = tradeCostSentence(trade.factors);
  if (cost) lines.push(`    ${cost}`);
  lines.push(theme.muted(`  ${'─'.repeat(46)}`));
  const uncapped = ti.uncapped_score != null && ti.uncapped_score !== ti.score
    ? theme.muted(`  (${ti.uncapped_score} before caps)`)
    : '';
  lines.push(`  ${'= Trust score'.padEnd(30)}${scoreCell(ti.score, ti.label)}${uncapped}`);
  if (ti.caps.length > 0) {
    lines.push(`  ${theme.warning('Caps applied:')} ${ti.caps.map(formatCap).join('; ')}`);
  }
  if (ti.floors_breached.length > 0) {
    const pillarName = (key: string) => integrity.breakdown.find((p) => p.key === key)?.name ?? key;
    const breaches = ti.floors_breached.map((b) => `${pillarName(b.key)} ${b.score} (floor ${b.floor})`);
    lines.push(`  ${theme.warning('Below safety floor:')} ${breaches.join('; ')}`);
  }
  lines.push(theme.muted('  Weighted blend with hard caps — a critically weak safety pillar, or a severe'));
  lines.push(theme.muted('  trading anomaly, caps the total regardless of the rest.'));

  // Trust profile
  lines.push('');
  lines.push(theme.muted('  TRUST PROFILE'));
  const counts = integrity.screen_counts
    ? `${integrity.screen_counts.run} screens run · ${integrity.screen_counts.not_applicable} don't apply · ${integrity.screen_counts.awaiting_data} awaiting data`
    : null;
  lines.push(`  ${theme.bold('Integrity')}${counts ? `   ${theme.muted(counts)}` : ''}`);
  for (const pillar of integrity.breakdown) lines.push(pillarRow(pillar));
  lines.push(`  ${theme.bold('Trade quality')}`);
  for (const c of trade.breakdown) lines.push(profileRow(c.name, c.score, c.label));
  return lines;
}

function formatCap(cap: TrustIndex['caps'][number]): string {
  return cap.ceiling === null ? cap.name : `${cap.name} (capped at ${cap.ceiling})`;
}

function gauge(value: number, label: string): string {
  const width = 30;
  const filled = Math.round((value / 100) * width);
  return colorByLabel('█'.repeat(filled), label) + theme.muted('░'.repeat(width - filled));
}

function profileRow(name: string, value: number | null, label: string | null, summary?: string | null): string {
  const tail = summary ? `  ${theme.muted(summary)}` : '';
  return `    ${name.padEnd(24)}${scoreCell(value, label ?? '', 10)}${tail}`.trimEnd();
}

function pillarRow(pillar: TrustPillar): string {
  return profileRow(pillar.name, pillar.score, pillar.label, pillar.summary);
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

function formatPerContract(trust: TrustIndexResponse): string[] {
  const trade = trust.trust_index.profile.trade_quality;
  // Sort by market quality desc (unscored last); the best markets surface first.
  const quality = (m: TrustMarket) => m.scores.market_quality?.score ?? -1;
  const sorted = (trade.markets ?? []).slice().sort((a, b) => quality(b) - quality(a));
  const rows: string[][] = sorted.map((m) => [
    m.is_primary ? '*' : ' ',
    m.market_ticker,
    truncate(m.title ?? '', 30),
    colorScore(m.scores.market_quality?.score),
    theme.muted(m.scores.market_quality?.label ?? ''),
  ]);
  const lines = [
    theme.muted('  PER-CONTRACT MARKET QUALITY'),
    formatTable(['', 'Market', 'Title', 'Quality', 'Label'], rows),
    theme.muted('  * = primary outcome.  Higher is better; — = not scored (not applicable or insufficient data).'),
  ];
  const ex = trade.exclusions;
  if (ex && ex.total > 0) {
    const reasons = [
      ex.terminal_lifecycle ? `${ex.terminal_lifecycle} closed` : null,
      ex.below_volume_floor ? `${ex.below_volume_floor} below volume floor` : null,
      ex.no_ticker ? `${ex.no_ticker} without a ticker` : null,
    ].filter(Boolean).join(', ');
    lines.push(theme.muted(`  ${ex.total} market${ex.total === 1 ? '' : 's'} left out of the read${reasons ? ` (${reasons})` : ''}.`));
  }
  return lines;
}

function formatTrustDetail(trust: TrustIndexResponse, market: TrustMarket, verbose: boolean): string {
  const ti = trust.trust_index;
  const lines: string[] = [];
  const primaryMark = market.is_primary ? ' (primary)' : '';
  lines.push(`Trader Trust — ${market.market_ticker}${primaryMark}`);
  if (market.title) lines.push(`  ${market.title}`);
  lines.push(`  Event ${trust.event_ticker}  ·  Calculation ${ti.version}  ·  Computed ${fmtComputedAt(ti)}`);
  if (market.last_trade_cents !== null) lines.push(`  ${theme.muted(`Last trade ${fmtCents(market.last_trade_cents)}`)}`);
  lines.push('');

  for (const key of SCORE_KEYS) {
    const score = market.scores[key];
    const label = SCORE_HEADER_LABELS[key].padEnd(10);
    if (!score) {
      lines.push(`  ${label}  ${colorScore(null)}      ${theme.muted('not reported')}`);
      lines.push('');
      continue;
    }
    const why = score.score !== null ? ''
      : score.not_applicable ? ' (not applicable)' : ' (insufficient data)';
    const valueStr = score.score === null ? `${colorScore(null)}    ` : `${colorScore(score.score)}/100`;
    lines.push(`  ${label}  ${valueStr}  ${theme.muted(score.label ?? '')}${why}`);
    if (score.warning) lines.push(`      ${theme.warning(score.warning)}`);
    for (const d of score.drivers.slice(0, 3)) {
      lines.push(`      • ${d}`);
    }
    if (verbose) {
      if (score.evidence.length > 0) {
        lines.push(theme.muted(`      Evidence:`));
        for (const e of score.evidence) {
          const window = e.window ? ` [${e.window}]` : '';
          lines.push(theme.muted(`        ${e.text}${window}`));
        }
      }
      for (const [check, result] of Object.entries(score.checks ?? {})) {
        lines.push(theme.muted(`      Check ${check}: ${result}`));
      }
      if (score.confidence) lines.push(theme.muted(`      Confidence: ${score.confidence}`));
    }
    lines.push('');
  }
  return lines.join('\n');
}
