import { callKalshiApi } from '../tools/kalshi/api.js';
import { buildV2Order, cancelOrderV2, placeOrderV2 } from '../tools/kalshi/orders-v2.js';
import type { KalshiOrder, KalshiPosition } from '../tools/kalshi/types.js';
import type { KalshiBalanceResponse } from './formatters.js';
import {
  formatBalance,
  formatPositions,
  formatOrders,
  formatExchangeStatus,
  formatOrderConfirmation,
} from './formatters.js';
import { handleThemes, formatThemesHuman } from './themes.js';
import type { ParsedArgs, Subcommand } from './parse-args.js';

function defaultArgs(overrides: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'chat', positionalArgs: [], json: false,
    live: false, refresh: false, report: false, dryRun: false,
    verbose: false, performance: false, resolved: false,
    unresolved: false,
    behavioral: false, ranked: false, showCluster: false,
    activeOnly: false, cells: false, autoProbs: false,
    parseErrors: [],
    ...overrides,
  };
}
import { handleBacktest, formatBacktestHuman } from './backtest.js';
import { handleAnalyze, formatAnalyzeHuman } from './analyze.js';
import { handlePortfolio, formatPortfolioHuman } from './portfolio.js';
import { reviewPortfolio, formatReviewHuman } from './review.js';
import { buildHelp, validateTradeArgs } from './help.js';
import { fetchMarketQuote } from './helpers.js';
import { trackEvent } from '../utils/telemetry.js';
import { parseArgs } from './parse-args.js';
import { handleSimilar, formatSimilarHuman } from './similar.js';
import { handleClusters, formatClustersHuman } from './clusters.js';
import { handlePeers, formatPeersHuman } from './peers.js';
import { handleCorrelate, formatCorrelationHuman } from './correlate.js';
import { handleBasket, formatBasketHuman } from './basket.js';
import { handleEvents, formatEventsHuman } from './events.js';
import { handleTrust, formatTrustHuman } from './trust.js';
import { handleReport, formatReportHuman } from './report.js';
import { handleSeries, formatSeriesHuman } from './series.js';
import { handleEditorialThemes, formatEditorialThemesHuman } from './editorial-themes.js';
import { handleCatalysts, formatCatalystsHuman } from './catalysts.js';
import { handleOctagonChat } from './octagon-chat.js';
import { getDb } from '../db/index.js';
import { syncSettlements } from '../tools/kalshi/settle.js';
import { getSettlements, summarizeSettlements } from '../db/settlements.js';
import { computeCalibration, formatCalibrationHuman } from '../eval/calibration.js';
import { categoryOf } from '../eval/calibration.js';
import { generateMissingLessons, getLessonsForCategory, getRecentLessons, formatLessonsForContext } from '../eval/reflection.js';
import { addHypothesis, resolveHypothesis, listHypotheses, scoreboard, formatHypothesesHuman, type HypothesisStatus } from '../db/hypotheses.js';
import { getMandateStatus, formatMandateHuman, activateKillSwitch, deactivateKillSwitch, MandateViolation } from '../risk/mandate.js';
import { needsBearCheck, runBearCheck, formatBearCheck, type BearCheckLlm } from '../eval/bear-check.js';
import { formatIndependenceNote, type ModelProvenance } from '../scan/model-independence.js';
import { openPaperPosition, settlePaperPositions, listPaperPositions, paperSummary, formatPaperHuman, winningSide } from './paper.js';
import { computeVariantLeaderboard, formatVariantLeaderboard } from '../backtest/variants.js';

/** Fast-model structured-output wrapper for the bear check. */
function makeBearCheckLlm(): BearCheckLlm | undefined {
  try {
    const provider = resolveProvider(DEFAULT_MODEL);
    const model = getFastModel(provider.id, DEFAULT_MODEL);
    return async (prompt, schema) => {
      const res = await callLlm(prompt, { model, outputSchema: schema });
      return typeof res.response === 'string' ? JSON.parse(res.response) : res.response;
    };
  } catch {
    return undefined;
  }
}
import { callLlm, getFastModel, DEFAULT_MODEL } from '../model/llm.js';
import { resolveProvider } from '../providers.js';

/** Fast-model wrapper for reflection; null when no provider key is configured. */
export function makeReflectionLlm(): ((prompt: string) => Promise<string>) | undefined {
  try {
    const provider = resolveProvider(DEFAULT_MODEL);
    const model = getFastModel(provider.id, DEFAULT_MODEL);
    return async (prompt: string) => {
      const res = await callLlm(prompt, {
        model,
        systemPrompt: 'You write terse, falsifiable trading lessons. One sentence, max 30 words, no preamble.',
      });
      const c = typeof res.response === 'string' ? res.response : String(res.response.content ?? '');
      return c;
    };
  } catch {
    return undefined;
  }
}

export interface CommandResult {
  output: string;
  /** If set, show this as a pending trade requiring approval */
  pendingTrade?: {
    ticker: string;
    action: 'buy' | 'sell';
    side: 'yes' | 'no';
    count: number;
    price: number | undefined;
  };
  /** If set, run this async function after showing `output` and append the result */
  asyncFollowUp?: () => Promise<string>;
}

/**
 * Split a command line into tokens, honoring double/single quotes so
 * multi-word values survive: `--theme "Bitcoin Breakout"` → ['--theme', 'Bitcoin Breakout'].
 * Unterminated quotes fall back to whitespace splitting of the remainder.
 */
export function tokenizeCommand(line: string): string[] {
  // A token is a run of bare characters and/or quoted segments with no
  // whitespace between them, so attached values like --theme="Bitcoin
  // Breakout" stay one token. Double quotes always delimit; single quotes
  // delimit only at a token boundary or after '=', so apostrophes inside
  // words (don't, market's) pass through untouched. Unterminated quotes
  // degrade to bare text.
  const tokens: string[] = [];
  let current = '';
  let started = false;
  let i = 0;

  const flush = () => {
    if (started) tokens.push(current);
    current = '';
    started = false;
  };

  while (i < line.length) {
    const ch = line[i];
    if (/\s/.test(ch)) {
      flush();
      i++;
      continue;
    }
    const atBoundary = !started || current.endsWith('=');
    if (ch === '"' || (ch === "'" && atBoundary)) {
      const close = line.indexOf(ch, i + 1);
      if (close === -1) {
        // Unterminated: drop the quote char, keep the text.
        started = true;
        i++;
        continue;
      }
      current += line.slice(i + 1, close);
      started = true;
      i = close + 1;
      continue;
    }
    current += ch;
    started = true;
    i++;
  }
  flush();
  return tokens;
}

export async function handleSlashCommand(input: string): Promise<CommandResult | null> {
  const trimmed = input.trim();
  if (!trimmed.startsWith('/')) return null;

  const parts = tokenizeCommand(trimmed.slice(1).trim());
  return executeSlashCommand(parts[0]?.toLowerCase(), parts.slice(1));
}

/**
 * Execute a slash command from pre-tokenized arguments. CLI dispatch calls
 * this directly with argv tokens so shell-quoted values ("Fed won't cut")
 * are never flattened and re-tokenized.
 */
export async function executeSlashCommand(command: string | undefined, args: string[]): Promise<CommandResult | null> {
  // Enrich Octagon-Kalshi commands with subview/mode flags so analytics can
  // distinguish e.g. "basket build" vs "basket backtest", or thematic vs
  // behavioral clusters. Outer command name is always tracked.
  const slashMeta: Record<string, string | boolean> = { command: command ?? '' };
  if (command === 'basket') {
    const sub = args[0]?.toLowerCase();
    if (sub === 'build' || sub === 'backtest' || sub === 'size' || sub === 'candles') {
      slashMeta.subview = sub;
    }
    slashMeta.kelly_sizing = args.includes('--bankroll');
  } else if (command === 'clusters') {
    slashMeta.behavioral = args.includes('--behavioral');
    slashMeta.ranked = args.includes('--ranked');
  } else if (command === 'peers') {
    slashMeta.behavioral = args.includes('--behavioral');
    slashMeta.show_cluster = args.includes('--show-cluster');
  } else if (command === 'similar') {
    slashMeta.anchor = args.includes('-q') || args.includes('--query') ? 'query' : 'ticker';
  } else if (command === 'search') {
    slashMeta.remote = !!process.env.OCTAGON_API_KEY;
  }
  trackEvent('slash_command', slashMeta);

  switch (command) {
    case 'help': {
      const result = buildHelp('slash', args[0]);
      return { output: 'error' in result ? result.error : result.text };
    }

    // ─── /portfolio (with subviews) ──────────────────────────────────
    case 'portfolio':
      return handlePortfolioSlash(args[0]);

    // Hidden aliases → /portfolio <subview>
    case 'status':
      return handlePortfolioSlash('status');
    case 'balance':
      return handlePortfolioSlash('balance');
    case 'positions':
      return handlePortfolioSlash('positions');
    case 'orders':
      return handlePortfolioSlash('orders');
    case 'settlements':
      return handlePortfolioSlash('settlements');

    // ─── Trading ─────────────────────────────────────────────────────
    case 'buy':
      return handleTradeCommand('buy', args);
    case 'sell':
      return handleTradeCommand('sell', args);
    case 'cancel':
      return handleCancel(args[0]);

    // ─── /themes (editorial registry) ────────────────────────────────
    // The bare /themes call now hits the editorial-themes registry. Legacy
    // "Kalshi category labels" is still reachable via /search themes.
    case 'themes': {
      const parsed = parseArgs(['themes', ...args]);
      const sub = parsed.positionalArgs[0]?.toLowerCase();
      const isAsync = sub === 'report' || sub === 'audit';
      if (!isAsync) {
        const resp = await handleEditorialThemes(parsed);
        return { output: resp.ok ? formatEditorialThemesHuman(resp.data) : (resp.error?.message ?? 'themes failed') };
      }
      return {
        output: `Building themes ${sub} (this pulls the full Kalshi universe)...`,
        asyncFollowUp: async () => {
          const resp = await handleEditorialThemes(parsed);
          return resp.ok ? formatEditorialThemesHuman(resp.data) : (resp.error?.message ?? 'themes failed');
        },
      };
    }

    // ─── /variants (strategy-variant leaderboard over backtest signals) ─
    case 'variants': {
      // Same flag surface as /backtest — one pipeline, segmented lenses.
      const vArgs: Partial<ParsedArgs> = { subcommand: 'backtest' };
      for (let i = 0; i < args.length; i++) {
        const a = args[i];
        if (a === '--resolved') vArgs.resolved = true;
        else if (a === '--unresolved') vArgs.unresolved = true;
        else if (a === '--category') vArgs.category = args[++i];
        else if (a === '--days') { const v = Number(args[++i]); if (Number.isFinite(v) && v > 0) vArgs.days = v; }
        else if (a === '--min-edge') { const v = Number(args[++i]?.replace('%', '')); if (Number.isFinite(v)) vArgs.minEdge = v / 100; }
        else if (a === '--min-volume') { const v = Number(args[++i]); if (Number.isFinite(v) && v >= 0) vArgs.minVolume = v; }
      }
      return {
        output: 'Scoring signals and segmenting variants (reuses the backtest pipeline)...',
        asyncFollowUp: async () => {
          const resp = await handleBacktest(defaultArgs(vArgs));
          if (!resp.ok) return resp.error?.message ?? 'variants failed';
          const minEdgePp = (vArgs.minEdge ?? 0.005) * 100;
          const rows = computeVariantLeaderboard(resp.data.signals, minEdgePp);
          return formatVariantLeaderboard(rows, minEdgePp);
        },
      };
    }

    // ─── /paper (forward-test ledger — no exchange orders) ──────────
    case 'paper': {
      const db = getDb();
      const sub = args[0]?.toLowerCase();
      if (sub === 'buy' || sub === 'sell') {
        // Same argument shape as /buy: <ticker> <count> [price] [yes|no]
        const [ticker, countStr, ...rest] = args.slice(1);
        if (!ticker || !countStr) {
          return { output: `Usage: /paper ${sub} <ticker> <count> [price_in_cents] [yes|no]` };
        }
        const side = parseSide(rest.find((r) => parseSide(r) !== null)) ?? 'yes';
        const priceStr = rest.find((r) => /^\d+$/.test(r));
        const validation = validateTradeArgs(countStr, priceStr);
        if ('error' in validation) return { output: validation.error };
        let entry = validation.price;
        if (entry === undefined) {
          const quote = await fetchMarketQuote(ticker.toUpperCase(), sub, side);
          if ('error' in quote) return { output: quote.error };
          entry = quote.cents;
        }
        const id = openPaperPosition(db, { ticker: ticker.toUpperCase(), action: sub, side, count: validation.count, priceCents: entry });
        return { output: `Paper position #${id}: ${sub.toUpperCase()} ${side.toUpperCase()} x${validation.count} ${ticker.toUpperCase()} @ ${entry}¢ (no exchange order). Settles automatically when the market resolves.` };
      }
      // default: settle pass + view
      return {
        output: 'Checking open paper positions for settlements...',
        asyncFollowUp: async () => {
          const settled = await settlePaperPositions(db).catch(() => 0);
          return formatPaperHuman(listPaperPositions(db), paperSummary(db), settled);
        },
      };
    }

    // ─── /mandate /kill /resume (hard caps + instant halt) ──────────
    case 'mandate':
      return { output: formatMandateHuman(getMandateStatus()) };
    case 'kill': {
      const reason = args.join(' ').trim() || undefined;
      activateKillSwitch(reason);
      return { output: `⛔ Kill switch ACTIVE${reason ? ` — ${reason}` : ''}. All new orders will be refused. /resume to lift.` };
    }
    case 'resume': {
      deactivateKillSwitch();
      return { output: '✓ Kill switch lifted — trading enabled (mandate caps still apply).' };
    }

    // ─── /hypothesis (falsifiable-claim registry) ───────────────────
    case 'hypothesis':
    case 'hypotheses': {
      const db = getDb();
      const sub = args[0]?.toLowerCase();
      if (sub === 'add') {
        // Flags may come before or after the claim; anything that isn't a
        // recognized flag (or its value) is claim text.
        let ticker: string | undefined;
        let side: 'yes' | 'no' | undefined;
        const claimParts: string[] = [];
        for (let i = 1; i < args.length; i++) {
          if (args[i] === '--ticker') ticker = args[++i]?.toUpperCase();
          else if (args[i] === '--side') { const v = args[++i]?.toLowerCase(); if (v === 'yes' || v === 'no') side = v; }
          else claimParts.push(args[i]);
        }
        const claim = claimParts.join(' ').trim();
        if (!claim) return { output: 'Usage: /hypothesis add "claim" [--ticker KX... --side yes|no]' };
        const id = addHypothesis(db, { claim, ticker, predictedSide: side });
        return { output: `Hypothesis #${id} registered${ticker ? ` (auto-resolves when ${ticker} settles)` : ''}.` };
      }
      if (sub === 'resolve' || sub === 'retire') {
        const id = Number(args[1]);
        if (!Number.isInteger(id)) return { output: `Usage: /hypothesis ${sub} <id>${sub === 'resolve' ? ' <confirmed|refuted> [note]' : ' [note]'}` };
        const status = sub === 'retire' ? 'retired' : (args[2]?.toLowerCase() as 'confirmed' | 'refuted');
        if (sub === 'resolve' && status !== 'confirmed' && status !== 'refuted') {
          return { output: 'Usage: /hypothesis resolve <id> <confirmed|refuted> [note]' };
        }
        const note = args.slice(sub === 'retire' ? 2 : 3).join(' ') || undefined;
        const ok = resolveHypothesis(db, id, status, note);
        return { output: ok ? `Hypothesis #${id} ${status}.` : `Hypothesis #${id} not found or already resolved.` };
      }
      // list (default), optional status filter
      const statusFilter = (sub === 'list' ? args[1] : sub)?.toLowerCase() as HypothesisStatus | undefined;
      const valid = ['open', 'confirmed', 'refuted', 'expired', 'retired'];
      const rows = listHypotheses(db, statusFilter && valid.includes(statusFilter) ? statusFilter : undefined);
      return { output: formatHypothesesHuman(rows, scoreboard(db)) };
    }

    // ─── /reflect (generate lessons from settled positions) ─────────
    case 'reflect': {
      return {
        output: 'Reflecting on settled positions...',
        asyncFollowUp: async () => {
          const db = getDb();
          await syncSettlements(db).catch(() => { /* offline: use local ledger */ });
          const result = await generateMissingLessons(db, { llm: makeReflectionLlm() });
          const recent = getRecentLessons(db, 10);
          const lines: string[] = [];
          lines.push(result.generated > 0
            ? `Generated ${result.generated} lesson${result.generated === 1 ? '' : 's'} (${result.source}).`
            : 'No settled positions awaiting reflection.');
          if (recent.length > 0) {
            lines.push('');
            lines.push('Recent lessons:');
            for (const l of recent) lines.push(`  - [${l.settled_time.slice(0, 10)}] ${l.lesson}`);
          }
          return lines.join('\n');
        },
      };
    }

    // ─── /calibration (realized-outcome Brier / skill report) ───────
    case 'calibration': {
      return {
        output: 'Computing calibration from settled positions...',
        asyncFollowUp: async () => {
          const db = getDb();
          await syncSettlements(db).catch(() => { /* offline: render from local ledger */ });
          return formatCalibrationHuman(computeCalibration(db));
        },
      };
    }

    // ─── /octagon (conversational Prediction Markets Agent) ─────────
    case 'octagon': {
      const res = handleOctagonChat(args);
      if ('followUp' in res) {
        return { output: res.output, asyncFollowUp: res.followUp };
      }
      return { output: res.output };
    }

    // ─── /analyze ────────────────────────────────────────────────────
    case 'analyze':
      return handleAnalyzeCommand(args);

    // ─── /review ─────────────────────────────────────────────────────
    case 'review':
      return handleReviewCommand();

    // ─── /backtest ───────────────────────────────────────────────────
    case 'backtest': {
      // Parse backtest-specific flags from slash command args
      const btArgs: Partial<ParsedArgs> = { subcommand: 'backtest' };
      for (let i = 0; i < args.length; i++) {
        const a = args[i];
        if (a === '--resolved') btArgs.resolved = true;
        else if (a === '--unresolved') btArgs.unresolved = true;
        else if (a === '--category') btArgs.category = args[++i];
        else if (a === '--days') { const v = Number(args[++i]); if (Number.isFinite(v) && v > 0) btArgs.days = v; }
        else if (a === '--max-age') { const v = Number(args[++i]); if (Number.isFinite(v) && v > 0) btArgs.maxAge = v; }
        else if (a === '--min-edge') { const v = Number(args[++i]?.replace('%', '')); if (Number.isFinite(v)) btArgs.minEdge = v / 100; }
        else if (a === '--min-volume') { const v = Number(args[++i]); if (Number.isFinite(v) && v >= 0) btArgs.minVolume = v; }
        else if (a === '--min-price') { const v = Number(args[++i]); if (Number.isFinite(v) && v >= 0 && v <= 100) btArgs.minPrice = v; }
        else if (a === '--max-price') { const v = Number(args[++i]); if (Number.isFinite(v) && v >= 0 && v <= 100) btArgs.maxPrice = v; }
        else if (a === '--export') { const v = args[++i]; if (v) btArgs.exportPath = v; }
        else if (a === '--universe') { const v = args[++i]; if (v === 'api' || v === 'local') btArgs.backtestUniverse = v; }
        else if (a === '--fees') { const v = args[++i]; if (v === 'none' || v === 'taker' || v === 'maker') btArgs.backtestFees = v; }
      }
      // Mirror parse-args' mutual-exclusion check — the slash parser above
      // accepts both flags independently, which would put btArgs in a
      // conflicting state before handleBacktest could see it.
      if (btArgs.resolved && btArgs.unresolved) {
        return { output: 'Error: --resolved and --unresolved cannot be used together.' };
      }
      const mode = btArgs.resolved ? 'resolved markets' : btArgs.unresolved ? 'open markets' : 'resolved + open markets';
      const daysLabel = btArgs.days ?? 15;
      return {
        output: `Running ${daysLabel}-day backtest on ${mode}...`,
        asyncFollowUp: async () => {
          const resp = await handleBacktest(defaultArgs(btArgs));
          if (!resp.ok || !resp.data) return resp.error?.message ?? 'Backtest failed';
          const text = formatBacktestHuman(resp.data, { minEdge: btArgs.minEdge ?? 0.005 });
          return btArgs.exportPath
            ? `${text}\n\nExported per-market detail to ${btArgs.exportPath}`
            : text;
        },
      };
    }

    // ─── Octagon Kalshi search/clusters/basket ───────────────────────
    case 'similar': {
      const parsed = parseArgs(['similar', ...args]);
      return {
        output: 'Querying Octagon for similar markets...',
        asyncFollowUp: async () => {
          const resp = await handleSimilar(parsed);
          return resp.ok ? formatSimilarHuman(resp.data) : (resp.error?.message ?? 'similar failed');
        },
      };
    }
    case 'clusters': {
      const parsed = parseArgs(['clusters', ...args]);
      return {
        output: 'Querying Octagon for clusters...',
        asyncFollowUp: async () => {
          const resp = await handleClusters(parsed);
          return resp.ok ? formatClustersHuman(resp.data) : (resp.error?.message ?? 'clusters failed');
        },
      };
    }
    case 'peers': {
      const parsed = parseArgs(['peers', ...args]);
      return {
        output: 'Querying Octagon for cluster peers...',
        asyncFollowUp: async () => {
          const resp = await handlePeers(parsed);
          return resp.ok ? formatPeersHuman(resp.data) : (resp.error?.message ?? 'peers failed');
        },
      };
    }
    case 'correlate': {
      const parsed = parseArgs(['correlate', ...args]);
      return {
        output: 'Computing correlation matrix...',
        asyncFollowUp: async () => {
          const resp = await handleCorrelate(parsed);
          return resp.ok ? formatCorrelationHuman(resp.data) : (resp.error?.message ?? 'correlate failed');
        },
      };
    }
    case 'basket': {
      const parsed = parseArgs(['basket', ...args]);
      const sub = parsed.positionalArgs[0] ?? '';
      return {
        output: `Running basket ${sub || '(no subcommand)'}...`,
        asyncFollowUp: async () => {
          const resp = await handleBasket(parsed);
          return resp.ok ? formatBasketHuman(resp.data) : (resp.error?.message ?? 'basket failed');
        },
      };
    }
    case 'events': {
      const parsed = parseArgs(['events', ...args]);
      return {
        output: 'Querying Octagon events...',
        asyncFollowUp: async () => {
          const resp = await handleEvents(parsed);
          return resp.ok ? formatEventsHuman(resp.data) : (resp.error?.message ?? 'events failed');
        },
      };
    }
    case 'trust': {
      const parsed = parseArgs(['trust', ...args]);
      return {
        output: 'Fetching Trust Index...',
        asyncFollowUp: async () => {
          const resp = await handleTrust(parsed);
          return resp.ok ? formatTrustHuman(resp.data) : (resp.error?.message ?? 'trust failed');
        },
      };
    }
    case 'report': {
      const parsed = parseArgs(['report', ...args]);
      // Reject unknown / malformed flags before kicking off the Octagon call
      // (network round-trip + 3 credits on --refresh). dispatch.ts does the
      // same for the CLI path; slash command path needs its own guard.
      if (parsed.parseErrors.length > 0) {
        return { output: parsed.parseErrors.join('; ') };
      }
      return {
        output: parsed.refresh ? 'Refreshing Octagon report...' : 'Fetching Octagon report...',
        asyncFollowUp: async () => {
          const resp = await handleReport(parsed);
          return resp.ok ? formatReportHuman(resp.data) : (resp.error?.message ?? 'report failed');
        },
      };
    }
    case 'series': {
      const parsed = parseArgs(['series', ...args]);
      const sub = parsed.positionalArgs[0]?.toLowerCase();
      return {
        output: sub === 'candles' ? 'Building series NAV...' : 'Rolling up Kalshi series...',
        asyncFollowUp: async () => {
          const resp = await handleSeries(parsed);
          return resp.ok ? formatSeriesHuman(resp.data) : (resp.error?.message ?? 'series failed');
        },
      };
    }
    case 'catalysts': {
      const parsed = parseArgs(['catalysts', ...args]);
      return {
        output: 'Loading upcoming catalysts...',
        asyncFollowUp: async () => {
          const resp = await handleCatalysts(parsed);
          return resp.ok ? formatCatalystsHuman(resp.data) : (resp.error?.message ?? 'catalysts failed');
        },
      };
    }

    case 'config':
      // Fall through to agent — better handled by the LLM
      return null;

    default:
      return null;
  }
}

export async function executePendingTrade(trade: NonNullable<CommandResult['pendingTrade']>): Promise<string> {
  let effectivePrice = trade.price;
  // When no price given, fetch best quote to simulate a market order
  if (effectivePrice === undefined) {
    const quoteResult = await fetchMarketQuote(trade.ticker, trade.action, trade.side);
    if ('error' in quoteResult) return quoteResult.error;
    effectivePrice = quoteResult.cents;
  }
  // effectivePrice is quoted on the chosen side; buildV2Order maps it onto
  // the V2 YES-side book (buy NO at p → sell YES at 1 - p).
  let data;
  try {
    data = await placeOrderV2(
    buildV2Order({
      ticker: trade.ticker,
      action: trade.action,
      side: trade.side,
      count: trade.count,
      priceCents: effectivePrice,
    })
    );
  } catch (err) {
    if (err instanceof MandateViolation) {
      trackEvent('trade_blocked', { action: trade.action, side: trade.side, reason: 'mandate' });
      return `⛔ ${err.message}`;
    }
    throw err;
  }
  trackEvent('trade_executed', { action: trade.action, side: trade.side, success: 'true' });
  // Auto-file a falsifiable hypothesis for the position: the side that
  // profits is the predicted settlement. Settlement sync resolves it.
  try {
    const predicted = winningSide(trade.action, trade.side);
    addHypothesis(getDb(), {
      claim: `Model-backed ${trade.action.toUpperCase()} ${trade.side.toUpperCase()} x${trade.count} on ${trade.ticker} at ${effectivePrice}¢ settles ${predicted.toUpperCase()}`,
      ticker: trade.ticker,
      predictedSide: predicted,
      source: 'trade',
    });
  } catch { /* registry is additive — never block execution */ }
  if (data.order_id) {
    const filled = parseFloat(String(data.fill_count ?? '0'));
    const remaining = parseFloat(String(data.remaining_count ?? '0'));
    return `Order placed. ID: ${data.order_id} | Filled: ${filled} | Resting: ${remaining}`;
  }
  return `Order submitted. Response: ${JSON.stringify(data)}`;
}

// ─── Portfolio subview handler ──────────────────────────────────────────────

async function handlePortfolioSlash(subview?: string): Promise<CommandResult> {
  const view = subview?.toLowerCase() ?? 'overview';

  try {
    if (view === 'positions') {
      const data = await callKalshiApi('GET', '/portfolio/positions');
      const allPositions = (data.market_positions ?? data.positions ?? []) as KalshiPosition[];
      const positions = allPositions.filter((p) => {
        const pos = parseFloat(String(p.position ?? '0'));
        return pos !== 0;
      });
      return { output: formatPositions(positions) };
    }

    if (view === 'settlements') {
      const db = getDb();
      const sync = await syncSettlements(db);
      const summary = summarizeSettlements(db);
      const recent = getSettlements(db, 15);
      const lines: string[] = [];
      lines.push('**Settlements (realized P&L)**');
      lines.push('');
      lines.push(`Synced: ${sync.new_settlements} new (${sync.fetched} fetched)${sync.positions_closed ? `, ${sync.positions_closed} local positions closed` : ''}${sync.complete ? '' : ' — INCOMPLETE: page cap hit, run again to continue'}`);
      lines.push(`Lifetime: ${summary.count} settlements · realized P&L ${summary.total_realized_pnl >= 0 ? '+' : ''}$${summary.total_realized_pnl.toFixed(2)} · fees $${summary.total_fees.toFixed(2)} · ${summary.wins}W/${summary.losses}L`);
      if (summary.with_model_view > 0) {
        lines.push(`Model-side outcome: ${summary.model_side_wins}/${summary.with_model_view} settlements went the model's way`);
      }
      if (recent.length > 0) {
        lines.push('');
        for (const r of recent) {
          const model = r.model_prob_entry !== null ? ` model=${(r.model_prob_entry * 100).toFixed(0)}%` : '';
          lines.push(`  ${r.settled_time.slice(0, 10)}  ${r.ticker}  ${r.market_result.toUpperCase()}  ${r.realized_pnl >= 0 ? '+' : ''}$${r.realized_pnl.toFixed(2)}${model}`);
        }
      } else {
        lines.push('');
        lines.push('No settlements recorded yet.');
      }
      return { output: lines.join('\n') };
    }

    if (view === 'orders') {
      const data = await callKalshiApi('GET', '/portfolio/orders', { params: { status: 'resting' } });
      const orders = (data.orders ?? []) as KalshiOrder[];
      return { output: formatOrders(orders) };
    }

    if (view === 'balance') {
      const data = await callKalshiApi('GET', '/portfolio/balance') as unknown as KalshiBalanceResponse;
      return { output: formatBalance(data) };
    }

    if (view === 'status') {
      const data = await callKalshiApi('GET', '/exchange/status');
      return { output: formatExchangeStatus(data) };
    }

    // Default: full portfolio overview
    const resp = await handlePortfolio(defaultArgs({ subcommand: 'portfolio' }));
    return { output: formatPortfolioHuman(resp.data) };
  } catch (err) {
    return { output: `Portfolio error: ${err instanceof Error ? err.message : String(err)}` };
  }
}

// ─── Analyze ────────────────────────────────────────────────────────────────

async function handleAnalyzeCommand(args: string[]): Promise<CommandResult> {
  const ticker = args[0];
  if (!ticker) return { output: 'Usage: /analyze <ticker> [refresh]  |  /analyze <t1> <t2> [t3...]' };
  // Multiple tickers → batch edge readout (one Octagon call for all).
  const nonFlag = args.filter((a) => !a.startsWith('--') && a.toLowerCase() !== 'refresh');
  if (nonFlag.length > 1) {
    return {
      output: `Batch-analyzing ${nonFlag.length} tickers...`,
      asyncFollowUp: async () => {
        const { handleAnalyzeBatch, formatAnalyzeBatchHuman } = await import('./analyze-batch.js');
        const resp = await handleAnalyzeBatch(nonFlag.map((t) => t.toUpperCase()));
        return resp.ok ? formatAnalyzeBatchHuman(resp.data) : (resp.error?.message ?? 'analyze (batch) failed');
      },
    };
  }
  const refresh = args[1]?.toLowerCase() === 'refresh';
  try {
    const data = await handleAnalyze(ticker.toUpperCase(), refresh);
    let output = formatAnalyzeHuman(data);
    // Reflection loop: surface lessons from settled positions in the same
    // series next to the fresh analysis, so realized errors inform the
    // next decision instead of being forgotten.
    let lessonTexts: string[] = [];
    try {
      const lessons = getLessonsForCategory(getDb(), categoryOf(data.eventTicker ?? '', ticker.toUpperCase()));
      lessonTexts = lessons.map((l) => l.lesson);
      const block = formatLessonsForContext(lessons);
      if (block) output += `\n\n${block}`;
    } catch { /* lessons are additive — never break analyze */ }

    // Provenance: an edge computed against a market-anchored model is not a
    // disagreement with the market. Say so before anyone acts on it.
    try {
      const note = formatIndependenceNote(
        data.edge !== null ? data.edge * 100 : null,
        (data as { provenance?: ModelProvenance }).provenance ?? null,
      );
      if (note) output += `\n\n${note}`;
    } catch { /* advisory only */ }

    // Adversarial bear-check: extreme edges get a skeptic pass appended.
    const edgePp = data.edge !== null && data.modelProb !== null ? data.edge * 100 : null;
    if (edgePp !== null && needsBearCheck(edgePp)) {
      return {
        output,
        asyncFollowUp: async () => {
          const llm = makeBearCheckLlm();
          const result = await runBearCheck({
            ticker: data.ticker,
            title: data.title,
            modelProb: data.modelProb!,
            marketProb: data.marketProb ?? 0.5,
            edgePp,
            keyDrivers: (data.drivers ?? []).slice(0, 5).map((d: { claim: string }) => d.claim),
            lessons: lessonTexts,
          }, llm);
          return formatBearCheck(result, edgePp);
        },
      };
    }
    return { output };
  } catch (err) {
    return { output: `Analyze failed: ${err instanceof Error ? err.message : String(err)}` };
  }
}

// ─── Trade command ──────────────────────────────────────────────────────────

function parseSide(val: string | undefined): 'yes' | 'no' | null {
  const v = val?.toLowerCase();
  if (v === 'yes' || v === 'y') return 'yes';
  if (v === 'no' || v === 'n') return 'no';
  return null;
}

function handleTradeCommand(action: 'buy' | 'sell', args: string[]): CommandResult {
  const [ticker, countStr, ...rest] = args;

  if (!ticker || !countStr) {
    return { output: `Usage: /${action} <ticker> <count> [price_in_cents] [yes|no]` };
  }

  // Extract side and price from remaining args: [price] [side], [side], or nothing
  let side: 'yes' | 'no' = 'yes';
  let priceArg: string | undefined;

  if (rest.length >= 2) {
    // e.g. /buy TICKER 10 50 no
    priceArg = rest[0];
    side = parseSide(rest[1]) ?? 'yes';
  } else if (rest.length === 1) {
    // Could be price or side: /buy TICKER 10 50  OR  /buy TICKER 10 no
    const asSide = parseSide(rest[0]);
    if (asSide) {
      side = asSide;
    } else {
      priceArg = rest[0];
    }
  }

  const validated = validateTradeArgs(countStr, priceArg);
  if ('error' in validated) {
    return { output: validated.error };
  }

  const pendingTrade = { ticker: ticker.toUpperCase(), action, side, count: validated.count, price: validated.price };

  return {
    output: formatOrderConfirmation(ticker.toUpperCase(), action, side, validated.count, validated.price),
    pendingTrade,
  };
}

async function handleReviewCommand(): Promise<CommandResult> {
  try {
    const reviews = await reviewPortfolio();
    return { output: formatReviewHuman(reviews) };
  } catch (err) {
    return { output: `Review failed: ${err instanceof Error ? err.message : String(err)}` };
  }
}

async function handleCancel(orderId: string | undefined): Promise<CommandResult> {
  if (!orderId) return { output: 'Usage: /cancel <order_id>' };

  try {
    const res = await cancelOrderV2(orderId);
    const reduced = res.reduced_by !== undefined ? ` (${res.reduced_by} contracts canceled)` : '';
    return { output: `Order ${orderId} canceled.${reduced}` };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const hint = msg.includes('404') ? ' (order not found or already filled)' : '';
    return { output: `Cancel failed: ${msg}${hint}` };
  }
}
