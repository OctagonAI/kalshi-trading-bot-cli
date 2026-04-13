import { callKalshiApi } from '../tools/kalshi/api.js';
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
import { handleAnalyze, formatAnalyzeHuman } from './analyze.js';
import { handlePortfolio, formatPortfolioHuman } from './portfolio.js';
import { reviewPortfolio, formatReviewHuman } from './review.js';
import { buildHelp, validateTradeArgs } from './help.js';
import { fetchMarketQuote } from './helpers.js';
import { trackEvent } from '../utils/telemetry.js';

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
}

export async function handleSlashCommand(input: string): Promise<CommandResult | null> {
  const trimmed = input.trim();
  if (!trimmed.startsWith('/')) return null;

  const parts = trimmed.slice(1).trim().split(/\s+/);
  const command = parts[0]?.toLowerCase();
  const args = parts.slice(1);
  trackEvent('slash_command', { command: command ?? '' });

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

    // ─── Trading ─────────────────────────────────────────────────────
    case 'buy':
      return handleTradeCommand('buy', args);
    case 'sell':
      return handleTradeCommand('sell', args);
    case 'cancel':
      return handleCancel(args[0]);

    // ─── /search themes (inline) ─────────────────────────────────────
    // Note: /search <non-themes> is handled in cli.ts via browseController
    case 'themes': {
      const resp = await handleThemes({ subcommand: 'themes', positionalArgs: [], json: false, live: false, refresh: false, report: false, dryRun: false, verbose: false, performance: false, resolved: false, unresolved: false, snapshotLast: false, parseErrors: [] });
      return { output: formatThemesHuman(resp.data) };
    }

    // ─── /analyze ────────────────────────────────────────────────────
    case 'analyze':
      return handleAnalyzeCommand(args);

    // ─── /review ─────────────────────────────────────────────────────
    case 'review':
      return handleReviewCommand();

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
  const body: Record<string, unknown> = {
    ticker: trade.ticker,
    action: trade.action,
    side: trade.side,
    type: 'limit',
    count: trade.count,
    ...(trade.side === 'no'
      ? { no_price: effectivePrice }
      : { yes_price: effectivePrice }),
  };

  const data = await callKalshiApi('POST', '/portfolio/orders', { body });
  const order = data.order as Record<string, unknown> | undefined;
  trackEvent('trade_executed', { action: trade.action, side: trade.side, success: 'true' });
  if (order) {
    return `Order placed. ID: ${order.order_id} | Status: ${order.status}`;
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
    const resp = await handlePortfolio({
      subcommand: 'portfolio', positionalArgs: [], json: false,
      live: false, refresh: false, report: false, dryRun: false, verbose: false, performance: false,
      resolved: false, unresolved: false, snapshotLast: false, parseErrors: [],
    });
    return { output: formatPortfolioHuman(resp.data) };
  } catch (err) {
    return { output: `Portfolio error: ${err instanceof Error ? err.message : String(err)}` };
  }
}

// ─── Analyze ────────────────────────────────────────────────────────────────

async function handleAnalyzeCommand(args: string[]): Promise<CommandResult> {
  const ticker = args[0];
  if (!ticker) return { output: 'Usage: /analyze <ticker> [refresh]' };
  const refresh = args[1]?.toLowerCase() === 'refresh';
  try {
    const data = await handleAnalyze(ticker.toUpperCase(), refresh);
    return { output: formatAnalyzeHuman(data) };
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
    await callKalshiApi('DELETE', `/portfolio/orders/${orderId}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const hint = msg.includes('404') ? ' (order not found or already filled)' : '';
    return { output: `Cancel failed: ${msg}${hint}` };
  }
  return { output: `Order ${orderId} canceled.` };
}
