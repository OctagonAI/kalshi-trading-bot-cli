import type { ParsedArgs, Subcommand } from './parse-args.js';
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import { handleEdge, formatEdgeHuman } from './edge.js';
import { handleAnalyze, formatAnalyzeHuman, promptAnalyzeActions } from './analyze.js';
import { formatRawReport } from '../controllers/browse.js';
import { handlePortfolio, formatPortfolioHuman } from './portfolio.js';
import { handleConfig, formatConfigHuman } from './config.js';
import { handleAlerts, formatAlertsHuman } from './alerts.js';
import { handleStatus } from './status.js';
import { handleThemes, formatThemesHuman } from './themes.js';
import { handleWatch } from './watch.js';
import { callKalshiApi } from '../tools/kalshi/api.js';
import {
  formatBalance,
  formatPositions,
  formatOrders,
} from './formatters.js';
import type { KalshiOrder, KalshiPosition } from '../tools/kalshi/types.js';
import { buildHelp, validateTradeArgs } from './help.js';
import { fetchMarketQuote } from './helpers.js';
import { ensureIndex, forceRefreshIndex } from '../tools/kalshi/search-index.js';
import { searchEventIndex } from '../db/event-index.js';
import type { KalshiBalanceResponse } from './formatters.js';
import { ExitCode, exitCodeFromError } from '../utils/errors.js';
import { trackEvent } from '../utils/telemetry.js';

// ─── Alias resolution ────────────────────────────────────────────────────────
// Maps legacy CLI subcommands to canonical commands with mode/subview context

interface ResolvedCommand {
  canonical: Subcommand;
  mode?: string;
  subview?: string;
}

function resolveAlias(subcommand: Subcommand, positionalArgs: string[]): ResolvedCommand {
  switch (subcommand) {
    // Legacy analysis aliases → analyze
    case 'edge':
      return { canonical: 'edge', mode: 'edge-only' };
    // Legacy account aliases → portfolio
    case 'status':
      return { canonical: 'portfolio', subview: 'status' };

    // themes → search themes
    case 'themes':
      return { canonical: 'search', subview: 'themes' };

    default:
      return { canonical: subcommand };
  }
}

export async function dispatch(args: ParsedArgs): Promise<void> {
  const { subcommand, json } = args;
  const resolved = resolveAlias(subcommand, args.positionalArgs);
  trackEvent('cli_command', { command: resolved.canonical, subview: resolved.subview ?? '' });

  try {
    // ─── reject invalid flags early (for all commands) ───────────────
    if (args.parseErrors.length > 0) {
      const msg = args.parseErrors.join('; ');
      if (json) {
        console.log(JSON.stringify(wrapError(subcommand, 'INVALID_ARGS', msg)));
        process.exit(ExitCode.USER_ERROR);
      } else {
        console.error(msg);
        process.exit(ExitCode.USER_ERROR);
      }
      return;
    }

    // ─── search ────────────────────────────────────────────────────────
    if (resolved.canonical === 'search') {
      const sub = resolved.subview ?? args.positionalArgs[0];
      if (sub === 'themes' || resolved.subview === 'themes') {
        const resp = await handleThemes(args);
        if (json) {
          console.log(JSON.stringify(resp));
        } else {
          console.log(formatThemesHuman(resp.data));
        }
        process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
        return;
      }
      if (!sub) {
        // No query provided — show themes as a starting point
        const resp = await handleThemes(args);
        if (json) {
          console.log(JSON.stringify(resp));
        } else {
          console.log(formatThemesHuman(resp.data));
        }
        process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
        return;
      }
      // General search: query the local event index
      if (args.refresh) {
        await forceRefreshIndex();
      } else {
        await ensureIndex();
      }
      const db = (await import('../db/index.js')).getDb();
      const query = args.positionalArgs.join(' ');
      const results = searchEventIndex(db, query, 30);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('search', { events: results })));
      } else {
        if (results.length === 0) {
          console.log(`No events found for "${query}".`);
        } else {
          console.log(`Found ${results.length} event(s) for "${query}":\n`);
          for (const ev of results) {
            const markets = ev.markets_json ? JSON.parse(ev.markets_json) : [];
            const openMarkets = markets.filter((m: any) => m.status === 'open' || m.status === 'active');
            console.log(`  ${ev.event_ticker}  ${ev.title}  (${openMarkets.length} market${openMarkets.length !== 1 ? 's' : ''})`);
          }
        }
      }
      return;
    }

    // ─── portfolio (with subviews) ─────────────────────────────────────
    if (resolved.canonical === 'portfolio') {
      const subview = resolved.subview ?? args.positionalArgs[0] ?? 'overview';

      if (subview === 'positions') {
        const data = await callKalshiApi('GET', '/portfolio/positions');
        const allPositions = (data.market_positions ?? data.positions ?? []) as KalshiPosition[];
        const positions = allPositions.filter((p) => {
          const pos = parseFloat(String(p.position ?? '0'));
          return pos !== 0;
        });
        if (json) {
          console.log(JSON.stringify(wrapSuccess('portfolio:positions', { positions })));
        } else {
          console.log(formatPositions(positions));
        }
        return;
      }

      if (subview === 'orders') {
        const data = await callKalshiApi('GET', '/portfolio/orders', { params: { status: 'resting' } });
        const orders = (data.orders ?? []) as KalshiOrder[];
        if (json) {
          console.log(JSON.stringify(wrapSuccess('portfolio:orders', { orders })));
        } else {
          console.log(formatOrders(orders));
        }
        return;
      }

      if (subview === 'balance') {
        const data = await callKalshiApi('GET', '/portfolio/balance') as unknown as KalshiBalanceResponse;
        if (json) {
          console.log(JSON.stringify(wrapSuccess('portfolio:balance', data)));
        } else {
          console.log(formatBalance(data));
        }
        return;
      }

      if (subview === 'status') {
        const output = await handleStatus();
        if (json) {
          console.log(JSON.stringify({ ok: true, output }));
        } else {
          console.log(output);
        }
        return;
      }

      // Default: full portfolio overview
      const resp = await handlePortfolio(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else {
        console.log(formatPortfolioHuman(resp.data));
        const warnings = (resp.meta as Record<string, unknown>)?.warnings;
        if (Array.isArray(warnings) && warnings.length > 0) {
          for (const w of warnings) console.error(`  ⚠ ${String(w)}`);
        }
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── analyze ───────────────────────────────────────────────────────
    if (resolved.canonical === 'analyze') {
      const ticker = args.positionalArgs[0];
      if (!ticker) {
        const errResp = wrapError('analyze', 'MISSING_TICKER', 'Usage: analyze <ticker> [--refresh] [--report]');
        if (json) {
          console.log(JSON.stringify(errResp));
          process.exit(ExitCode.USER_ERROR);
        } else {
          console.error('Usage: analyze <ticker> [--refresh] [--report]');
          process.exit(ExitCode.USER_ERROR);
        }
        return;
      }
      const refresh = args.refresh;
      const data = await handleAnalyze(ticker, refresh);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('analyze', data)));
      } else {
        console.log(formatAnalyzeHuman(data));
        if (args.report && data.rawReport) {
          console.log('\n' + formatRawReport(data.rawReport, ticker));
        }
        await promptAnalyzeActions(data);
      }
      return;
    }

    // ─── watch ─────────────────────────────────────────────────────────
    if (resolved.canonical === 'watch') {
      // Force index rebuild before watching if --refresh is set
      if (args.refresh) {
        await forceRefreshIndex();
      }
      // Per-ticker mode if a positional arg is given and no --theme
      const ticker = args.positionalArgs[0];
      if (ticker && !args.theme) {
        const { handleWatchTicker } = await import('./watch.js');
        await handleWatchTicker(ticker.toUpperCase(), args);
        return;
      }
      // Theme scan mode (existing behavior)
      await handleWatch(args);
      return;
    }

    // ─── buy / sell ────────────────────────────────────────────────────
    if (subcommand === 'buy' || subcommand === 'sell') {
      const [ticker, countStr, priceStr] = args.positionalArgs;
      if (!ticker || !countStr) {
        const usage = `Usage: ${subcommand} <ticker> <count> [price_in_cents] [--side yes|no]`;
        const errResp = wrapError(subcommand, 'MISSING_ARGS', usage);
        if (json) {
          console.log(JSON.stringify(errResp));
          process.exit(ExitCode.USER_ERROR);
        } else {
          console.error(usage);
          process.exit(ExitCode.USER_ERROR);
        }
        return;
      }
      const validated = validateTradeArgs(countStr, priceStr);
      if ('error' in validated) {
        const errResp = wrapError(subcommand, 'INVALID_ARGS', validated.error);
        if (json) {
          console.log(JSON.stringify(errResp));
          process.exit(ExitCode.USER_ERROR);
        } else {
          console.error(validated.error);
          process.exit(ExitCode.USER_ERROR);
        }
        return;
      }
      let effectivePrice = validated.price;
      // When no price given, fetch best quote to simulate a market order
      // (Kalshi API requires a price field even for market-like orders)
      const tradeSide = args.side ?? 'yes';
      if (effectivePrice === undefined) {
        const quoteResult = await fetchMarketQuote(ticker.toUpperCase(), subcommand as 'buy' | 'sell', tradeSide);
        if ('error' in quoteResult) {
          if (json) {
            console.log(JSON.stringify(wrapError(subcommand, 'NO_QUOTE', quoteResult.error)));
            process.exit(ExitCode.EXTERNAL_ERROR);
          } else {
            console.error(quoteResult.error);
            process.exit(ExitCode.EXTERNAL_ERROR);
          }
          return;
        }
        effectivePrice = quoteResult.cents;
      }
      const body: Record<string, unknown> = {
        ticker: ticker.toUpperCase(),
        action: subcommand,
        side: tradeSide,
        type: 'limit',
        count: validated.count,
        ...(tradeSide === 'no'
          ? { no_price: effectivePrice }
          : { yes_price: effectivePrice }),
      };
      const data = await callKalshiApi('POST', '/portfolio/orders', { body });
      if (json) {
        console.log(JSON.stringify(wrapSuccess(subcommand, data)));
      } else {
        const order = data.order as Record<string, unknown> | undefined;
        console.log(order ? `Order placed. ID: ${order.order_id} | Status: ${order.status}` : `Order submitted.`);
      }
      return;
    }

    // ─── cancel ────────────────────────────────────────────────────────
    if (subcommand === 'cancel') {
      const orderId = args.positionalArgs[0];
      if (!orderId) {
        const errResp = wrapError('cancel', 'MISSING_ARGS', 'Usage: cancel <order_id>');
        if (json) {
          console.log(JSON.stringify(errResp));
          process.exit(ExitCode.USER_ERROR);
        } else {
          console.error('Usage: cancel <order_id>');
          process.exit(ExitCode.USER_ERROR);
        }
        return;
      }
      try {
        await callKalshiApi('DELETE', `/portfolio/orders/${orderId}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const hint = msg.includes('404') ? ' (order not found or already filled)' : '';
        const code = exitCodeFromError(err);
        if (json) {
          console.log(JSON.stringify(wrapError('cancel', 'CANCEL_FAILED', msg + hint)));
          process.exit(code);
        } else {
          console.error(`Cancel failed: ${msg}${hint}`);
          process.exit(code);
        }
        return;
      }
      if (json) {
        console.log(JSON.stringify(wrapSuccess('cancel', { orderId, canceled: true })));
      } else {
        console.log(`Order ${orderId} canceled.`);
      }
      return;
    }

    // ─── help ──────────────────────────────────────────────────────────
    if (subcommand === 'help') {
      const topic = args.positionalArgs[0];
      const result = buildHelp('cli', topic);
      if ('error' in result) {
        const errResp = wrapError('help', 'UNKNOWN_TOPIC', result.error);
        if (json) {
          console.log(JSON.stringify(errResp));
          process.exit(ExitCode.USER_ERROR);
        } else {
          console.error(result.error);
          process.exit(ExitCode.USER_ERROR);
        }
        return;
      }
      if (json) {
        console.log(JSON.stringify(wrapSuccess('help', { text: result.text })));
      } else {
        console.log(result.text);
      }
      return;
    }

    // ─── Legacy commands (kept for backward compat) ────────────────────

    // Edge command
    if (subcommand === 'edge') {
      const resp = await handleEdge(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else {
        console.log(formatEdgeHuman(resp.data));
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // Config command
    if (subcommand === 'config') {
      const resp = await handleConfig(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (!resp.ok) {
        const errMsg = (resp as { error?: { message?: string } }).error?.message ?? 'Config error';
        console.error(errMsg);
      } else {
        console.log(formatConfigHuman(resp.data));
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // Clear cache command
    if (subcommand === 'clear-cache') {
      const { handleClearCache } = await import('./clear-cache.js');
      const result = handleClearCache();
      if (json) {
        console.log(JSON.stringify(wrapSuccess('clear-cache', result)));
      } else {
        console.log(result.message);
      }
      return;
    }

    // Alerts command
    if (subcommand === 'alerts') {
      const resp = await handleAlerts(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else {
        console.log(formatAlertsHuman(resp.data));
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // Unknown command
    const resp = wrapError(subcommand, 'UNKNOWN_COMMAND', `Unknown command: ${subcommand}`);
    if (json) {
      console.log(JSON.stringify(resp));
      process.exit(ExitCode.USER_ERROR);
    } else {
      console.error(`Error: unknown command "${subcommand}"`);
      process.exit(ExitCode.USER_ERROR);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const code = exitCodeFromError(err);
    const errorCode = code === ExitCode.AUTH_ERROR
      ? 'AUTH_ERROR'
      : code === ExitCode.EXTERNAL_ERROR
        ? 'EXTERNAL_ERROR'
        : code === ExitCode.USER_ERROR
          ? 'USER_ERROR'
          : 'INTERNAL_ERROR';
    const resp = wrapError(subcommand, errorCode, message);
    trackEvent('error_occurred', { command: subcommand, error_code: errorCode });

    if (json) {
      console.log(JSON.stringify(resp));
      process.exit(code);
    } else {
      console.error(`Error running "${subcommand}": ${message}`);
      process.exit(code);
    }
  }
}
