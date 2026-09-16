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
import { handleBacktest, formatBacktestHuman } from './backtest.js';
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
import { scanEdges, formatEdgeScanHuman } from './search-edge.js';
import type { KalshiBalanceResponse } from './formatters.js';
import { ExitCode, exitCodeFromError } from '../utils/errors.js';
import { trackEvent } from '../utils/telemetry.js';
import { handleSimilar, formatSimilarHuman, looksLikeTicker } from './similar.js';
import { handleClusters, formatClustersHuman } from './clusters.js';
import { handlePeers, formatPeersHuman } from './peers.js';
import { handleCorrelate, formatCorrelationHuman } from './correlate.js';
import { handleBasket, formatBasketHuman } from './basket.js';
import { getMarketsWithEdge, searchOctagonEvents, searchOctagonMarkets } from '../scan/octagon-kalshi-api.js';
import { formatMarketSearchHuman, formatMarketsWithEdgeHuman, formatEventSearchHuman, formatEventMarketsHuman } from './search-remote.js';
import { parseThemeQuery } from '../scan/theme-registry.js';
import { handleEvents, formatEventsHuman } from './events.js';
import { handleTrust, formatTrustHuman } from './trust.js';
import { handleReport, formatReportHuman } from './report.js';
import { handleOctagonChat } from './octagon-chat.js';
import { computeCalibration, formatCalibrationHuman } from '../eval/calibration.js';
import { generateMissingLessons, getRecentLessons } from '../eval/reflection.js';
import { makeReflectionLlm } from './index.js';
import { executeSlashCommand } from './index.js';
import { syncSettlements } from '../tools/kalshi/settle.js';
import { getDb } from '../db/index.js';
import { getBotSetting } from '../utils/bot-config.js';
import { buildV2Order, placeOrderV2, cancelOrderV2 } from '../tools/kalshi/orders-v2.js';
import { handleSeries, formatSeriesHuman } from './series.js';
import { handleEditorialThemes, formatEditorialThemesHuman } from './editorial-themes.js';
import { handleCatalysts, formatCatalystsHuman } from './catalysts.js';

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

    // `themes` is now the editorial-themes registry (curated narrative buckets).
    // Legacy "kalshi search themes" (Kalshi category labels) is still reachable
    // via `search themes`.

    // basket sub-routing (build/backtest/size/candles) — exposed for telemetry granularity
    case 'basket': {
      const sub = positionalArgs[0]?.toLowerCase();
      if (sub === 'build' || sub === 'backtest' || sub === 'size' || sub === 'candles') {
        return { canonical: 'basket', subview: sub };
      }
      return { canonical: 'basket' };
    }

    default:
      return { canonical: subcommand };
  }
}

function modeFlagsFor(canonical: Subcommand, args: ParsedArgs): Record<string, string | boolean> {
  switch (canonical) {
    case 'clusters':
      return { behavioral: args.behavioral, ranked: args.ranked };
    case 'peers':
      return { behavioral: args.behavioral, show_cluster: args.showCluster };
    case 'similar':
      return { anchor: args.ticker ? 'ticker' : args.query ? 'query' : 'positional' };
    case 'basket':
      return { kelly_sizing: args.bankroll !== undefined };
    case 'search':
      return { remote: !!process.env.OCTAGON_API_KEY };
    default:
      return {};
  }
}

/**
 * One-time stderr hint when a user pipes `--json` through `bunx` without
 * `--silent`. Bunx prints install chatter to stdout *before* our process even
 * starts, which corrupts JSON pipelines — `--silent` fixes it entirely, but
 * users rarely discover that flag on their own. We can't strip the chatter
 * (it's not in our stdout), but we can nudge them once.
 *
 * Heuristic: --json + non-TTY stdout + BUN_INSTALL_CACHE_DIR set (bunx sets
 * this; `bun add -g` installs don't). Silenced after first emit by touching
 * a sentinel file under ~/.kalshi-bot/.
 */
async function maybeEmitBunxHint(args: ParsedArgs): Promise<void> {
  if (!args.json) return;
  if (process.stdout.isTTY) return;
  if (!process.env.BUN_INSTALL_CACHE_DIR) return;
  try {
    // Dynamic ESM imports to avoid pulling these into the module graph at init.
    const { appPath } = await import('../utils/paths.js');
    const { existsSync, writeFileSync, mkdirSync } = await import('fs');
    const sentinel = appPath('.bunx-hint-shown');
    if (existsSync(sentinel)) return;
    process.stderr.write(
      '[kalshi] Tip: for clean JSON output and parallel-safe scripting, install once with\n' +
      '[kalshi]   bun add -g kalshi-trading-bot-cli\n' +
      '[kalshi] then call `kalshi …` directly. Or use `bunx --silent` to suppress install\n' +
      '[kalshi] chatter from this invocation. See README → Scripting & Parallel Use.\n',
    );
    const dir = appPath('.');
    mkdirSync(dir, { recursive: true });
    writeFileSync(sentinel, String(Date.now()));
  } catch {
    // Best-effort hint — never fail the actual command because of it.
  }
}

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

export async function dispatch(args: ParsedArgs): Promise<void> {
  // --days-to-close N is ergonomic sugar over --close-before <iso>. Resolve
  // it once here so every downstream command (search, events, series,
  // catalysts, basket --theme, similar) gets the same filter without each
  // handler reimplementing the arithmetic.
  if (args.daysToClose !== undefined && !args.closeBefore) {
    const target = new Date(Date.now() + args.daysToClose * MILLISECONDS_PER_DAY);
    args.closeBefore = target.toISOString();
  }

  const { subcommand, json } = args;
  const resolved = resolveAlias(subcommand, args.positionalArgs);
  await maybeEmitBunxHint(args);
  trackEvent('cli_command', {
    command: resolved.canonical,
    subview: resolved.subview ?? '',
    ...modeFlagsFor(resolved.canonical, args),
  });

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
      if (sub === 'edge') {
        const minEdgePp = (args.minEdge ?? 0.05) * 100;
        if (process.env.OCTAGON_API_KEY) {
          // edge_pp_min is asymmetric (only filters lower bound). Skip when
          // user passes --min-edge 0 so they see the full distribution.
          const data = await getMarketsWithEdge({
            category: args.category,
            ...(minEdgePp > 0 ? { edge_pp_min: minEdgePp } : {}),
            sort_by: (args.sortBy as 'edge_pp' | 'expected_return' | 'total_volume' | 'model_probability' | undefined) ?? 'edge_pp',
            limit: args.limit ?? 20,
          });
          if (json) {
            console.log(JSON.stringify(wrapSuccess('search', data)));
          } else {
            console.log(formatMarketsWithEdgeHuman(data, minEdgePp));
          }
          process.exit(ExitCode.SUCCESS);
          return;
        }
        // Local fallback: scan cached Octagon reports in SQLite
        const db = (await import('../db/index.js')).getDb();
        const result = scanEdges(db, { minEdgePp, limit: args.limit, category: args.category });
        if (json) {
          console.log(JSON.stringify(wrapSuccess('search', result)));
        } else {
          console.log(formatEdgeScanHuman(result, minEdgePp));
        }
        process.exit(ExitCode.SUCCESS);
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
      const query = args.positionalArgs.join(' ');

      // Octagon-powered server-side search: broader universe, full-text + structured filters.
      if (process.env.OCTAGON_API_KEY) {
        // --aggregate-by series → route to series rollup
        if (args.aggregateBy === 'series') {
          const { handleSeries, formatSeriesHuman } = await import('./series.js');
          const seriesArgs = { ...args, positionalArgs: query ? ['search', query] : ['list'] };
          const resp = await handleSeries(seriesArgs);
          if (json) {
            console.log(JSON.stringify(resp));
          } else if (resp.ok) {
            console.log(formatSeriesHuman(resp.data));
          } else {
            console.error(resp.error?.message ?? 'series rollup failed');
          }
          process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
          return;
        }
        // Market-level filters exist only on the markets route. The events
        // route answers an unrecognised param with zero rows rather than an
        // error, so any query carrying one stays on markets — which is also
        // what keeps --min-volume meaningful.
        const usesMarketFilters = Boolean(
          args.category || args.seriesTicker || args.seriesPrefix ||
          args.minVolume !== undefined || args.closeBefore || args.sortBy,
        );

        // An event identifier drills into that event's markets rather than
        // searching for the literal string. Ticker shape alone can't separate
        // an event from a market (KXNEWPOPE-70 vs KXNEWPOPE-70-PPIZ, while
        // KXELONMARS-99's only market IS KXELONMARS-99), so resolution decides:
        // ask for the event's markets and fall through if there are none.
        //
        // Errors deliberately propagate. The old bare catch here reported a 401,
        // a timeout and a malformed body all as "not an event ticker", then fell
        // through to a search that printed a clean zero-row table.
        if (!usesMarketFilters && query && looksLikeTicker(query)) {
          const drill = await searchOctagonMarkets({
            event_ticker: query.toUpperCase(),
            limit: args.limit ?? 30,
          });
          if (drill.data.length > 0) {
            if (json) {
              console.log(JSON.stringify(wrapSuccess('search', { kind: 'markets', ...drill })));
            } else {
              console.log(formatEventMarketsHuman(query.toUpperCase(), drill));
            }
            return;
          }
        }

        // A theme resolves to Octagon's cross-venue vocabulary. The value comes
        // from the registry, never from raw input: meta_category is
        // case-sensitive and a wrong case returns zero rows, not an error.
        // `theme:subtheme` ANDs the subtheme in as free text.
        const { theme, subtheme } = parseThemeQuery(query);
        if (!usesMarketFilters && theme) {
          const eventsPage = await searchOctagonEvents({
            meta_category: theme.metaCategory,
            // Autocomplete offers kebab-cased tags (oil-and-energy); q is full
            // text, so hyphens have to become spaces or it matches nothing.
            ...(subtheme ? { q: subtheme.replace(/-/g, ' ') } : {}),
            limit: args.limit ?? 30,
          });
          const describe = subtheme ? `theme ${theme.id}:${subtheme}` : `theme ${theme.id}`;
          if (json) {
            console.log(JSON.stringify(wrapSuccess('search', { kind: 'events', ...eventsPage })));
          } else {
            console.log(formatEventSearchHuman(describe, eventsPage));
          }
          return;
        }

        // Free text is event-first too — the subject lives on the event, not on
        // individual outcome rows. Markets remain the fallback when no event
        // matches, so nothing that used to be findable stops being findable.
        if (!usesMarketFilters && query) {
          const eventsPage = await searchOctagonEvents({ q: query, limit: args.limit ?? 30 });
          if (eventsPage.data.length > 0) {
            if (json) {
              console.log(JSON.stringify(wrapSuccess('search', { kind: 'events', ...eventsPage })));
            } else {
              console.log(formatEventSearchHuman(`"${query}"`, eventsPage));
            }
            return;
          }
        }

        // sort_by is now server-side (true top-N across the whole universe);
        // series_prefix lets us tree-browse (KXBTC matches all Bitcoin series).
        // This is the venue-agnostic route, same as the drill-down above — it
        // accepts every filter the Kalshi-only one did, so both CLIs now answer
        // market queries from one corpus.
        const serverSortBy = (args.sortBy === 'volume_24h' || args.sortBy === 'close_time' || args.sortBy === 'last_price')
          ? args.sortBy
          : undefined;
        const page = await searchOctagonMarkets({
          q: query,
          category: args.category,
          series_ticker: args.seriesTicker,
          series_prefix: args.seriesPrefix,
          min_volume_24h: args.minVolume,
          close_before: args.closeBefore,
          sort_by: serverSortBy,
          limit: args.limit ?? 30,
        });
        // --active-only is defensive — the live universe is active by default.
        const rows = args.activeOnly
          ? page.data.filter((m) => m.status === 'active' || m.status === 'open')
          : page.data;
        const filteredPage = { ...page, data: rows };
        if (json) {
          console.log(JSON.stringify(wrapSuccess('search', { kind: 'markets', ...filteredPage })));
        } else {
          console.log(formatMarketSearchHuman(query, filteredPage));
        }
        return;
      }

      // Local fallback: query the pre-built event index.
      if (args.refresh) {
        await forceRefreshIndex();
      } else {
        await ensureIndex();
      }
      const db = (await import('../db/index.js')).getDb();
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
      // Batch mode: 2+ positional tickers OR --tickers csv. Routes through
      // POST /kalshi/markets/edge in a single call (vs. N serial Octagon
      // round-trips). Use --refresh on a single ticker for the full deep
      // analysis pipeline.
      const csvTickers = args.tickers
        ? args.tickers.split(',').map((s) => s.trim()).filter(Boolean)
        : [];
      const tickerList = [...args.positionalArgs, ...csvTickers];
      if (tickerList.length > 1) {
        const { handleAnalyzeBatch, formatAnalyzeBatchHuman } = await import('./analyze-batch.js');
        const resp = await handleAnalyzeBatch(tickerList);
        if (json) {
          console.log(JSON.stringify(resp));
        } else if (resp.ok) {
          console.log(formatAnalyzeBatchHuman(resp.data));
        } else {
          console.error(resp.error?.message ?? 'analyze (batch) failed');
        }
        process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
        return;
      }
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

    // ─── similar (Octagon semantic search) ─────────────────────────────
    if (resolved.canonical === 'similar') {
      const resp = await handleSimilar(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatSimilarHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'similar failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── clusters (Octagon thematic & behavioral) ──────────────────────
    if (resolved.canonical === 'clusters') {
      const resp = await handleClusters(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatClustersHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'clusters failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── peers (Octagon cluster peers) ─────────────────────────────────
    if (resolved.canonical === 'peers') {
      const resp = await handlePeers(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatPeersHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'peers failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── correlate (Octagon correlation matrix) ────────────────────────
    if (resolved.canonical === 'correlate') {
      const resp = await handleCorrelate(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatCorrelationHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'correlate failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── catalysts (upcoming market closes grouped by week) ────────────
    if (resolved.canonical === 'catalysts') {
      const resp = await handleCatalysts(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatCatalystsHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'catalysts failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── themes (editorial narrative registry) ─────────────────────────
    if (resolved.canonical === 'themes') {
      const resp = await handleEditorialThemes(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatEditorialThemesHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'themes failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── series (Kalshi series rollup) ─────────────────────────────────
    if (resolved.canonical === 'series') {
      const resp = await handleSeries(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatSeriesHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'series failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── variants (strategy-variant leaderboard) ──────────────────────
    if (resolved.canonical === 'variants') {
      const { computeVariantLeaderboard, formatVariantLeaderboard } = await import('../backtest/variants.js');
      const resp = await handleBacktest({ ...args, subcommand: 'backtest' });
      if (!resp.ok) {
        console.error(resp.error?.message ?? 'variants failed');
        process.exit(ExitCode.USER_ERROR);
        return;
      }
      const minEdgePp = (args.minEdge ?? 0.005) * 100;
      const rows = computeVariantLeaderboard(resp.data.signals, minEdgePp);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('variants', { min_edge_pp: minEdgePp, rows })));
      } else {
        console.log(formatVariantLeaderboard(rows, minEdgePp));
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── daemon (background prefetch/settle maintenance loop) ─────────
    if (resolved.canonical === 'daemon') {
      const { runDaemonCycle, formatCycleSummary } = await import('../daemon/loop.js');
      const { makeReflectionLlm } = await import('./index.js');
      const db = getDb();
      const cfg = getBotSetting('daemon.interval_minutes');
      const intervalMin = typeof cfg === 'number' && Number.isFinite(cfg) && cfg > 0 ? cfg : 15;
      const once = args.positionalArgs[0]?.toLowerCase() === 'once';
      console.log(once
        ? 'Running one maintenance cycle...'
        : `Maintenance daemon started — cycle every ${intervalMin}m (Ctrl-C to stop).`);

      let stopped = false;
      process.once('SIGINT', () => { stopped = true; console.log('\nStopping after current cycle...'); });

      for (;;) {
        const cycle = await runDaemonCycle(db, { reflectionLlm: makeReflectionLlm() });
        console.log(formatCycleSummary(cycle));
        if (once || stopped) break;
        const wakeAt = Date.now() + intervalMin * 60_000;
        while (Date.now() < wakeAt && !stopped) {
          await new Promise((r) => setTimeout(r, 1_000));
        }
        if (stopped) break;
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── paper (forward-test ledger) ──────────────────────────────────
    if (resolved.canonical === 'paper') {
      const res = await executeSlashCommand('paper', args.positionalArgs);
      let out = res?.output ?? '';
      if (res?.asyncFollowUp) out = await res.asyncFollowUp();
      if (json) {
        console.log(JSON.stringify(wrapSuccess('paper', { output: out })));
      } else {
        console.log(out);
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── mandate / kill / resume (hard caps + instant halt) ───────────
    if (resolved.canonical === 'mandate' || resolved.canonical === 'kill' || resolved.canonical === 'resume') {
      const res = await executeSlashCommand(resolved.canonical, args.positionalArgs);
      if (json) {
        console.log(JSON.stringify(wrapSuccess(resolved.canonical, { output: res?.output ?? '' })));
      } else {
        console.log(res?.output ?? '');
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── hypothesis (falsifiable-claim registry) ──────────────────────
    if (resolved.canonical === 'hypothesis') {
      // Reuse the slash handler for identical semantics across surfaces.
      const res = await executeSlashCommand('hypothesis', args.positionalArgs);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('hypothesis', { output: res?.output ?? '' })));
      } else {
        console.log(res?.output ?? 'hypothesis: no output');
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── reflect (generate lessons from settled positions) ────────────
    if (resolved.canonical === 'reflect') {
      const db = getDb();
      await syncSettlements(db).catch(() => { /* offline */ });
      const result = await generateMissingLessons(db, { llm: makeReflectionLlm() });
      const recent = getRecentLessons(db, 10);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('reflect', { ...result, lessons: recent })));
      } else {
        console.log(result.generated > 0 ? `Generated ${result.generated} lessons (${result.source}).` : 'No settled positions awaiting reflection.');
        for (const l of recent) console.log(`  - [${l.settled_time.slice(0, 10)}] ${l.lesson}`);
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── calibration (realized-outcome Brier / skill report) ──────────
    if (resolved.canonical === 'calibration') {
      const db = getDb();
      await syncSettlements(db).catch(() => { /* offline: render from local ledger */ });
      const report = computeCalibration(db);
      if (json) {
        console.log(JSON.stringify(wrapSuccess('calibration', report)));
      } else {
        console.log(formatCalibrationHuman(report));
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    // ─── octagon (conversational Prediction Markets Agent) ────────────
    if (resolved.canonical === 'octagon') {
      const res = handleOctagonChat(args.positionalArgs);
      const answer = 'followUp' in res ? await res.followUp() : null;
      if (json) {
        // Single JSON envelope; the interim status line is human-only.
        console.log(JSON.stringify(wrapSuccess('octagon', { answer: answer ?? res.output })));
      } else if (answer !== null) {
        console.log(res.output);
        console.log(answer);
      } else {
        console.log(res.output);
      }
      process.exit(ExitCode.SUCCESS);
      return;
    }

    if (resolved.canonical === 'report') {
      const resp = await handleReport(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatReportHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'report failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── trust (Trader Trust scorecard) ────────────────────────────────
    if (resolved.canonical === 'trust') {
      const resp = await handleTrust(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatTrustHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'trust failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── events (Octagon events list / detail) ─────────────────────────
    if (resolved.canonical === 'events') {
      const resp = await handleEvents(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatEventsHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'events failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
      return;
    }

    // ─── basket (build, backtest, size, candles) ───────────────────────
    if (resolved.canonical === 'basket') {
      const resp = await handleBasket(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok) {
        console.log(formatBasketHuman(resp.data));
      } else {
        console.error(resp.error?.message ?? 'basket failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
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

    // ─── backtest ──────────────────────────────────────────────────────
    if (resolved.canonical === 'backtest') {
      const resp = await handleBacktest(args);
      if (json) {
        console.log(JSON.stringify(resp));
      } else if (resp.ok && resp.data) {
        console.log(formatBacktestHuman(resp.data, {
          minEdge: args.minEdge ?? 0.005,
        }));
      } else {
        console.error(resp.error?.message ?? 'Backtest failed');
      }
      process.exit(resp.ok ? ExitCode.SUCCESS : ExitCode.USER_ERROR);
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
      // V2 order path — V1 /portfolio/orders writes return 410 Gone, and the
      // mandate/kill-switch chokepoint lives inside placeOrderV2.
      const data = await placeOrderV2(
        buildV2Order({
          ticker: ticker.toUpperCase(),
          action: subcommand as 'buy' | 'sell',
          side: tradeSide,
          count: validated.count,
          priceCents: effectivePrice,
        })
      );
      if (json) {
        console.log(JSON.stringify(wrapSuccess(subcommand, data)));
      } else {
        const filled = parseFloat(String(data.fill_count ?? '0'));
        const remaining = parseFloat(String(data.remaining_count ?? '0'));
        console.log(data.order_id ? `Order placed. ID: ${data.order_id} | Filled: ${filled} | Resting: ${remaining}` : `Order submitted.`);
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
        await cancelOrderV2(orderId);
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
