// ─── Shared help content for both TUI slash commands and CLI batch mode ─────

/** Context determines prefix style: slash commands use "/", CLI uses "kalshi" */
type HelpContext = 'slash' | 'cli';

function prefix(ctx: HelpContext): string {
  return ctx === 'slash' ? '/' : 'kalshi ';
}

function buildTopics(ctx: HelpContext): Record<string, string> {
  const p = prefix(ctx);
  return {
    search: `**${p}search** — Discovery (Octagon-powered when OCTAGON_API_KEY is set)

${p}search [theme|ticker|query]  Full-text market search (server-side when key is set, else local index)
${p}search themes                List all available themes and subcategories
${p}search edge                  Edge ranking from latest Octagon run (server-side) or local cache
${p}search edge --min-edge 30    Markets with ≥30pp edge
${p}search edge --limit 50       Top 50 results
${p}search edge --category crypto Filter by category
${p}search edge --sort-by total_volume  Sort: edge_pp | expected_return | total_volume | model_probability

Search flags (server-side path):
  --category <name>     Filter by category
  --series <ticker>     Filter to a series
  --min-volume <n>      Floor on 24h volume
  --close-before <iso>  Only markets closing before
  --limit <n>           Page size (default 30)

Examples:
  ${p}search crypto
  ${p}search "bitcoin price" --min-volume 10000
  ${p}search edge --min-edge 30 --category crypto

Tip: ${p}similar gives semantic match (catches "Bitcoin pierce six figures" ↔ "BTC > $100k").`,

    portfolio: `**${p}portfolio** — Account state

${p}portfolio                    Full overview: positions, P&L, risk snapshot
${p}portfolio positions          Open positions with P&L
${p}portfolio orders             Resting orders
${p}portfolio balance            Account balance
${p}portfolio status             Exchange status${ctx === 'cli' ? ' and setup verification' : ''}
${ctx === 'cli' ? `
Flags:
  --performance                     Include win rate, Sharpe, Brier scores
  --json                            JSON output` : ''}`,

    analyze: `**${p}analyze** — Deep market analysis

${p}analyze <ticker>             Full analysis: edge, drivers, catalysts, Kelly sizing
${p}analyze <ticker> ${ctx === 'cli' ? '--' : ''}refresh     Force fresh Octagon report
${ctx === 'cli' ? `
Legacy aliases (still work):
  ${p}edge [--ticker X]          Edge history / snapshots (default: last 24h)
  ${p}edge --since <date>        Edges since date (e.g. 2026-03-01)` : ''}`,

    watch: `**${p}watch** — Live monitoring

Modes:
  ${p}watch <ticker>               Per-ticker price/orderbook feed (5s default)
  ${p}watch --theme <theme>        Continuous theme scan${ctx === 'cli' ? ' (default: every 60m)' : ' (press Esc to stop)'}
${ctx === 'cli' ? `
Flags:
  --interval <minutes>              Scan interval for theme mode (min 15)
  --live                            Force 15m interval
  --json                            NDJSON output (one line per tick/cycle)
  --dry-run                         Scan without persisting edges

Press Ctrl+C to stop.` : `
Per-ticker mode shows live price, bid/ask, spread, volume, and top-5 orderbook.
Theme mode runs recurring Octagon scans and displays an edge table.`}`,

    buy: `**${p}buy** — Buy contracts

${p}buy <ticker> <count> [price${ctx === 'cli' ? '_in_cents' : ''}] [yes|no]${ctx === 'slash' ? '   Buy contracts (price in cents)' : ''}

Example${ctx === 'cli' ? 's' : ''}:
  ${p}buy KXBTC-26MAR14-T50049 10 ${ctx === 'cli' ? '          Buy at best ask (10 YES contracts)' : '56'}
  ${p}buy KXBTC-26MAR14-T50049 10 ${ctx === 'cli' ? '56        Limit order at $0.56' : '56 no   Buy NO contracts'}
${ctx === 'cli' ? `  ${p}buy KXBTC-26MAR14-T50049 10 56 no   Limit order for NO contracts at $0.56` : ''}
Side defaults to YES if omitted.`,

    sell: `**${p}sell** — Sell contracts

${p}sell <ticker> <count> [price${ctx === 'cli' ? '_in_cents' : ''}] [yes|no]${ctx === 'slash' ? '  Sell contracts (price in cents)' : ''}

Example${ctx === 'cli' ? 's' : ''}:
  ${p}sell KXBTC-26MAR14-T50049 10 ${ctx === 'cli' ? '         Sell at best ask (10 YES contracts)' : '72'}
  ${p}sell KXBTC-26MAR14-T50049 10 ${ctx === 'cli' ? '72       Limit order at $0.72' : '72 no   Sell NO contracts'}
${ctx === 'cli' ? `  ${p}sell KXBTC-26MAR14-T50049 10 72 no  Limit order for NO contracts at $0.72` : ''}
Side defaults to YES if omitted.`,

    cancel: `**${p}cancel** — Cancel a resting order

${p}cancel <order_id>`,

    backtest: `**${p}backtest** — Model accuracy scorecard & edge scanner

${p}backtest                              15-day lookback, both sections (default)
${p}backtest --days 30                    30-day lookback
${p}backtest --max-age 14                 Reject predictions older than 14 days (default = --days)
${p}backtest --resolved                   Resolved markets only
${p}backtest --unresolved                 Unresolved markets only
${p}backtest --category crypto            Filter by category
${p}backtest --min-edge 10                Stricter edge threshold in pp (default 0.5pp)
${p}backtest --min-volume 10              Per-contract volume gate (default 1)
${p}backtest --min-price 5 --max-price 95 Tradeable price band 0-100 (defaults: 5 / 95)
${p}backtest --export results.csv         Per-market detail CSV
${p}backtest --json                       Machine-readable output

Looks back N days, compares what the model said then to where the market is now.
Resolved markets: scored against Kalshi settlement (0 or 100).
Unresolved markets: mark-to-market vs current Kalshi trading price.
Per-contract entry: mp/kp come from the per-contract outcome_probabilities on the
Octagon snapshot (no event-level fallback). Volume gate uses per-contract volume
from the snapshot when available, else current Kalshi lifetime volume.
ROI is capital-weighted: sum(pnl) / sum(capital) across edge signals, where capital
is kp/100 for YES edges and (100-kp)/100 for NO edges (matches Supabase methodology).`,

    'clear-cache': `**${ctx === 'cli' ? '' : 'kalshi '}clear-cache** — Delete local cache

${ctx === 'cli' ? `${p}` : 'kalshi '}clear-cache                Delete the local SQLite database (~/.kalshi-bot/kalshi-bot.db)
                               A fresh database will be created on next command.

Use this when the local cache is corrupted or you want to start fresh.${ctx !== 'cli' ? '\nRun from terminal: kalshi clear-cache' : ''}`,

    init: `**${p}init** — Re-run setup wizard

${p}init                       Launch the TUI with the setup wizard open
                               Use this to configure or reconfigure API keys and preferences.`,

    help: `**${p}help** — Show help

${p}help                       Show all commands
${p}help <command>             Show detailed help for a command`,

    similar: `**${p}similar** — Semantic market search (Octagon-powered)

${p}similar <ticker>                  Markets near this ticker by embedding distance
${p}similar -q "free-text query"      Markets matching free-text intent (server-side embed)
${p}similar <ticker> --top-k 25       Return top-25 nearest neighbors
${p}similar -q "..." --category crypto --min-volume 10000 --close-before 2026-08-19T00:00:00Z

Flags:
  --top-k <n>             Number of neighbors (default 25, max 100)
  --category <name>       Restrict to a Kalshi category
  --min-volume <n>        Floor on 24h volume
  --close-before <iso>    Only markets closing before this timestamp
  --json                  JSON output

Catches matches keyword search misses — "Will Bitcoin pierce six figures" ↔ "BTC over $100k".`,

    clusters: `**${p}clusters** — Browse Octagon clusters (thematic + behavioral)

${p}clusters                              List thematic clusters with sample titles
${p}clusters --label fed                  Filter by label substring
${p}clusters <id>                         Show markets in cluster
${p}clusters --behavioral                 List behavioral clusters (mean return + vol)
${p}clusters <id> --behavioral            Members of a behavioral cluster
${p}clusters --ranked                     Rank clusters by historical basket return
${p}clusters --ranked --timeframe 1y --min-return 0.20 --top-k 5

Flags:
  --behavioral            Use behavioral clustering (30-day return vectors)
  --label <substring>     Case-insensitive label filter
  --ranked                Score clusters by equal-weight basket return
  --timeframe <1w|1m|3m|6m|1y>   Window for --ranked (default 1y)
  --min-return <n>        Minimum total_return to include (e.g. 0.20)
  --top-k <n>             Basket size per cluster for --ranked (default 5)
  --limit <n>             Max clusters to evaluate
  --json                  JSON output`,

    peers: `**${p}peers** — Find markets in the same cluster as a ticker

${p}peers <ticker>                        Thematic cluster peers (default)
${p}peers <ticker> --behavioral           Behavioral cluster peers
${p}peers <ticker> --limit 50             Up to 50 peers (excluding anchor)
${p}peers <ticker> --show-cluster         Just show which clusters this ticker belongs to

Flags:
  --behavioral            Use behavioral clusters instead of thematic
  --limit <n>             Number of peers to return (default 50)
  --show-cluster          Print cluster membership only (no peer list)
  --json                  JSON output`,

    correlate: `**${p}correlate** — Pairwise correlation matrix over close-price candles

${p}correlate <ticker1> <ticker2> [...]   Pearson correlation across 2-100 tickers
${p}correlate --tickers KX-A,KX-B,KX-C    Same, comma-separated
${p}correlate KX-A KX-B --window-days 90  90-day lookback
${p}correlate KX-A KX-B --correlation-interval 1d   Force daily candles

Flags:
  --window-days <n>             Lookback (1-730, default 30; auto interval picks 1d if >=90)
  --correlation-interval <1h|1d>  Override bin size
  --tickers <csv>               Alternative to positional args
  --json                        JSON output (matrix + ranked_pairs)

Output ranks pairs ascending by correlation — most-uncorrelated first.`,

    basket: `**${p}basket** — Build, backtest, and size diversified baskets

${p}basket build [universe filters] [-n N] [--max-per-cluster M] [--max-corr X] [--bankroll $ --kelly K --probs ...]
${p}basket backtest --tickers KX-A,KX-B --weights 0.6,0.4 --timeframe 1y
${p}basket candles  --tickers KX-A,KX-B --timeframe 6m
${p}basket size     --bankroll 1000 --kelly 0.25 --probs KX-A:0.62,KX-B:0.55 [--side yes|no]

Build flags (universe + diversification):
  --category <name>             Restrict candidate pool by category
  --series <ticker>             Restrict to a series
  --min-volume <n>              Volume floor for candidates
  --close-before <iso>          Only markets closing before
  --label <csv>                 Restrict cluster labels (substring match, comma-separated)
  -q "<text>"                   Anchor candidate pool by free-text intent (semantic)
  --ticker <ticker>             Anchor candidate pool by ticker (semantic)
  -n <n>                        Number of legs (1-20)
  --max-per-cluster <n>         Cap legs per thematic cluster
  --max-corr <x>                Pairwise correlation cap (-1 to 1)
  --limit <n>                   Candidate pool size (2-200)
  --window-days <n>             Correlation window (7-365)

Sizing flags (build & size):
  --bankroll <usd>              Required for Kelly sizing
  --kelly <fraction>            Kelly multiplier 0-1 (default 0.25)
  --probs KX-A:0.62,KX-B:0.55   Model probabilities per ticker
  --side <yes|no>               Default leg side for "basket size" (default yes)

Backtest/candles flags:
  --tickers <csv>               Tickers (required)
  --weights <csv>               Optional weights, same length as tickers
  --timeframe <1w|1m|3m|6m|1y>  Window/bin size

Common:
  --json                        JSON output

Recipes:
  ${p}basket build --category crypto --min-volume 10000 -n 8 --max-per-cluster 2 --max-corr 0.6
  ${p}basket build --label fed,cpi,fomc,gdp,jobs -n 5 --max-per-cluster 1 --max-corr 0.4
  ${p}basket backtest --tickers KX-A,KX-B,KX-C --weights 0.4,0.4,0.2 --timeframe 1y`,
  };
}

function buildOverview(ctx: HelpContext): string {
  const p = prefix(ctx);
  if (ctx === 'cli') {
    return `**Kalshi Trading Bot CLI — CLI Commands**

Quick start:
  kalshi search crypto          Find markets by keyword or theme
  kalshi analyze <ticker>       Deep analysis + trade recommendation
  kalshi watch --theme crypto   Continuous scan across a theme

Discovery:
  search [theme|ticker|query]   Find markets (Octagon when key set, else local)
  search --refresh <query>      Force local index rebuild then search
  search themes                 List all themes and subcategories
  search edge [--min-edge N]    Edge ranking (Octagon when key set, else local)
  similar <ticker>              Semantic neighbors (embedding distance)
  similar -q "free text"        Semantic search by natural-language query
  clusters [--label X]          Browse thematic clusters
  clusters <id>                 List markets in a cluster
  clusters --behavioral         Behavioral clusters (30-day return vectors)
  clusters --ranked             Rank clusters by historical basket return
  peers <ticker>                Find markets in the same cluster
  watch <ticker>                Live price/orderbook feed
  watch --theme <theme>         Continuous theme scan (Ctrl+C to stop)
  watch --refresh               Force index rebuild before watching

Portfolio construction:
  correlate <t1> <t2> [...]     Pairwise Pearson correlation matrix
  basket build [filters] -n N   Diversified basket with cluster + correlation caps
  basket backtest --tickers ... NAV summary with Sharpe, max DD, win rate
  basket size --bankroll $ --probs ...   Fractional Kelly sizing for picked legs
  basket candles --tickers ...  OHLC bars for a weighted basket NAV

Analysis & Trading:
  analyze <ticker>              Full report: edge, drivers, Kelly sizing
  analyze <ticker> --refresh    Force fresh Octagon report
  buy <ticker> <n> [price] [yes|no]   Buy contracts (price in cents)
  sell <ticker> <n> [price] [yes|no]  Sell contracts
  cancel <order_id>                   Cancel a resting order

Analysis:
  backtest                      Model accuracy scorecard + live edge scanner
  backtest --resolved           Resolved markets scorecard only
  backtest --unresolved         Live edge scanner only

Account:
  portfolio                     Overview: positions, P&L, risk snapshot
  portfolio positions           Open positions
  portfolio orders              Resting orders
  portfolio balance             Account balance

System:
  init                          Launch with setup wizard (configure API keys)
  clear-cache                   Delete local SQLite cache and start fresh
  setup                         Re-run setup wizard
  help [command]                Show help for a command

Flags: --json, --refresh, --performance, --dry-run, --verbose
Backtest flags: --days, --max-age, --resolved, --unresolved, --category, --min-edge,
                --min-volume, --min-price, --max-price, --export
Run "kalshi help <command>" for detailed usage.`;
  }

  return `**Kalshi Trading Bot CLI — Commands**

Quick start:
  /search crypto          Find markets by keyword or theme
  /analyze <ticker>       Deep analysis + trade recommendation
  /watch --theme crypto   Continuous scan across a theme

Discovery:
  /search [theme|ticker|query]   Find markets (Octagon when key set, else local)
  /search --refresh <query>      Force local index rebuild then search
  /search themes                 List all themes and subcategories
  /search edge [--min-edge N]    Edge ranking (Octagon when key set, else local)
  /similar <ticker>              Semantic neighbors (embedding distance)
  /similar -q "free text"        Semantic search by natural-language query
  /clusters [--label X]          Browse thematic clusters
  /clusters <id>                 List markets in a cluster
  /clusters --behavioral         Behavioral clusters (30-day return vectors)
  /clusters --ranked             Rank clusters by historical basket return
  /peers <ticker>                Find markets in the same cluster
  /watch <ticker>                Live price/orderbook feed
  /watch --theme <theme>         Continuous theme scan (Esc to stop)
  /watch --refresh               Force index rebuild before watching

Portfolio construction:
  /correlate <t1> <t2> [...]     Pairwise Pearson correlation matrix
  /basket build [filters] -n N   Diversified basket with cluster + correlation caps
  /basket backtest --tickers ... NAV summary with Sharpe, max DD, win rate
  /basket size --bankroll $ --probs ...   Fractional Kelly sizing for picked legs
  /basket candles --tickers ...  OHLC bars for a weighted basket NAV

Analysis:
  /backtest                      Model accuracy scorecard + live edge scanner
  /analyze <ticker>              Full report: edge, drivers, Kelly sizing
  /analyze <ticker> refresh      Force fresh Octagon report
  /buy <ticker> <n> [price] [yes|no]   Buy contracts (price in cents)
  /sell <ticker> <n> [price] [yes|no]  Sell contracts
  /review                              Review positions for close signals
  /cancel <order_id>                   Cancel a resting order

Account:
  /portfolio                     Overview: positions, P&L, risk snapshot
  /portfolio positions           Open positions
  /portfolio orders              Resting orders
  /portfolio balance             Account balance

System:
  /model                         Change LLM model/provider
  /setup                         Re-run setup wizard
  init                           Launch with setup wizard (run: kalshi init)
  clear-cache                    Delete local cache (run: kalshi clear-cache)
  /help [command]                Show help for a command
  /quit                          Quit

Tips:
  Type natural language — e.g. "analyze KXBTC", "show my portfolio"
  Press Esc to cancel a running query`;
}

export function buildHelp(ctx: HelpContext, topic?: string): { text: string } | { error: string } {
  const topics = buildTopics(ctx);

  if (topic && topics[topic]) {
    return { text: topics[topic] };
  }

  if (topic) {
    return { error: `Unknown help topic: "${topic}". Available: ${Object.keys(topics).join(', ')}` };
  }

  return { text: buildOverview(ctx) };
}

/** Shared trade argument validation for both dispatch and slash handlers. */
export function validateTradeArgs(
  countStr: string,
  priceStr?: string,
): { count: number; price: number | undefined } | { error: string } {
  if (!/^\d+$/.test(countStr)) {
    return { error: `Invalid count: ${countStr}` };
  }
  const count = Number(countStr);
  if (count <= 0) {
    return { error: `Invalid count: ${countStr}` };
  }

  let price: number | undefined;
  if (priceStr !== undefined) {
    if (!/^\d+$/.test(priceStr)) {
      return { error: `Invalid price: ${priceStr}. Price must be 1-99 (cents).` };
    }
    price = Number(priceStr);
    if (price < 1 || price > 99) {
      return { error: `Invalid price: ${priceStr}. Price must be 1-99 (cents).` };
    }
  }

  return { count, price };
}
