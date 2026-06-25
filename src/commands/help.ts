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
  --close-before <iso>  Only markets closing before this timestamp
  --days-to-close <n>   Shortcut: only markets closing in the next N days
  --limit <n>           Page size (default 30)
  --sort-by <key>       volume_24h | close_time | last_price (server-side sort)
  --aggregate-by series Roll up results by series (calls series rollup)
  --active-only         Drop non-active markets (defensive; the live universe is active by default)

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

${p}analyze <ticker>                       Full analysis: edge, drivers, catalysts, Kelly sizing
${p}analyze <ticker> ${ctx === 'cli' ? '--' : ''}refresh             Force fresh Octagon report

Batch mode (one Octagon round-trip instead of N):
${p}analyze KX-A KX-B KX-C                 Edge readout across 2-100 tickers
${p}analyze --tickers KX-A,KX-B,KX-C       Same, comma-separated
${p}analyze KX-A KX-B KX-C --json          For pipelines / scripting

The batch mode hits POST /kalshi/markets/edge in one call and returns
model_probability, market_probability, edge_pp, expected_return per ticker.
Use single-ticker mode when you need the full deep-analysis pipeline
(drivers, catalysts, Kelly sizing, risk gate).${ctx === 'cli' ? `

Legacy aliases (still work):
  ${p}edge [--ticker X]                    Edge history / snapshots (default: last 24h)
  ${p}edge --since <date>                  Edges since date (e.g. 2026-03-01)` : ''}`,

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
${p}backtest --universe api              Systematic Octagon-API universe (default; reproducible across machines)
${p}backtest --universe local            Legacy local octagon_reports universe (offline, NON-SYSTEMATIC)
${p}backtest --fees taker                Apply Kalshi taker fee (0.07·p·(1−p) per entry); default 'none' = gross
${p}backtest --fees maker                Maker execution (free entry)
${p}backtest --export results.csv         Per-market detail CSV
${p}backtest --json                       Machine-readable output

Looks back N days, compares what the model said then to where the market is now.
Resolved markets: scored against Kalshi settlement (0 or 100).
Unresolved markets: mark-to-market vs current Kalshi trading price.
Per-contract entry: mp/kp come from the per-contract outcome_probabilities on the
Octagon snapshot (no event-level fallback). Volume gate requires per-contract
volume from the snapshot; signals without it are dropped (the legacy fallback
to Kalshi lifetime volume was a look-ahead and has been removed).
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

    scripting: `**Scripting & Parallel Use** — for agents, pipelines, and parallel invocations

The \`bunx kalshi-trading-bot-cli@latest …\` form is convenient for one-off use
but has two gotchas under scripting:

  1. Bun's install chatter ("Resolving dependencies", "Saved lockfile") leaks
     into stdout before our CLI runs, corrupting JSON pipelines.
  2. Parallel \`bunx\` invocations race on the install cache and fail with
     "Failed to link …: EEXIST" / "could not determine executable".
     See oven-sh/bun#12917 for upstream status.

**Recommended for scripts and agents:**

  bun add -g kalshi-trading-bot-cli           # install once; emits no chatter on subsequent runs
  parallel -j 30 'kalshi analyze {} --json' ::: TICKER1 TICKER2 …

**If you must use bunx:**

  bunx --silent kalshi-trading-bot-cli@latest analyze KX-A --json
                ^^^^^^^^ suppresses install chatter; keeps our stdout clean

For parallel bunx, pre-warm the cache serially before fanning out:

  bunx --silent kalshi-trading-bot-cli@latest --version        # one-shot, warms cache
  parallel -j 30 'bunx --silent kalshi-trading-bot-cli@latest analyze {} --json' ::: …

See README → Scripting & Parallel Use for the full picture.`,

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
${p}correlate KX-A KX-B --sides yes,no    Side-aware: corr(YES_A, NO_B) flips sign
${p}correlate KX-A KX-B --cells           Per-cell detail (overlap_count + reason)

Flags:
  --window-days <n>             Lookback (1-730, default 30; auto interval picks 1d if >=90)
  --correlation-interval <1h|1d>  Override bin size
  --tickers <csv>               Alternative to positional args
  --sides yes,no,yes            Per-ticker side (same length as tickers); default all yes
  --cells                       Include per-cell detail (overlap_count, reason)
  --json                        JSON output (matrix + ranked_pairs + cells_detail)

Output ranks pairs ascending by correlation — most-uncorrelated first.`,

    report: `**${p}report** — Print the full Octagon markdown report for an event

${p}report <event_ticker>           Cached report body (most recent)
${p}report <market_ticker>          Resolves to the parent event automatically
${p}report <series_ticker>          Resolves to the latest event in the series
${p}report <kalshi_url>             Accepts a full kalshi.com URL too
${p}report <ticker> --refresh       Force a fresh pull from Octagon (costs 3 credits)

The full deep-research markdown body — same content the OctagonAI web app shows.
Lookup is more lenient than \`analyze\`: tries Octagon's event endpoint first
before falling back to the Kalshi resolver chain, so series tickers and
events without open Kalshi markets still work.

Flags:
  --refresh    Force a fresh report instead of returning the cached one${ctx === 'cli' ? `
  --json       JSON envelope output (rawReport carries the markdown)` : ''}

When a report body is found, the output footer shows: source (cache | fresh),
local cache fetch timestamp + age, and the upstream Octagon
analysis_last_updated when available — so you can decide whether to --refresh.
Error paths (missing ticker, event not found, no report body yet) print just
the error message instead.`,

    trust: `**${p}trust** — Trader Trust scorecard (market-integrity metrics)

${p}trust <event_ticker>                       Table across all markets in the event
${p}trust <event_ticker> --market <market>     Single-market detail card
${p}trust <event_ticker> --market <market> --verbose
                                            Include raw evidence + confidence/freshness

Six per-market scores (each 0-100), produced by Octagon's deterministic
Trader Trust calculation:

  trader_trust       Overall composite                      (higher = better)
  liquidity_quality  Depth/spread/fill behavior             (higher = better)
  move_quality       Price-move plausibility                (higher = better)
  resolution_risk    Resolution clarity (higher = clearer)  (higher = better)
  market_avoid       Avoidance signal                       (higher = WORSE)
  quote_risk         Quote-side risk                        (higher = WORSE)

Flags:
  --market <ticker>   Drill into one market in the event
  --verbose           Show evidence (raw metrics), confidence, data freshness
  --json              JSON envelope output

Notes:
  - When trader_trust_json is null (older reports), prints "no trust scorecard for
    this event yet" — not an error.
  - Higher-is-better vs. higher-is-worse semantics differ per score; tables and
    detail views color and annotate accordingly.
  - "(as of report time)" is shown for scores whose data_freshness is
    point_in_time (e.g. quote_risk, liquidity_quality on snapshot reports).`,

    events: `**${p}events** — Octagon event rollups (event ↔ outcome ladder)

${p}events                              List events sorted by total_volume
${p}events --category Politics          Filter by series_category
${p}events --min-volume 10000           Volume floor
${p}events --limit 25                   Page size (default 50)
${p}events KXFEDCHAIRNOM-29             Drill into one event: outcome probabilities + per-contract edge

Flags:
  --category <name>     Filter by series_category (case-insensitive substring)
  --min-volume <n>      Floor on total_volume
  --limit <n>           Page size (default 50)
  --json                JSON envelope output

Each event is a multi-market Kalshi question (e.g. "Who will Trump nominate as Fed Chair?")
with one binary sub-market per outcome (Kevin Warsh, Judy Shelton, ...).
Octagon supplies a model_probability per outcome so you can rank contracts by edge.`,

    series: `**${p}series** — Series-level rollups over the Kalshi universe

${p}series                          List series with 24h vol, market count, dominant category
${p}series list --min-volume 10000  Liquidity filter
${p}series list --category Crypto   Filter by category
${p}series KXBTCD                   Drill in: all sub-markets sorted by volume
${p}series search "bitcoin"         Keyword search → rolled up by series
${p}series candles KXBTCD --timeframe 3m   Series NAV = equal-weight basket of top sub-markets
${p}series events KXIPO              List events in a series (e.g. KXIPO → KXIPO-26)

Flags:
  --min-volume <n>       Floor on 24h volume per series
  --category <name>      Filter by category
  --limit <n>            Page size (default 50)
  --timeframe <1w|1m|3m|6m|1y>  Candle window (default 1y; for "series candles")
  --top-k <n>            Sub-markets to include in series NAV basket (default 20)
  --series <prefix>      Filter list by series-ticker prefix (e.g. KXBTC)
  --json                 JSON output

A series is the Kalshi grouping above individual markets — KXBTCD is the BTC
strike ladder, with hundreds of sub-markets like KXBTCD-26DEC31-T100000.
Series list is now a single server-side call (was 25 paginated calls).`,

    catalysts: `**${p}catalysts** — Upcoming Kalshi market closes grouped by week

${p}catalysts upcoming                       Next 30 days
${p}catalysts upcoming --days 7              Next week
${p}catalysts upcoming --days 14 --min-volume 5000 --category Politics
${p}catalysts upcoming --limit 10            Up to 10 markets per week shown

Flags:
  --days <n>           Lookback window (default 30)
  --min-volume <n>     Floor on 24h volume
  --category <name>    Filter by category
  --limit <n>          Top-N markets per week (default 8)
  --json               JSON output

Use for catalyst-calendar planning: see which weeks have major Kalshi
resolutions cluster up so you can position before catalyst risk.`,

    themes: `**${p}themes** — Editorial narrative registry (curated theme buckets)

${p}themes                                List registered editorial themes
${p}themes import                         Seed from data/themes_seo.json (25 starter themes)
${p}themes import <path>                  Import from a custom JSON file
${p}themes export <path>                  Export current registry
${p}themes show "Iran Escalation"         Drill into one theme
${p}themes create "My Theme" --tickers KXA,KXB --label "..." [--min-volume N]
${p}themes delete "My Theme"
${p}themes add-series "My Theme" KXBTCD,KXETHD
${p}themes remove-series "My Theme" KXBTCD
${p}themes set-search-volume "My Theme" 100000
${p}themes report                         Dashboard: 25-theme grid with SEO + liquidity
${p}themes audit                          Flag dead themes (high SEO + zero volume)
${p}themes overlap                        Cross-theme dedupe report

Editorial themes are narrative buckets you curate (e.g. "AI Race Milestones",
"Iran Escalation") — distinct from Octagon's ML clusters. Each theme maps to a
list of Kalshi series and an optional monthly search-volume estimate.

Flags:
  --label <desc>        Set description on create
  --min-volume <n>      Set search_volume on create (poorly named — improve later)
  --tickers <csv>       Comma-separated series on create
  --json                JSON output

Legacy: ${p}search themes still lists Kalshi category labels (the pre-registry view).`,

    basket: `**${p}basket** — Build, backtest, and size diversified baskets

${p}basket build [universe filters] [-n N] [--max-per-cluster M] [--max-corr X] [--bankroll $ --kelly K --probs ...]
${p}basket backtest --tickers KX-A,KX-B --weights 0.6,0.4 --timeframe 1y
${p}basket candles  --tickers KX-A,KX-B --timeframe 6m
${p}basket size     --bankroll 1000 --kelly 0.25 --probs KX-A:0.62,KX-B:0.55 [--side yes|no]

Validate flags:
  --tickers KX-A,KX-B           Validate explicit tickers (equal-stake split)
  --probs KX-A:yes:170,KX-B:no:160  Per-leg ticker:side:stake
  --theme <name>                Resolve from editorial registry
  --bankroll <usd>              Used to compute max_leg_pct + warnings
  --window-days <n>             Correlation lookback (default 30)
  --max-corr <-1..1>            Soft threshold for correlation warning

Build flags (universe + diversification):
  --category <name>             Restrict candidate pool by category
  --series <ticker>             Restrict to a series
  --min-volume <n>              Volume floor for candidates
  --close-before <iso>          Only markets closing before
  --label <csv>                 Restrict cluster labels (substring match, comma-separated)
  -q "<text>"                   Anchor candidate pool by free-text intent (semantic)
  --ticker <ticker>             Anchor candidate pool by ticker (semantic)
  --tickers KX-A,KX-B           Explicit candidate pool (universe.market_tickers)
  --theme <name>                Resolve theme registry → explicit candidate pool
  --auto-probs                  Auto-fetch model probabilities (markets/edge)
                                and use Kelly sizing
  -n <n>                        Number of legs (1-20)
  --max-per-cluster <n>         Cap legs per thematic cluster
  --max-corr <x>                Pairwise correlation cap (-1 to 1)
  --limit <n>                   Candidate pool size (2-200)
  --window-days <n>             Correlation window (7-365)

Sizing flags (build & size):
  --bankroll <usd>              Required for Kelly sizing
  --kelly <fraction>            Kelly multiplier 0-1 (default 0.25)
  --probs KX-A:0.62,KX-B:0.55   Model probabilities per ticker (manual)
  --auto-probs --tickers KX-A,KX-B   Auto-fetch via POST /markets/edge
  --auto-probs --theme <name>   Resolve theme + auto-fetch probabilities
  --side <yes|no>               Default leg side for "basket size" (default yes)

Backtest/candles flags:
  --tickers <csv>               Tickers (required if --theme is absent)
  --theme <name>                Resolve from editorial registry (top market per series)
  --weights <csv>               Optional weights, same length as tickers
  --timeframe <1w|1m|3m|6m|1y>  Window/bin size

Common:
  --json                        JSON output

Recipes:
  ${p}basket build --category crypto --min-volume 10000 -n 8 --max-per-cluster 2 --max-corr 0.6
  ${p}basket build --label fed,cpi,fomc,gdp,jobs -n 5 --max-per-cluster 1 --max-corr 0.4
  ${p}basket build --tickers KX-A,KX-B,KX-C -n 2 --max-corr 0.5   # explicit candidate pool
  ${p}basket build --theme "Iran Escalation" -n 3 --max-per-cluster 1 --auto-probs --bankroll 1000
  ${p}basket backtest --tickers KX-A,KX-B,KX-C --weights 0.4,0.4,0.2 --timeframe 1y
  ${p}basket size --auto-probs --theme "Iran Escalation" --bankroll 1000 --kelly 0.25
  ${p}basket validate --theme "Iran Escalation" --bankroll 1000      # sanity-check before placing
  ${p}basket validate --tickers KX-A,KX-B --bankroll 1000 --max-corr 0.5`,
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
  search --sort-by volume_24h   Top-N by liquidity
  search --aggregate-by series  Roll up results to series level
  search themes                 (Legacy) Kalshi category labels
  search edge [--min-edge N]    Edge ranking (Octagon when key set, else local)
  similar <ticker>              Semantic neighbors (embedding distance)
  similar -q "free text"        Semantic search by natural-language query
  clusters [--label X]          Browse thematic clusters
  clusters <id>                 List markets in a cluster
  clusters --behavioral         Behavioral clusters (30-day return vectors)
  clusters --ranked             Rank clusters by historical basket return
  peers <ticker>                Find markets in the same cluster
  events                        Octagon events (event ↔ outcome ladder)
  events <event_ticker>         Drill into one event's outcome probabilities
  series                        Series rollup with 24h vol, market count
  series <SERIES>               Sub-markets in one series
  series candles <SERIES>       Series NAV (basket of top sub-markets)
  catalysts upcoming --days 30  Markets closing soon, grouped by week
  trust <event_ticker>          Trader Trust scorecard (table across markets)
  trust <event> --market <mkt>  Single-market trust detail card
  report <event_ticker>         Full Octagon markdown report (use --refresh for fresh pull)
  watch <ticker>                Live price/orderbook feed
  watch --theme <theme>         Continuous theme scan (Ctrl+C to stop)
  watch --refresh               Force index rebuild before watching

Editorial themes (narrative registry):
  themes                        List registered editorial themes
  themes import                 Seed from data/themes_seo.json (25 starter themes)
  themes show <name>            Drill into one theme
  themes report                 25-theme dashboard with SEO + liquidity
  themes audit                  Flag dead themes (high SEO + zero volume)
  themes overlap                Cross-theme dedupe report
  themes create/delete/add-series/remove-series/set-search-volume/export

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
                --min-volume, --min-price, --max-price, --export,
                --universe api|local (default api), --fees none|taker|maker (default none)
Run "kalshi help <command>" for detailed usage.`;
  }

  return `**Kalshi Trading Bot CLI — Commands**

Quick start:
  /search crypto          Find markets by keyword or theme
  /analyze <ticker>       Deep analysis + trade recommendation
  /watch --theme crypto   Continuous scan across a theme

Discovery:
  /search [theme|ticker|query]   Find markets (Octagon when key set, else local)
  /search --sort-by volume_24h   Top-N by liquidity
  /search --aggregate-by series  Roll up results to series level
  /search themes                 (Legacy) Kalshi category labels
  /search edge [--min-edge N]    Edge ranking (Octagon when key set, else local)
  /similar <ticker>              Semantic neighbors (embedding distance)
  /similar -q "free text"        Semantic search by natural-language query
  /clusters [--label X]          Browse thematic clusters
  /clusters <id>                 List markets in a cluster
  /clusters --behavioral         Behavioral clusters (30-day return vectors)
  /clusters --ranked             Rank clusters by historical basket return
  /peers <ticker>                Find markets in the same cluster
  /events                        Octagon events (event ↔ outcome ladder)
  /events <event_ticker>         Drill into one event's outcome probabilities
  /series                        Series rollup with 24h vol, market count
  /series <SERIES>               Sub-markets in one series
  /series candles <SERIES>       Series NAV (basket of top sub-markets)
  /catalysts upcoming --days 30  Markets closing soon, grouped by week
  /trust <event_ticker>          Trader Trust scorecard (table across markets)
  /trust <event> --market <mkt>  Single-market trust detail card
  /report <event_ticker>         Full Octagon markdown report (use --refresh for fresh pull)
  /watch <ticker>                Live price/orderbook feed
  /watch --theme <theme>         Continuous theme scan (Esc to stop)
  /watch --refresh               Force index rebuild before watching

Editorial themes (narrative registry):
  /themes                        List registered editorial themes
  /themes import                 Seed from data/themes_seo.json (25 starter themes)
  /themes show <name>            Drill into one theme
  /themes report                 25-theme dashboard with SEO + liquidity
  /themes audit                  Flag dead themes (high SEO + zero volume)
  /themes overlap                Cross-theme dedupe report
  /themes create/delete/add-series/remove-series/set-search-volume/export

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
