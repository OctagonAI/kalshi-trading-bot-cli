// ─── Shared help content for both TUI slash commands and CLI batch mode ─────

/** Context determines prefix style: slash commands use "/", CLI uses "kalshi" */
type HelpContext = 'slash' | 'cli';

function prefix(ctx: HelpContext): string {
  return ctx === 'slash' ? '/' : 'kalshi ';
}

function buildTopics(ctx: HelpContext): Record<string, string> {
  const p = prefix(ctx);
  return {
    search: `**${p}search** — Discovery

${p}search [theme|ticker|query]  Search events by theme, ticker, or free-text
${p}search themes                List all available themes and subcategories
${p}search edge                  Scan all markets by Octagon model edge
${p}search edge --min-edge 30    Markets with ≥30pp edge
${p}search edge --limit 50       Top 50 results
${p}search edge --category crypto Filter by category

Examples:
  ${p}search crypto
  ${p}search crypto:btc
  ${p}search "bitcoin price"
  ${p}search edge --min-edge 30 --category crypto`,

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

${p}backtest                              30-day lookback, both sections (default)
${p}backtest --days 60                    60-day lookback
${p}backtest --resolved                   Resolved markets only
${p}backtest --unresolved                 Unresolved markets only
${p}backtest --category crypto            Filter by category
${p}backtest --min-edge 10                Stricter edge threshold (pp)
${p}backtest --export results.csv         Per-market detail CSV
${p}backtest --json                       Machine-readable output

Looks back N days, compares what the model said then to where the market is now.
Resolved markets: scored against Kalshi settlement (0 or 100).
Unresolved markets: mark-to-market vs current Kalshi trading price.`,

    'clear-cache': `**${ctx === 'cli' ? '' : 'bun start '}clear-cache** — Delete local cache

${ctx === 'cli' ? `${p}` : 'bun start '}clear-cache                Delete the local SQLite database (~/.kalshi-bot/kalshi-bot.db)
                               A fresh database will be created on next command.

Use this when the local cache is corrupted or you want to start fresh.${ctx !== 'cli' ? '\nRun from terminal: bun start clear-cache' : ''}`,

    init: `**${p}init** — Re-run setup wizard

${p}init                       Launch the TUI with the setup wizard open
                               Use this to configure or reconfigure API keys and preferences.`,

    help: `**${p}help** — Show help

${p}help                       Show all commands
${p}help <command>             Show detailed help for a command`,
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
  search [theme|ticker|query]   Find markets by keyword or theme
  search --refresh <query>      Force index rebuild then search
  search themes                 List all themes and subcategories
  watch <ticker>                Live price/orderbook feed
  watch --theme <theme>         Continuous theme scan (Ctrl+C to stop)
  watch --refresh               Force index rebuild before watching

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
Backtest flags: --days, --resolved, --unresolved, --category, --min-edge, --export
Run "kalshi help <command>" for detailed usage.`;
  }

  return `**Kalshi Trading Bot CLI — Commands**

Quick start:
  /search crypto          Find markets by keyword or theme
  /analyze <ticker>       Deep analysis + trade recommendation
  /watch --theme crypto   Continuous scan across a theme

Discovery:
  /search [theme|ticker|query]   Find markets by keyword or theme
  /search --refresh <query>      Force index rebuild then search
  /search themes                 List all themes and subcategories
  /watch <ticker>                Live price/orderbook feed
  /watch --theme <theme>         Continuous theme scan (Esc to stop)
  /watch --refresh               Force index rebuild before watching

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
  init                           Launch with setup wizard (run: bun start init)
  clear-cache                    Delete local cache (run: bun start clear-cache)
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
