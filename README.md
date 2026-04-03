# Kalshi Trading Bot CLI

AI-powered Kalshi trading CLI that finds edge and executes trades.

Runs deep fundamental research on every market — independent probability estimates, ranked price drivers, catalyst calendars — then computes edge as the spread between model price and the live order book. Signals are sized using half-Kelly and filtered through a 5-gate risk engine before a dollar is risked.

Integrates with the [Octagon Research API](https://app.octagonai.co) for AI-generated probability estimates that power the edge detection engine.

![Kalshi Deep Trading Bot](assets/screenshot.png)

## Quick Start

```bash
git clone https://github.com/OctagonAI/kalshi-deep-trading-bot-cli.git
cd kalshi-deep-trading-bot-cli
bun install
bun start
```

The setup wizard runs automatically on first launch — it walks you through API keys and validates connectivity. No `.env` editing required.

> Don't have Bun? Install it with `curl -fsSL https://bun.com/install | bash`

## Example Session

```
$ bun start

Welcome to Kalshi Deep Trading Bot
Type help for commands, or just ask a question.

> search crypto

  Ticker                  Title                          Last    Volume
  KXBTC-26APR-B95000      Bitcoin above $95k by Apr 30   $0.58   12,841
  KXBTC-26APR-B100000     Bitcoin above $100k by Apr 30  $0.31    8,203
  KXETH-26APR-B2000       Ethereum above $2k by Apr 30   $0.72    5,419

3 markets found

> analyze KXBTC-26APR-B95000

  Octagon Research Report — KXBTC-26APR-B95000
  ─────────────────────────────────────────────
  Model Probability   72%
  Market Price        58%
  Edge               +14.0%  (very_high confidence)

  Top Drivers
  1. Bitcoin ETF inflows accelerating            impact: high
  2. Halving cycle momentum                      impact: high
  3. Macro risk-on sentiment                     impact: moderate

  Kelly Sizing
  Recommended: 3 contracts YES at $0.58
  Risk gates: ✓ Kelly  ✓ Liquidity  ✓ Correlation  ✓ Concentration  ✓ Drawdown

> buy KXBTC-26APR-B95000 3 58

  ✓ Order placed: BUY 3 YES @ $0.58
  Order ID: abc-123-def

> portfolio

  Ticker                  Side  Qty  Entry   Now    Edge    P&L
  KXBTC-26APR-B95000      YES    3   $0.58   $0.61  +11.0%  +$0.09

  Cash: $487.26 · Exposure: $1.74 · Positions: 1
```

## Commands

| Command | Description |
|---------|-------------|
| `search [theme\|ticker\|query]` | Find markets by keyword or theme |
| `analyze <ticker>` | Deep analysis: edge, drivers, Kelly sizing |
| `watch <ticker>` | Live price and orderbook feed |
| `watch --theme <theme>` | Continuous theme scan |
| `buy <ticker> <count> [price] [yes\|no]` | Buy contracts |
| `sell <ticker> <count> [price] [yes\|no]` | Sell contracts |
| `cancel <order_id>` | Cancel a resting order |
| `portfolio` | Positions, P&L, risk snapshot |
| `setup` | Re-run setup wizard (inside TUI) |
| `init` | Launch setup wizard from CLI (`bun start init`) |
| `clear-cache` | Delete local cache and rebuild (`bun start clear-cache`) |
| `help [command]` | Detailed help for a command |

### Flags

| Flag | Description |
|------|-------------|
| `--json` | JSON output for scripts and agents |
| `--refresh` | Force fresh Octagon report (analyze) |
| `--performance` | Include win rate, Sharpe, Brier scores (portfolio) |
| `--dry-run` | Scan without persisting edges (watch) |
| `--verbose` | Verbose output |
| `--min-edge <n>` | Minimum edge threshold |
| `--interval <min>` | Scan interval in minutes (watch) |
| `--live` | Force 15m scan interval (watch) |

### Demo Mode

Set `KALSHI_USE_DEMO=true` in your `.env` to use Kalshi's demo environment. All trades are simulated — no real money at risk.

## Agent Usage

Every command supports `--json` for structured output, making the bot easy to orchestrate from scripts or AI agents.

```bash
bun start search crypto --json
bun start analyze KXBTC-26APR-B95000 --json
bun start buy KXBTC-26APR-B95000 3 58 --json
bun start portfolio --json
```

### JSON Response Format

All responses follow the same envelope:

```json
{
  "ok": true,
  "command": "analyze",
  "data": {
    "ticker": "KXBTC-26APR-B95000",
    "modelProb": 0.72,
    "marketProb": 0.58,
    "edge": 0.14,
    "confidence": "very_high",
    "drivers": [
      { "claim": "Bitcoin ETF inflows accelerating", "impact": "high" }
    ]
  },
  "meta": {
    "octagon_credits_used": 3,
    "octagon_cache_hits": 0
  },
  "timestamp": "2026-03-30T10:00:00.000Z"
}
```

Errors return `"ok": false` with an `error` object containing `code` and `message`. Exit code is 0 for success, 1 for failure.

### Example Orchestration Flow

```bash
# 1. Find markets
MARKETS=$(bun start search crypto --json | jq '.data')

# 2. Analyze top pick
ANALYSIS=$(bun start analyze KXBTC-26APR-B95000 --json)
EDGE=$(echo "$ANALYSIS" | jq '.data.edge')

# 3. Trade if edge is high enough
if (( $(echo "$EDGE > 0.05" | bc -l) )); then
  bun start buy KXBTC-26APR-B95000 3 58 --json
fi

# 4. Check portfolio
bun start portfolio --json
```

The `watch --theme` command outputs NDJSON (one JSON object per scan cycle), suitable for streaming pipelines.

## Configuration

### Environment Variables

```bash
cp env.example .env
```

The setup wizard (`bun start setup`) handles this interactively, but you can also edit `.env` directly:

**Required:**

| Variable | Description |
|----------|-------------|
| `KALSHI_API_KEY` | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_FILE` | Path to your Kalshi RSA private key PEM file |
| `OPENAI_API_KEY` | OpenAI API key (default model is GPT-5.4) |
| `OCTAGON_API_KEY` | Octagon API key for deep research. Get one at [app.octagonai.co](https://app.octagonai.co) |

**Optional:**

| Variable | Description |
|----------|-------------|
| `KALSHI_USE_DEMO` | `true` for demo environment (simulated trades) |
| `KALSHI_PRIVATE_KEY` | Inline PEM key as alternative to file path |
| `ANTHROPIC_API_KEY` | Anthropic (Claude) |
| `GOOGLE_API_KEY` | Google (Gemini) |
| `XAI_API_KEY` | xAI (Grok) |
| `OPENROUTER_API_KEY` | OpenRouter (multi-model) |
| `TAVILY_API_KEY` | Tavily web search for event research |

> **Note:** The bot defaults to GPT-5.4. If using a different provider, switch the model via the `config` command — otherwise queries will fail without `OPENAI_API_KEY`.

### Octagon Credits

Each Octagon report costs 3 credits. Reports are cached with tiered TTLs based on market close proximity — markets closing soon get shorter cache windows. Use `--refresh` to force a fresh report. Set a daily credit ceiling with `config octagon.daily_credit_ceiling <n>`.

### Runtime Settings

```bash
bun start config                              # List all settings
bun start config risk.kelly_multiplier        # Get a value
bun start config risk.kelly_multiplier 0.3    # Set a value
```

| Setting | Default | Description |
|---------|---------|-------------|
| `scan.interval` | `60` | Scan interval in minutes |
| `scan.theme` | `top50` | Default market theme |
| `risk.kelly_multiplier` | `0.5` | Kelly fraction (0.5 = half-Kelly) |
| `risk.max_drawdown` | `0.20` | Max drawdown before circuit breaker |
| `risk.max_positions` | `10` | Max concurrent open positions |
| `risk.max_per_category` | `3` | Max positions per event category |
| `risk.daily_loss_limit` | `200` | Daily loss limit in dollars |
| `octagon.daily_credit_ceiling` | `100` | Max Octagon credits per day |
| `alerts.min_edge` | `0.05` | Minimum edge to trigger an alert |

## Architecture

![Kalshi Trading Flow](assets/kalshi-flow-light.png)

The CLI talks to two external services: the Kalshi exchange API (market data, order placement, portfolio) and the Octagon research API (AI probability estimates, price drivers). Results are cached in a local SQLite database to minimize API calls and credit usage.

### LLM Providers

Default model is GPT-5.4. Switch with the `config` command.

| Prefix | Provider |
|--------|----------|
| `gpt-` | OpenAI |
| `claude-` | Anthropic |
| `gemini-` | Google |
| `grok-` | xAI |
| `openrouter/` | OpenRouter |
| `ollama:` | Ollama (local) |

### Development

```bash
bun dev              # Dev mode with hot reload
bun run typecheck    # Type checking
bun test             # Run tests
```

## Documentation

See the [User Guide](GUIDE.md) for detailed usage instructions, examples, and tips.

## License

MIT License — see [LICENSE](LICENSE) for details.
