# Kalshi Trading Bot CLI — User Guide

AI-powered prediction market terminal for [Kalshi](https://kalshi.com). Ask natural language questions, research markets, and trade — all from your terminal.

---

## Getting Started

### Prerequisites

- **[Bun](https://bun.com/) ≥ 1.1** — required. The bot uses `bun:sqlite` and runs `.tsx` directly, so Node.js will not work.
  ```bash
  curl -fsSL https://bun.com/install | bash
  ```
- A **Kalshi** account with API access (API key + RSA private key)
- At least one **LLM API key** (OpenAI, Anthropic, Google, xAI, OpenRouter, or a local Ollama)
- Optional: **[Octagon](https://app.octagonai.co)** key for AI edge analysis, **Tavily** key for web research

### Setup

```bash
bunx kalshi-trading-bot-cli
```

That's it — no clone required. The setup wizard runs automatically on first launch and writes your API keys to `~/.kalshi-bot/.env`.

Other ways to run it:

```bash
bun add -g kalshi-trading-bot-cli  # then just `kalshi`
```

Or from a clone (development):

```bash
git clone https://github.com/OctagonAI/kalshi-trading-bot-cli.git
cd kalshi-trading-bot-cli
bun install
bun start        # or `bun run dev` for hot-reload
```

### Where things live

- **Config, cache, SQLite DB:** `~/.kalshi-bot/`
- **API keys (`.env`):** `~/.kalshi-bot/.env`. A `.env` in the current directory takes precedence (dev override).

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `KALSHI_API_KEY` | Yes | Your Kalshi API key |
| `KALSHI_PRIVATE_KEY_FILE` | Yes* | Path to RSA private key PEM file |
| `KALSHI_PRIVATE_KEY` | Yes* | Inline RSA private key (alternative to file) |
| `KALSHI_USE_DEMO` | No | Set `true` for demo/paper trading (no real money) |
| `OPENAI_API_KEY` | One of these | OpenAI API key |
| `ANTHROPIC_API_KEY` | One of these | Anthropic API key |
| `GOOGLE_API_KEY` | One of these | Google AI API key |
| `XAI_API_KEY` | One of these | xAI API key |
| `OPENROUTER_API_KEY` | One of these | OpenRouter API key |
| `OLLAMA_BASE_URL` | No | Ollama endpoint (default `http://127.0.0.1:11434`) |
| `TAVILY_API_KEY` | No | Enables web search tool for background research |
| `LANGSMITH_API_KEY` | No | LangSmith tracing for debugging |

*Provide either `KALSHI_PRIVATE_KEY_FILE` or `KALSHI_PRIVATE_KEY`, not both.

---

## How It Works

The bot runs an **AI agent loop** (up to 10 iterations) that can reason, call tools, inspect results, and call more tools before delivering a final answer. You interact via two modes:

1. **Natural language** — type any question and the agent researches it using its tools
2. **Slash commands** — quick shortcuts for common actions (see below)

### Switching Models

Type `/model` to pick your LLM provider and model. Your choice persists across sessions. Supported providers: OpenAI, Anthropic, Google, Ollama (local), and OpenRouter (any model).

---

## Slash Commands

Quick commands that bypass the AI agent and call the Kalshi API directly.

| Command | Description | Example |
|---|---|---|
| `/help` | Show all available commands | `/help` |
| `/status` | Exchange open/closed status | `/status` |
| `/balance` | Account balance | `/balance` |
| `/positions` | Open positions with P&L | `/positions` |
| `/orders` | Resting (open) orders | `/orders` |
| `/markets [series]` | Browse markets, optionally filter by series ticker | `/markets KXBTC` |
| `/market <ticker>` | Market detail + top-of-book orderbook | `/market KXBTC-26MAR-B80000` |
| `/buy <ticker> <count> [price]` | Buy YES contracts (price in cents) | `/buy KXBTC-26MAR-B80000 5 56` |
| `/sell <ticker> <count> [price]` | Sell YES contracts | `/sell KXBTC-26MAR-B80000 5 60` |
| `/cancel <order_id>` | Cancel a resting order | `/cancel abc-123-def` |

**Trade confirmation:** `/buy` and `/sell` always show a confirmation prompt before executing. Type `yes` to confirm or `no` to cancel.

**Price format:** Prices are always in cents. `56` = $0.56 = 56% implied probability.

---

## Natural Language Queries

This is the primary way to use the bot. The AI agent has access to all the tools below and will chain them together automatically.

### Example Queries

**Market research:**
- "What are the odds of Trump winning in 2028?"
- "Show me all open Bitcoin markets"
- "What's the implied probability of the Fed cutting rates this month?"
- "Find markets related to AI regulation"

**Price and data:**
- "What's the current price of KXBTC-26MAR-B80000?"
- "Show me the orderbook for KXBTC-26MAR-B80000"
- "Give me a price history chart for this market over the last week"

**Portfolio:**
- "What's my balance?"
- "Show me my open positions and P&L"
- "List my recent fills"
- "Do I have any resting orders?"

**Trading (requires confirmation):**
- "Buy 10 YES contracts of KXBTC-26MAR-B80000 at 55 cents"
- "Sell my position in KXBTC-26MAR-B80000"
- "Cancel all my resting orders"
- "Place a limit order: 5 YES on KXPRES-28-DJT at 30 cents"

**Web research:**
- "What's the latest news about the 2028 presidential race?"
- "Search for recent Bitcoin ETF developments"

---

## Tool Reference

The agent has access to the following tools. You never call these directly — the agent selects them based on your query.

### kalshi_search (Market Research Router)

The primary research tool. Takes your natural language query and automatically routes to the right Kalshi API endpoints across up to **3 iterations** (browse → drill down → analyze).

**How it works:**
1. An LLM reads your query and decides which sub-tools to call
2. Sub-tool results are collected and the LLM decides if it needs more data
3. If so, it calls additional sub-tools (e.g., drilling into a specific event for contract prices)
4. After at most 3 iterations (or when the LLM has enough data), combined results are returned

**Sub-tools available to the router:**

#### Market Tools

| Tool | Purpose | Key Parameters |
|---|---|---|
| `get_markets` | List/browse markets | `event_ticker`, `series_ticker`, `status` (open/closed/settled), `tickers[]`, `limit` |
| `get_market` | Single market details | `ticker` (required) |
| `get_market_orderbook` | Order book depth (bid/ask levels) | `ticker` (required), `depth` |
| `get_market_candlesticks` | OHLC price history | `ticker` (required), `start_ts`, `end_ts`, `period_interval` (minutes) |

#### Event & Series Tools

| Tool | Purpose | Key Parameters |
|---|---|---|
| `get_events` | Browse/list events | `status`, `series_ticker`, `with_nested_markets`, `limit` |
| `get_event` | Single event with optional nested markets | `event_ticker` (required), `with_nested_markets` |
| `get_series` | Series metadata and settlement sources | `series_ticker` (required) |

#### Portfolio Tools

| Tool | Purpose | Key Parameters |
|---|---|---|
| `get_balance` | Account balance | *(none)* |
| `get_positions` | Open positions | `event_ticker`, `ticker` |
| `get_fills` | Trade executions/fills | `ticker`, `order_id`, `min_ts`, `max_ts`, `limit` |
| `get_settlements` | Resolved market settlements | `ticker`, `limit` |
| `get_orders` | Order history | `ticker`, `event_ticker`, `status` (resting/canceled/executed/all), `limit` |
| `get_order` | Single order details | `order_id` (required) |

#### Historical Tools

| Tool | Purpose | Key Parameters |
|---|---|---|
| `get_historical_markets` | Past/closed markets | `series_ticker`, `event_ticker`, `status`, `limit` |
| `get_historical_market` | Single historical market | `ticker` (required) |
| `get_historical_candlesticks` | Historical OHLC data | `ticker` (required), `start_ts`, `end_ts`, `period_interval` |
| `get_historical_fills` | Historical fills | `ticker`, `limit` |
| `get_historical_orders` | Historical orders | `ticker`, `limit` |

#### Exchange Tools

| Tool | Purpose | Key Parameters |
|---|---|---|
| `get_exchange_status` | Is the exchange open/trading? | *(none)* |
| `get_exchange_schedule` | Trading hours and maintenance windows | *(none)* |

### kalshi_trade (Trade Execution Router)

Routes natural language trade instructions to the appropriate trading action. **Always requires user approval** before executing.

**Sub-tools:**

| Tool | Purpose | Key Parameters |
|---|---|---|
| `place_order` | Place a single order | `ticker`, `action` (buy/sell), `side` (yes/no), `type` (limit/market), `count`, `yes_price` (1-99 cents) |
| `amend_order` | Modify a resting order | `order_id`, `count`, `yes_price`, `expiration_ts` |
| `cancel_order` | Cancel one order | `order_id` |
| `cancel_orders` | Batch cancel | `order_ids[]` |
| `place_batch_orders` | Place multiple orders at once | `orders[]` (array of order specs) |

### portfolio_overview

Quick composite tool that fetches balance + all positions in a single call. Used when the agent needs a fast portfolio snapshot.

### exchange_status

Checks whether the Kalshi exchange is currently open and trading is active.

### web_search

Searches the web for current events, news, and background research (powered by Tavily). Only available if `TAVILY_API_KEY` is set.

### web_fetch

Fetches and parses content from a specific URL. Used for reading articles, press releases, or any web content referenced in market research.

---

## Ticker Formats

Kalshi uses a hierarchical ticker system:

| Level | Format | Example | Description |
|---|---|---|---|
| Series | `KXBTC` | `KXBTC` | A recurring topic (e.g., Bitcoin price) |
| Event | `KXBTC-26MAR` | `KXPRES-28` | A specific occurrence (e.g., March 2026 BTC, 2028 election) |
| Market | `KXBTC-26MAR-B80000` | `KXPRES-28-DJT` | A single yes/no contract within an event |

**Price interpretation:** All prices are in **cents** (1-99). A price of `56` means $0.56, which implies a **56% probability** of the YES outcome. YES + NO prices always sum to approximately 100 cents.

---

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `Enter` | Submit message |
| `Esc` | Cancel current action (agent execution, model selection) |
| `Ctrl+C` | Exit the app |
| Up/Down arrows | Navigate input history |

---

## Tips

- **Demo mode**: Set `KALSHI_USE_DEMO=true` to trade with fake money while learning
- **Multi-step research**: The search router automatically drills down — ask "what's the implied probability of X" and it will find the event, then fetch contract-level prices
- **Be specific**: "BTC markets closing this week" works better than "crypto"
- **Trade safely**: All trades require explicit confirmation. The agent will show you the order details and ask for approval
- **Web + Kalshi**: Combine web search with market data — "what's the latest polling for 2028 and how do Kalshi odds compare?"
