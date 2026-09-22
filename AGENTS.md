# Repository Guidelines

A terminal CLI for Kalshi prediction-market research and trading: market
discovery and edge scanning, Octagon-backed analysis, risk-gated order
placement, backtesting, and a conversational agent. TypeScript on Bun.

## Project Structure

- Entry point: `src/index.tsx` (also the `kalshi` bin). Flag handling lives in
  `src/commands/parse-args.ts`; one-shot commands go through
  `src/commands/dispatch.ts`, the interactive TUI through `src/cli.ts`.
- `src/commands/` — one module per command (`trust.ts`, `similar.ts`,
  `analyze.ts`, `basket.ts`, `series.ts`, …), plus `help.ts` (help topics) and
  `scan-formatters.ts` (shared table rendering).
- `src/tools/` — tools exposed to the agent. `registry.ts` registers them
  conditionally on env vars; `kalshi/` is the signed exchange client
  (`api.ts`, `search-index.ts`, `dlq.ts`), `fetch/` is the web-fetch tool,
  `v2/` holds the newer tool set.
- `src/scan/` — Octagon clients and the edge pipeline: `octagon-events-api.ts`,
  `octagon-kalshi-api.ts`, `octagon-reports-api.ts`, `invoker.ts`,
  `edge-computer.ts`.
- `src/db/` — local SQLite (`bun:sqlite`): `schema.ts` (migrations),
  `index.ts` (`getDb`/`createDb`), `event-index.ts`, `edge.ts`.
- `src/risk/` — Kelly sizing, mandate caps, circuit breaker.
- Also: `src/backtest/`, `src/eval/`, `src/daemon/` (maintenance loop),
  `src/gateway/`, `src/audit/`, `src/setup/wizard.ts`, `src/components/` +
  `src/controllers/` (pi-tui UI), `src/model/`, `src/utils/`, `src/theme.ts`.
- User data lives under `~/.kalshi-bot/` (see `src/utils/paths.ts`):
  `config.json`, `kalshi-bot.db`, `messages/chat_history.json`, `.env`.
  Always build these paths with `appPath(...)`.

## Build, Test, and Development Commands

- Runtime: Bun. Install deps with `bun install`.
- Run: `bun run start` (or `bun run dev` for watch mode).
- Type-check: `bun run typecheck`. Tests: `bun test`.
- Integration tests (hit live APIs, need keys): `bun run test:integration`.
- Gateway: `bun run gateway:login`, `bun run gateway`.
- Run `bun run typecheck` and `bun test` before pushing.

## Coding Style & Conventions

- TypeScript, ESM, strict mode. Prefer precise types; avoid `any`.
- The UI is [@mariozechner/pi-tui](https://www.npmjs.com/package/@mariozechner/pi-tui),
  not Ink/React. `.tsx` is used only for the entry point.
- Match the surrounding style; keep changes surgical.
- Comment non-obvious logic — especially anything that encodes an upstream API
  quirk — and say why, not what.
- All network calls go through `fetchWithDeadline` (`src/utils/http.ts`) so a
  half-open connection can't hang the process.
- Colored table cells must go through `formatTable`, which measures visible
  width; padding by raw `.length` breaks alignment on ANSI strings.

### Changing a command's flags or signature

Update every one of these together, or the CLI, TUI and docs drift apart:
`src/commands/parse-args.ts`, `src/commands/help.ts`, `src/commands/index.ts`,
`src/commands/dispatch.ts`, `src/cli.ts`, `src/components/intro.ts`,
`README.md`, `src/__tests__/e2e.test.ts`, `src/gateway/commands/handler.ts`.

## Agent Tools

Registered in `src/tools/registry.ts`, gated on available env vars:
`kalshi_search`, `kalshi_trade`, `portfolio_overview`, `portfolio_query`,
`portfolio_review`, `exchange_status`, `edge_query`, `risk_status`,
`scan_markets`, `octagon_report`, `web_fetch`, `web_search`.

## Environment Variables

- Kalshi: `KALSHI_API_KEY`, and `KALSHI_PRIVATE_KEY` or
  `KALSHI_PRIVATE_KEY_FILE` (RSA-PSS request signing). `KALSHI_USE_DEMO=true`
  targets the demo exchange.
- Octagon: `OCTAGON_API_KEY`, `OCTAGON_BASE_URL`, `OCTAGON_CONCURRENCY`.
- LLM: `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY`, `OLLAMA_BASE_URL`,
  `DEFAULT_MODEL`. Search: `TAVILY_API_KEY`. Telemetry: `TELEMETRY_ENABLED`.
- Loaded by `src/utils/env.ts` from `~/.kalshi-bot/.env` or a local `.env`.
  Never commit `.env` files, `*.pem` keys, or real credentials.

## Testing

- Bun's built-in runner. Unit tests are colocated in `__tests__/` as
  `*.test.ts`; integration tests are `*.itest.ts` and are excluded from
  `bun test`.
- Use `createDb(':memory:')` for database tests — never the real DB.
- Stub the network by replacing `globalThis.fetch`; restore it in `afterEach`.

## Version & Release

- SemVer, tag prefix `v`. Release script: `bash scripts/release.sh [version]`.
- Packaging is governed by `.npmignore` (there is no `files` allowlist in
  `package.json`). Verify with `npm pack --dry-run` after changing it.
- Do not push, tag, or publish without explicit confirmation.
