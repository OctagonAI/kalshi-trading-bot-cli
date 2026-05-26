import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import { findSimilarMarkets, type SimilarResponse, type SimilarMarketRow } from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtMoney(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  return `$${v.toFixed(2)}`;
}

function fmtVol(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return v.toFixed(0);
}

function looksLikeTicker(s: string): boolean {
  // Kalshi tickers are uppercase with hyphens, digits, no spaces. Anything with
  // a space or lowercase letter is treated as a free-text query.
  return /^[A-Z0-9._-]+$/i.test(s) && /[A-Z]/i.test(s) && s.includes('-');
}

export async function handleSimilar(args: ParsedArgs): Promise<CLIResponse<SimilarResponse>> {
  const positional = args.positionalArgs.join(' ').trim();
  let anchorTicker = args.ticker;
  let q = args.query;

  if (!anchorTicker && !q && positional) {
    // Single-token uppercase-ish string with a hyphen → treat as ticker, else as query.
    if (looksLikeTicker(positional)) {
      anchorTicker = positional.toUpperCase();
    } else {
      q = positional;
    }
  }

  if (!anchorTicker && !q) {
    return wrapError('similar', 'MISSING_ANCHOR', 'Usage: similar <ticker> | similar -q "query text" [--top-k N] [--category C] [--min-volume N] [--close-before ISO]');
  }
  if (anchorTicker && q) {
    return wrapError('similar', 'AMBIGUOUS_ANCHOR', 'Pass either a ticker or -q "query", not both.');
  }

  try {
    const data = await findSimilarMarkets({
      anchor_ticker: anchorTicker,
      q,
      top_k: args.topK,
      category: args.category,
      min_volume_24h: args.minVolume,
      close_before: args.closeBefore,
    });
    return wrapSuccess('similar', data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('similar', 'OCTAGON_ERROR', message);
  }
}

export function formatSimilarHuman(data: SimilarResponse): string {
  const lines: string[] = [];
  const anchor = data.anchor_ticker
    ? `ticker ${data.anchor_ticker}`
    : data.anchor_query
      ? `query "${data.anchor_query}"`
      : 'unknown anchor';
  lines.push(`Markets similar to ${anchor} — ${data.data.length} result(s)`);
  lines.push('');

  if (data.data.length === 0) {
    lines.push('No similar markets found.');
    return lines.join('\n');
  }

  const rows: string[][] = data.data.map((m: SimilarMarketRow) => [
    m.market_ticker,
    truncate(m.title, 40),
    m.distance.toFixed(3),
    fmtMoney(m.last_price ?? m.yes_ask),
    fmtVol(m.volume_24h),
    m.category ?? '-',
  ]);

  lines.push(formatTable(
    ['Ticker', 'Title', 'Distance', 'Last', '24h Vol', 'Category'],
    rows,
  ));
  lines.push('');
  lines.push('Lower distance = closer cosine similarity.');
  return lines.join('\n');
}
