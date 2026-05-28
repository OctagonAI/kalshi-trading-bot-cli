/**
 * Batch analyze — fan out across multiple tickers in a single Octagon call.
 *
 * The full per-ticker `analyze` does deep work (resolve market, run risk gate,
 * Kelly-size, etc.) and is slow because it makes a fresh Octagon call per
 * ticker. When the user just wants a model-edge readout across N tickers,
 * `POST /kalshi/markets/edge` returns them all in one shot — orders of
 * magnitude faster.
 *
 * This is a complement to `analyze <ticker>`, not a replacement. Use it when
 * you need:
 *   - quick edge ladder across 5-100 tickers
 *   - JSON output for downstream scripting
 *   - refresh without paying per-ticker Octagon round-trips
 */
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import { getMarketsEdge, type PerTickerEdgeRow } from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtVol(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return v.toFixed(0);
}

export interface AnalyzeBatchResult {
  run_id: string;
  captured_at: string;
  data: PerTickerEdgeRow[];
  scored: number;
  unscored: number;
}

export async function handleAnalyzeBatch(tickers: string[]): Promise<CLIResponse<AnalyzeBatchResult>> {
  const cleaned = Array.from(new Set(
    tickers.map((t) => t.trim().toUpperCase()).filter(Boolean),
  ));
  if (cleaned.length === 0) {
    return wrapError('analyze', 'NO_TICKERS', 'analyze (batch): supply 1+ tickers');
  }
  if (cleaned.length > 100) {
    return wrapError('analyze', 'TOO_MANY_TICKERS', 'analyze (batch): at most 100 tickers per call');
  }
  try {
    const resp = await getMarketsEdge({ tickers: cleaned });
    const scored = resp.data.filter((r) => r.status === 'scored').length;
    return wrapSuccess('analyze', {
      run_id: resp.run_id,
      captured_at: resp.captured_at,
      data: resp.data,
      scored,
      unscored: resp.data.length - scored,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('analyze', 'OCTAGON_ERROR', message);
  }
}

export function formatAnalyzeBatchHuman(data: AnalyzeBatchResult): string {
  const lines: string[] = [];
  lines.push(`Batch analyze — ${data.scored}/${data.data.length} scored in run ${data.run_id.slice(0, 8)} (${data.captured_at.slice(0, 16).replace('T', ' ')})`);
  lines.push('');
  if (data.data.length === 0) {
    lines.push('No tickers returned.');
    return lines.join('\n');
  }
  const rows: string[][] = data.data.map((r) => {
    const status = r.status === 'scored' ? '✓' : '—';
    const modelStr = r.model_probability == null ? '-' : `${(r.model_probability * 100).toFixed(1)}%`;
    const marketStr = r.market_probability == null ? '-' : `${(r.market_probability * 100).toFixed(1)}%`;
    const edgeStr = r.edge_pp == null ? '-' : `${r.edge_pp >= 0 ? '+' : ''}${r.edge_pp.toFixed(1)}pp`;
    const erStr = r.expected_return == null ? '-' : `${(r.expected_return * 100).toFixed(1)}%`;
    return [
      r.input_ticker,
      status,
      truncate(r.title ?? '', 32),
      r.series_category ?? '-',
      modelStr,
      marketStr,
      edgeStr,
      erStr,
      fmtVol(r.total_volume),
    ];
  });
  lines.push(formatTable(
    ['Ticker', '?', 'Title', 'Category', 'Model', 'Market', 'Edge', 'Exp Ret', 'Volume'],
    rows,
  ));
  if (data.unscored > 0) {
    lines.push('');
    lines.push(`${data.unscored} ticker(s) not scored in this Octagon run — try \`kalshi analyze <ticker> --refresh\` for a one-off deep call.`);
  }
  return lines.join('\n');
}
