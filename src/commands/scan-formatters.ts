import { stripVTControlCharacters } from 'node:util';
import type { ScanResult } from '../scan/loop.js';
import type { EdgeSnapshot } from '../scan/types.js';
import type { EdgeRow } from '../db/edge.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

export function formatTable(headers: string[], rows: string[][]): string {
  // Measure visible width so ANSI-colored cells don't skew the columns.
  const width = (s: string) => stripVTControlCharacters(s).length;
  const colWidths = headers.map((h, i) =>
    Math.max(width(h), ...rows.map((r) => width(r[i] ?? '')))
  );

  const pad = (s: string, w: number) => s + ' '.repeat(w - width(s));
  const sep = '─';

  const topBorder = '┌' + colWidths.map((w) => sep.repeat(w + 2)).join('┬') + '┐';
  const headerRow = '│' + headers.map((h, i) => ` ${pad(h, colWidths[i])} `).join('│') + '│';
  const midBorder = '├' + colWidths.map((w) => sep.repeat(w + 2)).join('┼') + '┤';
  const bottomBorder = '└' + colWidths.map((w) => sep.repeat(w + 2)).join('┴') + '┘';

  const dataRows = rows.map(
    (row) => '│' + colWidths.map((w, i) => ` ${pad(row[i] ?? '', w)} `).join('│') + '│'
  );

  return [topBorder, headerRow, midBorder, ...dataRows, bottomBorder].join('\n');
}

function fmtEdge(edge: number): string {
  const pct = (edge * 100).toFixed(1);
  return edge >= 0 ? `+${pct}%` : `${pct}%`;
}

function fmtProb(prob: number): string {
  return `${(prob * 100).toFixed(1)}%`;
}

function fmtTimestamp(epoch: number): string {
  const d = new Date(epoch * 1000);
  const now = Date.now();
  const diffMs = now - d.getTime();
  const diffMin = Math.floor(diffMs / 60_000);

  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

export function formatScanTable(result: ScanResult): string {
  const lines: string[] = [];

  if (result.edgeSnapshots.length === 0) {
    lines.push('No edges found in this scan.');
  } else {
    const rows = result.edgeSnapshots.map((s) => [
      s.ticker,
      fmtProb(s.modelProb),
      fmtProb(s.marketProb),
      fmtEdge(s.edge),
      s.confidence,
      truncate(s.drivers[0]?.claim ?? '-', 40),
    ]);

    lines.push(formatTable(
      ['Ticker', 'Model%', 'Market%', 'Edge', 'Confidence', 'Top Driver'],
      rows
    ));
  }

  const actionable = result.edgeSnapshots.filter(
    (s) => s.confidence === 'high' || s.confidence === 'very_high'
  ).length;
  const secs = (result.duration / 1000).toFixed(1);
  lines.push('');
  lines.push(`Scanned ${result.eventsScanned} events, found ${actionable} actionable edges in ${secs}s`);

  return lines.join('\n');
}

export function formatEdgeTable(rows: EdgeRow[]): string {
  if (rows.length === 0) return 'No edges found.';

  const tableRows = rows.map((r) => [
    r.ticker,
    fmtProb(r.model_prob),
    fmtProb(r.market_prob),
    fmtEdge(r.edge),
    r.confidence ?? '-',
    fmtTimestamp(r.timestamp),
  ]);

  return formatTable(
    ['Ticker', 'Model%', 'Market%', 'Edge', 'Confidence', 'Timestamp'],
    tableRows
  );
}
