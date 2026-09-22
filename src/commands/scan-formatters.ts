import { stripVTControlCharacters } from 'node:util';
import type { ScanResult } from '../scan/loop.js';
import type { EdgeSnapshot } from '../scan/types.js';
import type { EdgeRow } from '../db/edge.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

/** Visible width, ignoring ANSI colouring. */
const visibleWidth = (s: string) => stripVTControlCharacters(s).length;

/**
 * Truncate to `max` visible columns, passing ANSI escapes through untouched so
 * a colour code is never sliced in half.
 */
function truncateVisible(s: string, max: number): string {
  if (visibleWidth(s) <= max) return s;
  const token = /(\[[0-9;]*m)|([\s\S])/g;
  let out = '';
  let seen = 0;
  let coloured = false;
  let m: RegExpExecArray | null;
  while ((m = token.exec(s)) !== null) {
    if (m[1]) {
      out += m[1];
      coloured = true;
      continue;
    }
    if (seen >= max - 1) break;
    out += m[2];
    seen++;
  }
  return out + '…' + (coloured ? '[0m' : '');
}

/**
 * Width budget for a table. Only interactive output is constrained: piped
 * output, `--json` and tests must stay byte-identical to what they printed
 * before this became width-aware.
 */
function terminalBudget(): number {
  if (!process.stdout.isTTY) return Infinity;
  return Math.max(40, process.stdout.columns || 80);
}

/**
 * Render a bordered table.
 *
 * Cells are whitespace-normalised first: some upstream titles carry embedded
 * newlines (`" XRP price at Sep 15, 2026 at 5pm EDT?\n"`), which would
 * otherwise split a row across lines and break every border below it.
 *
 * When the natural table is wider than the terminal, the widest text columns
 * shrink until it fits — wrapping mangles the borders far worse than an
 * ellipsis does.
 */
export function formatTable(headers: string[], rows: string[][], maxWidth?: number): string {
  const clean = (s: string) => s.replace(/\s+/g, ' ').trim();
  const heads = headers.map(clean);
  const cells = rows.map((r) => heads.map((_, i) => clean(r[i] ?? '')));

  const colWidths = heads.map((h, i) =>
    Math.max(visibleWidth(h), ...cells.map((r) => visibleWidth(r[i] ?? '')))
  );

  // Each column costs its content plus two padding spaces and a border; one
  // extra border closes the row. Verified against a live 6-column table:
  // 99 content + (3 × 6 + 1) = 118 columns.
  const chrome = colWidths.length * 3 + 1;
  const budget = maxWidth ?? terminalBudget();
  // Prefer 8, but let a narrow terminal squeeze wide tables further; a fixed
  // floor meant six columns could never fit under 67.
  const MIN_COL = Math.max(3, Math.min(8, Math.floor((budget - chrome) / colWidths.length)));
  let total = colWidths.reduce((a, b) => a + b, 0) + chrome;
  while (total > budget) {
    let widest = 0;
    for (let i = 1; i < colWidths.length; i++) {
      if (colWidths[i] > colWidths[widest]) widest = i;
    }
    if (colWidths[widest] <= MIN_COL) break;
    colWidths[widest] = Math.max(MIN_COL, colWidths[widest] - (total - budget));
    total = colWidths.reduce((a, b) => a + b, 0) + chrome;
  }

  const fit = (s: string, w: number) => {
    const t = truncateVisible(s, w);
    return t + ' '.repeat(Math.max(0, w - visibleWidth(t)));
  };
  const sep = '─';

  const topBorder = '┌' + colWidths.map((w) => sep.repeat(w + 2)).join('┬') + '┐';
  const headerRow = '│' + heads.map((h, i) => ` ${fit(h, colWidths[i])} `).join('│') + '│';
  const midBorder = '├' + colWidths.map((w) => sep.repeat(w + 2)).join('┼') + '┤';
  const bottomBorder = '└' + colWidths.map((w) => sep.repeat(w + 2)).join('┴') + '┘';

  const dataRows = cells.map(
    (row) => '│' + colWidths.map((w, i) => ` ${fit(row[i] ?? '', w)} `).join('│') + '│'
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
