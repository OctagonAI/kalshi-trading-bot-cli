/**
 * Octagon-powered search formatters that back the extended /search and
 * /search edge code paths. Used by dispatch.ts and index.ts when
 * OCTAGON_API_KEY is set; the legacy local-SQLite paths remain as fallback.
 */
import { formatTable } from './scan-formatters.js';
import type { KalshiMarketRow, PagedResult, MarketsWithEdgeResponse, EventSearchRow } from '../scan/octagon-kalshi-api.js';

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

function fmtCloseDate(iso: string | null): string {
  if (!iso) return '-';
  if (Number.isNaN(new Date(iso).getTime())) return '-';
  // Slice the original string rather than re-serialising: toISOString() shifts
  // the displayed day for any offset-bearing timestamp.
  return iso.slice(0, 10);
}

/**
 * Event-level search results.
 *
 * `describe` names what was searched (a theme id, or the raw query) so an empty
 * result says which question returned nothing rather than printing a bare table.
 *
 * Note the column semantics: each row is one event represented by its
 * best-matching market, so Last and 24h Vol belong to that market, not to the
 * event as a whole. The endpoint exposes no market count, so there is no Mkts
 * column.
 */
export function formatEventSearchHuman(describe: string, page: PagedResult<EventSearchRow>): string {
  const lines: string[] = [];
  const more = page.has_more ? ' (more available)' : '';
  lines.push(`Events matching ${describe} — ${page.data.length} shown${more}`);
  lines.push('');

  if (page.data.length === 0) {
    lines.push(`No events found for ${describe}.`);
    return lines.join('\n');
  }

  const rows: string[][] = page.data.map((e) => [
    eventIdOf(e),
    truncate(e.title ?? '-', 44),
    fmtMoney(e.last_price),
    fmtVol(e.volume_24h),
    e.category ?? '-',
    fmtCloseDate(e.close_time ?? null),
  ]);
  lines.push(formatTable(
    ['Event', 'Title', 'Last*', '24h Vol*', 'Category', 'Closes'],
    rows,
  ));
  lines.push('');
  lines.push('* of the event\'s best-matching market, not an event total.');
  lines.push(`Drill into one event: search ${eventIdOf(page.data[0])}`);
  return lines.join('\n');
}

/** The venue-native identifier; `event_ticker` is venue-prefixed. */
export function eventIdOf(e: EventSearchRow): string {
  return e.native_event_ticker ?? e.event_ticker;
}

export function formatMarketSearchHuman(query: string, page: PagedResult<KalshiMarketRow>): string {
  const lines: string[] = [];
  const more = page.has_more ? ' (more available)' : '';
  lines.push(`Markets matching "${query}" — ${page.data.length} shown${more}`);
  lines.push('');

  if (page.data.length === 0) {
    lines.push('No markets found.');
    return lines.join('\n');
  }

  const rows: string[][] = page.data.map((m) => [
    m.market_ticker,
    truncate(m.title, 40),
    fmtMoney(m.last_price ?? m.yes_ask),
    fmtVol(m.volume_24h),
    m.category ?? '-',
    fmtCloseDate(m.close_time),
  ]);
  lines.push(formatTable(['Ticker', 'Title', 'Last', '24h Vol', 'Category', 'Closes'], rows));
  return lines.join('\n');
}

export function formatMarketsWithEdgeHuman(data: MarketsWithEdgeResponse, minEdgePp: number): string {
  const lines: string[] = [];
  // Guard against invalid date strings — new Date('garbage').toISOString() throws RangeError.
  let captured = 'unknown';
  if (data.captured_at) {
    const d = new Date(data.captured_at);
    if (!Number.isNaN(d.getTime())) {
      captured = d.toISOString().slice(0, 16).replace('T', ' ');
    }
  }
  lines.push(`Octagon Edge Scanner (server-side) — run ${data.run_id.slice(0, 8)}, captured ${captured} UTC, sort by ${data.sort_by}`);
  lines.push('════════════════════════════════════════════════════════');
  lines.push('');

  if (data.data.length === 0) {
    lines.push(`  No events with |edge| ≥ ${minEdgePp}pp found.`);
    return lines.join('\n');
  }

  const rows: string[][] = data.data.map((r, i) => [
    String(i + 1),
    r.market_ticker || r.event_ticker,
    truncate(r.title, 35),
    `${r.model_probability.toFixed(1)}%`,
    `${r.market_probability.toFixed(1)}%`,
    `${r.edge_pp >= 0 ? '+' : ''}${r.edge_pp.toFixed(1)}pp`,
    `${(r.expected_return * 100).toFixed(1)}%`,
    fmtVol(r.total_volume),
    r.series_category ?? '-',
  ]);
  lines.push(formatTable(
    ['#', 'Ticker', 'Title', 'Model', 'Market', 'Edge', 'Exp Ret', 'Volume', 'Category'],
    rows,
  ));
  lines.push('');
  lines.push(`${data.data.length} event(s) returned${data.has_more ? ' (more available)' : ''}.`);
  return lines.join('\n');
}
