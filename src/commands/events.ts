import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  fetchOctagonEventsPage,
  fetchOctagonEventByTicker,
  type OctagonEventEntry,
} from '../scan/octagon-events-api.js';
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

export type EventsResult =
  | { kind: 'list'; data: OctagonEventEntry[]; total_returned: number; filtered_from?: number }
  | { kind: 'detail'; event: OctagonEventEntry };

export async function handleEvents(args: ParsedArgs): Promise<CLIResponse<EventsResult>> {
  const positional = args.positionalArgs[0];

  try {
    // events <event_ticker> — drill into one
    if (positional && positional.toUpperCase().startsWith('KX')) {
      const ev = await fetchOctagonEventByTicker(positional.toUpperCase());
      if (!ev) {
        return wrapError('events', 'EVENT_NOT_FOUND', `No event found for ticker ${positional}`);
      }
      return wrapSuccess('events', { kind: 'detail', event: ev });
    }

    // events list — filter + sort
    const wantLimit = args.limit ?? 50;
    const all: OctagonEventEntry[] = [];
    let cursor: string | null = null;
    // Cap pages defensively (universe is ~hundreds; this is paranoid)
    for (let i = 0; i < 25; i++) {
      const page: { data: OctagonEventEntry[]; next_cursor: string | null; has_more: boolean } =
        await fetchOctagonEventsPage({ cursor });
      all.push(...page.data);
      if (!page.has_more) break;
      cursor = page.next_cursor;
      if (!cursor) break;
    }

    let filtered = all;
    if (args.category) {
      const cat = args.category.toLowerCase();
      filtered = filtered.filter((e) => (e.series_category ?? '').toLowerCase().includes(cat));
    }
    if (args.minVolume !== undefined) {
      const floor = args.minVolume;
      filtered = filtered.filter((e) => (e.total_volume ?? 0) >= floor);
    }

    // Default sort: descending by total_volume
    filtered.sort((a, b) => (b.total_volume ?? 0) - (a.total_volume ?? 0));

    return wrapSuccess('events', {
      kind: 'list',
      data: filtered.slice(0, wantLimit),
      total_returned: Math.min(filtered.length, wantLimit),
      filtered_from: all.length !== filtered.length ? all.length : undefined,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('events', 'OCTAGON_ERROR', message);
  }
}

export function formatEventsHuman(result: EventsResult): string {
  if (result.kind === 'detail') return formatEventDetail(result.event);
  return formatEventList(result.data, result.filtered_from);
}

function formatEventList(events: OctagonEventEntry[], filteredFrom?: number): string {
  const lines: string[] = [];
  const fromLabel = filteredFrom != null ? ` (filtered from ${filteredFrom})` : '';
  lines.push(`Octagon events — ${events.length} shown${fromLabel}, sorted by total_volume desc`);
  lines.push('');
  if (events.length === 0) {
    lines.push('No events match.');
    return lines.join('\n');
  }
  const rows: string[][] = events.map((e) => [
    e.event_ticker,
    truncate(e.name ?? '', 40),
    e.series_category ?? '-',
    `${e.model_probability.toFixed(1)}%`,
    `${e.market_probability.toFixed(1)}%`,
    `${e.edge_pp >= 0 ? '+' : ''}${e.edge_pp.toFixed(1)}pp`,
    fmtVol(e.total_volume),
    (e.close_time ?? '').slice(0, 10),
  ]);
  lines.push(formatTable(
    ['Event', 'Name', 'Category', 'Model', 'Market', 'Edge', 'Volume', 'Closes'],
    rows,
  ));
  return lines.join('\n');
}

function formatEventDetail(e: OctagonEventEntry): string {
  const lines: string[] = [];
  lines.push(`Event ${e.event_ticker} — ${e.name}`);
  lines.push(`  Category   ${e.series_category}`);
  lines.push(`  Model      ${e.model_probability.toFixed(1)}%`);
  lines.push(`  Market     ${e.market_probability.toFixed(1)}%`);
  lines.push(`  Edge       ${e.edge_pp >= 0 ? '+' : ''}${e.edge_pp.toFixed(1)}pp  (confidence ${e.confidence_score.toFixed(1)}/10)`);
  lines.push(`  Volume     ${fmtVol(e.total_volume)}  open interest ${fmtVol(e.total_open_interest)}`);
  lines.push(`  Closes     ${e.close_time ?? '-'}`);
  if (e.key_takeaway) {
    lines.push('');
    lines.push(`  ${e.key_takeaway}`);
  }
  const outcomes = e.outcome_probabilities ?? [];
  if (outcomes.length > 0) {
    lines.push('');
    lines.push('Sub-markets (outcome probabilities):');
    const rows: string[][] = outcomes.map((o) => [
      o.market_ticker,
      truncate(o.outcome_name ?? '-', 35),
      `${o.model_probability.toFixed(1)}%`,
      `${o.market_probability.toFixed(1)}%`,
      `${(o.model_probability - o.market_probability) >= 0 ? '+' : ''}${(o.model_probability - o.market_probability).toFixed(1)}pp`,
      fmtVol(o.volume_24h ?? o.volume),
    ]);
    lines.push(formatTable(
      ['Market', 'Outcome', 'Model', 'Market', 'Edge', '24h Vol'],
      rows,
    ));
  }
  return lines.join('\n');
}
