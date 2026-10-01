import { describe, test, expect, beforeEach, afterEach, mock, setSystemTime } from 'bun:test';
import type { ParsedArgs } from '../parse-args.js';
import type { OctagonEventEntry } from '../../scan/octagon-events-api.js';
import { handleEvents, formatEventsHuman } from '../events.js';

function makeArgs(o: Partial<ParsedArgs>): ParsedArgs {
  return {
    subcommand: 'events',
    positionalArgs: [],
    json: false,
    live: false, refresh: false, report: false, dryRun: false, verbose: false,
    performance: false, resolved: false, unresolved: false,
    behavioral: false, ranked: false, showCluster: false, activeOnly: false,
    cells: false, autoProbs: false,
    parseErrors: [],
    ...o,
  };
}

/** An /events list row; `events <ticker>` resolves through the list scan, so detail uses it too. */
function makeEvent(overrides: Partial<OctagonEventEntry> = {}): OctagonEventEntry {
  return {
    history_id: 41,
    run_id: '1a9984cc-17b8-4d59-936b-ebaa0d0da5c5',
    captured_at: '2026-09-23T17:53:00Z',
    event_ticker: 'KXFEDDECISION-26OCT',
    name: 'Fed decision in October?',
    slug: 'kxfeddecision-26oct',
    series_category: 'Economics',
    available_on_brokers: true,
    mutually_exclusive: true,
    analysis_last_updated: '2026-09-23T17:40:00Z',
    confidence_score: 7,
    model_probability: 62,
    market_probability: 55,
    edge_pp: 7,
    expected_return: 0.12,
    r_score: 1.4,
    total_volume: 2_500_000,
    total_open_interest: 900_000,
    close_time: '2026-10-29T18:00:00Z',
    key_takeaway: 'A 25bp cut is the base case; a hold needs a hot CPI print.',
    outcome_probabilities: [
      { market_ticker: 'KXFEDDECISION-26OCT-C25', outcome_name: '25 bps decrease', model_probability: 62, market_probability: 55, volume_24h: 80_000 },
      { market_ticker: 'KXFEDDECISION-26OCT-H0', outcome_name: 'No change', model_probability: 35, market_probability: 42, volume_24h: 60_000 },
    ],
    ...overrides,
  };
}

describe('events — snapshot time', () => {
  let originalFetch: typeof globalThis.fetch;
  // A week after the fixtures' captured_at of 2026-09-23 17:53 UTC.
  beforeEach(() => {
    setSystemTime(new Date('2026-09-30T17:53:00Z'));
    process.env.OCTAGON_API_KEY = 'sk_test';
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    setSystemTime();
    globalThis.fetch = originalFetch;
    delete process.env.OCTAGON_API_KEY;
  });

  test('detail shows when the numbers were captured, right under the header', async () => {
    globalThis.fetch = mock(async () => new Response(JSON.stringify({
      data: [makeEvent()], next_cursor: null, has_more: false,
    }), { status: 200, headers: { 'Content-Type': 'application/json' } })) as unknown as typeof fetch;
    const resp = await handleEvents(makeArgs({ positionalArgs: ['kxfeddecision-26oct'] }));
    expect(resp.ok).toBe(true);
    if (!resp.ok) return;
    const lines = formatEventsHuman(resp.data).split('\n');
    expect(lines[0]).toBe('Event KXFEDDECISION-26OCT — Fed decision in October?');
    expect(lines[1]).toBe('  Snapshot   2026-09-23 17:53 UTC (7d ago)');
    expect(formatEventsHuman(resp.data)).toContain('A 25bp cut is the base case; a hold needs a hot CPI print.');
  });

  test('detail names the analysis date only when it predates the capture by over a day', () => {
    const carried = formatEventsHuman({ kind: 'detail', event: makeEvent({ analysis_last_updated: '2026-09-02T09:15:00Z' }) });
    expect(carried).toContain('  Snapshot   2026-09-23 17:53 UTC (7d ago) · analysis from 2026-09-02');

    const fresh = formatEventsHuman({ kind: 'detail', event: makeEvent({ analysis_last_updated: '2026-09-22T20:00:00Z' }) });
    expect(fresh).not.toContain('analysis from');
  });

  test('a zone-less captured_at is read as UTC', () => {
    const out = formatEventsHuman({ kind: 'detail', event: makeEvent({ captured_at: '2026-09-23T17:53:00' }) });
    expect(out).toContain('  Snapshot   2026-09-23 17:53 UTC (7d ago)');
  });

  test('an unparseable captured_at drops the line instead of crashing', () => {
    const out = formatEventsHuman({ kind: 'detail', event: makeEvent({ captured_at: 'not-a-date' }) });
    expect(out).not.toContain('Snapshot');
    expect(out).toContain('Model      62.0%');
  });

  test('list has a Captured column with the compact age', () => {
    const data = [
      makeEvent({ event_ticker: 'KXFRESH-26', captured_at: '2026-09-30T14:53:00Z' }),
      makeEvent({ event_ticker: 'KXSTALE-26' }),
      makeEvent({ event_ticker: 'KXUNDATED-26', captured_at: '' }),
    ];
    const out = formatEventsHuman({ kind: 'list', data, total_returned: data.length });
    expect(out).toMatch(/Captured/);
    expect(out).toMatch(/KXFRESH-26 .*│ 3h +│/);
    expect(out).toMatch(/KXSTALE-26 .*│ 7d +│/);
    expect(out).toMatch(/KXUNDATED-26 .*│ - +│/);
  });
});
