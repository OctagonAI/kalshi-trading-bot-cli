import { describe, expect, test } from 'bun:test';
import { analysisPredatesCapture, parseUtcTimestamp } from '../time.js';

describe('parseUtcTimestamp', () => {
  const iso = (v: string) => parseUtcTimestamp(v)?.toISOString();

  test('reads a zone-less timestamp as UTC, not local time', () => {
    expect(iso('2026-09-23T17:53:00')).toBe('2026-09-23T17:53:00.000Z');
    expect(iso('2026-09-23 17:53:00')).toBe('2026-09-23T17:53:00.000Z');
  });

  test('honours an explicit zone in every common spelling', () => {
    expect(iso('2026-09-23T17:53:00Z')).toBe('2026-09-23T17:53:00.000Z');
    expect(iso('2026-09-23T17:53:00+00:00')).toBe('2026-09-23T17:53:00.000Z');
    expect(iso('2026-09-23T12:53:00-0500')).toBe('2026-09-23T17:53:00.000Z');
    expect(iso('2026-09-23T12:53:00-05')).toBe('2026-09-23T17:53:00.000Z');
    expect(iso('2026-09-23 17:53:00 +00:00')).toBe('2026-09-23T17:53:00.000Z');
  });

  test('accepts the microseconds Python isoformat() emits', () => {
    expect(iso('2026-09-23T17:53:00.123456')).toBe('2026-09-23T17:53:00.123Z');
  });

  test('a date-only value is that UTC midnight, not a "-02" offset', () => {
    expect(iso('2026-09-02')).toBe('2026-09-02T00:00:00.000Z');
  });

  test('missing or unparseable → null', () => {
    expect(parseUtcTimestamp(null)).toBeNull();
    expect(parseUtcTimestamp('')).toBeNull();
    expect(parseUtcTimestamp('not-a-date')).toBeNull();
  });
});

describe('analysisPredatesCapture', () => {
  test('true only past a day of drift', () => {
    expect(analysisPredatesCapture('2026-09-23T17:53:00Z', '2026-09-02T09:15:00Z')).toBe(true);
    expect(analysisPredatesCapture('2026-09-23T17:53:00Z', '2026-09-23T17:40:00Z')).toBe(false);
    expect(analysisPredatesCapture('2026-09-23T17:53:00Z', '2026-09-22T17:53:00Z')).toBe(false);
  });

  test('missing or unparseable stamps make no claim', () => {
    expect(analysisPredatesCapture('2026-09-23T17:53:00Z', null)).toBe(false);
    expect(analysisPredatesCapture('', '2026-09-02T09:15:00Z')).toBe(false);
    expect(analysisPredatesCapture('2026-09-23T17:53:00Z', 'garbage')).toBe(false);
  });
});
