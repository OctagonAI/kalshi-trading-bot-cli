import { describe, test, expect } from 'bun:test';
import { parseArgs } from '../parse-args.js';

describe('parseArgs — integer-only flag validation', () => {
  test('--top-k rejects decimal', () => {
    const r = parseArgs(['similar', 'KX-A', '--top-k', '2.5']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid --top-k value');
    expect(r.topK).toBeUndefined();
  });

  test('--top-k accepts integer', () => {
    const r = parseArgs(['similar', 'KX-A', '--top-k', '25']);
    expect(r.parseErrors).toEqual([]);
    expect(r.topK).toBe(25);
  });

  test('--window-days rejects decimal', () => {
    const r = parseArgs(['correlate', 'KX-A', 'KX-B', '--window-days', '30.5']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid --window-days value');
  });

  test('-n rejects decimal', () => {
    const r = parseArgs(['basket', 'build', '-n', '5.5']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid -n value');
  });

  test('--max-per-cluster rejects decimal', () => {
    const r = parseArgs(['basket', 'build', '--max-per-cluster', '2.5']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid --max-per-cluster value');
  });

  test('integer flags reject zero and negatives', () => {
    expect(parseArgs(['similar', '--top-k', '0']).parseErrors.length).toBe(1);
    expect(parseArgs(['similar', '--top-k', '-5']).parseErrors.length).toBe(1);
    expect(parseArgs(['correlate', 'A', 'B', '--window-days', '-1']).parseErrors.length).toBe(1);
  });
});

describe('parseArgs — --days-to-close', () => {
  test('accepts positive integer', () => {
    const r = parseArgs(['search', 'bitcoin', '--days-to-close', '14']);
    expect(r.parseErrors).toEqual([]);
    expect(r.daysToClose).toBe(14);
  });

  test('rejects decimals', () => {
    const r = parseArgs(['search', '--days-to-close', '14.5']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid --days-to-close');
  });

  test('rejects zero and negative', () => {
    expect(parseArgs(['search', '--days-to-close', '0']).parseErrors.length).toBe(1);
    expect(parseArgs(['search', '--days-to-close', '-7']).parseErrors.length).toBe(1);
  });

  test('--max-dte is an alias', () => {
    const r = parseArgs(['search', 'bitcoin', '--max-dte', '7']);
    expect(r.parseErrors).toEqual([]);
    expect(r.daysToClose).toBe(7);
  });
});

describe('parseArgs — sortBy validation', () => {
  test('accepts each valid sortBy', () => {
    for (const val of ['edge_pp', 'expected_return', 'total_volume', 'model_probability', 'volume_24h', 'close_time', 'last_price']) {
      const r = parseArgs(['search', '--sort-by', val]);
      expect(r.parseErrors).toEqual([]);
      expect(r.sortBy).toBe(val);
    }
  });

  test('rejects unknown sortBy value', () => {
    const r = parseArgs(['search', '--sort-by', 'random_key']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('Invalid --sort-by value');
    expect(r.parseErrors[0]).toContain('random_key');
    expect(r.sortBy).toBeUndefined();
  });

  test('--sort-by without a value', () => {
    const r = parseArgs(['search', '--sort-by']);
    expect(r.parseErrors.length).toBe(1);
    expect(r.parseErrors[0]).toContain('--sort-by requires a value');
  });
});
