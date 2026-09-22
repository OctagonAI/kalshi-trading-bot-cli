import { describe, test, expect } from 'bun:test';
import { stripVTControlCharacters } from 'node:util';
import { formatTable } from '../scan-formatters.js';

describe('formatTable', () => {
  test('ANSI-colored cells do not skew column alignment', () => {
    const green = (s: string) => `[32m${s}[39m`;
    const out = formatTable(['Name', 'Score'], [
      ['alpha', green(' 77')],
      ['beta', '  —'],
    ]);
    const lines = stripVTControlCharacters(out).split('\n');
    // Every line (borders, header, rows) has the same visible width
    const widths = new Set(lines.map((l) => l.length));
    expect(widths.size).toBe(1);
    // Colors survive — only the measurement ignores them
    expect(out).toContain(green(' 77'));
  });
});
