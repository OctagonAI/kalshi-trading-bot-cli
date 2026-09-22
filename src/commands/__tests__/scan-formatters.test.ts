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

  test('a cell containing a newline does not split the row', () => {
    // Live Octagon titles carry these: " XRP price at Sep 15, 2026 at 5pm EDT?\n".
    // Padding was computed on the full string, so the row broke mid-cell and
    // every border below it came out ragged.
    const out = formatTable(['Event', 'Title'], [
      ['KXXRPD-26SEP1517', ' XRP price at Sep 15, 2026 at 5pm EDT?\n'],
      ['KXDOGED-26SEP1517', 'Dogecoin price  on Sep 15, 2026?'],
    ]);
    const lines = out.split('\n');
    // top border, header, mid border, two data rows, bottom border
    expect(lines.length).toBe(6);
    const widths = new Set(lines.map((l) => l.length));
    expect(widths.size).toBe(1);
    // Doubled spaces collapse too, so the column is sized to what is displayed
    expect(out).toContain('Dogecoin price on Sep 15, 2026?');
  });

  test('a table wider than the budget is shrunk to fit', () => {
    const out = formatTable(
      ['Event', 'Title', 'Last'],
      [['KXBTCD-26SEP1517', 'BTC price on Sep 15, 2026 at 5pm EDT?', '$0.17']],
      40,
    );
    for (const line of out.split('\n')) {
      expect(line.length).toBeLessThanOrEqual(40);
    }
    // The widest column absorbs the loss; the narrow ones stay readable
    expect(out).toContain('KXBTCD');
    expect(out).toContain('$0.17');
    expect(out).toContain('…');
  });

  test('no budget leaves output at natural width', () => {
    const rows = [['alpha', 'a fairly long descriptive title here']];
    const wide = formatTable(['A', 'B'], rows, Infinity);
    const lines = wide.split('\n');
    expect(lines[0].length).toBe(5 + 36 + 3 * 2 + 1);
    expect(wide).not.toContain('…');
  });

  test('shrinking never slices an ANSI escape in half', () => {
    const green = (s: string) => `\x1b[32m${s}\x1b[39m`;
    const out = formatTable(['A', 'B'], [[green('a very long coloured cell that must shrink'), 'x']], 30);
    // Whatever survives, the colour is opened and closed — never a bare fragment
    expect(out).toContain('\x1b[32m');
    expect(stripVTControlCharacters(out).split('\n').every((l) => l.length <= 30)).toBe(true);
  });
});
