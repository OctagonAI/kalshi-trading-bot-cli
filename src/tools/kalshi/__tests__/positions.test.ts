import { describe, expect, test } from 'bun:test';
import { netPosition } from '../positions.js';

describe('netPosition', () => {
  test('reads the fixed-point string production sends', () => {
    expect(netPosition({ position_fp: '10.00' })).toBe(10);
    expect(netPosition({ position_fp: '-3.00' })).toBe(-3);
    expect(netPosition({ position_fp: '0.00' })).toBe(0);
  });

  test('prefers position_fp over the legacy field', () => {
    expect(netPosition({ position_fp: '8.00', position: 0 })).toBe(8);
  });

  test('falls back to the legacy integer when position_fp is absent', () => {
    expect(netPosition({ position: 5 })).toBe(5);
    expect(netPosition({ position: -2 })).toBe(-2);
  });

  test('neither field, or an unparseable one, reads as no position', () => {
    expect(netPosition({})).toBe(0);
    expect(netPosition({ position_fp: 'n/a' })).toBe(0);
  });
});
