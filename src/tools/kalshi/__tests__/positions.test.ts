import { describe, expect, test } from 'bun:test';
import { heldPosition, netPosition } from '../positions.js';

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

describe('heldPosition', () => {
  test('keeps fractional sizes instead of rounding them away', () => {
    expect(heldPosition({ position_fp: '0.40' })).toEqual({ direction: 'yes', size: 0.4 });
    expect(heldPosition({ position_fp: '-0.60' })).toEqual({ direction: 'no', size: 0.6 });
    expect(heldPosition({ position_fp: '2.50' })).toEqual({ direction: 'yes', size: 2.5 });
  });

  test('whole positions read as before', () => {
    expect(heldPosition({ position_fp: '3.00' })).toEqual({ direction: 'yes', size: 3 });
    expect(heldPosition({ position: -2 })).toEqual({ direction: 'no', size: 2 });
  });

  test('a flat market holds nothing', () => {
    expect(heldPosition({ position_fp: '0.00' })).toBeNull();
    expect(heldPosition({})).toBeNull();
  });
});
