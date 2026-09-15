import { describe, test, expect } from 'bun:test';
import {
  META_CATEGORIES,
  THEMES,
  TOP50,
  UNMAPPED_KALSHI_CATEGORIES,
  allThemeIds,
  findTheme,
  isThemeId,
  parseThemeQuery,
  themeCategoryLabels,
} from '../theme-registry.js';

/**
 * The Kalshi category vocabulary as it actually exists upstream, measured from
 * the local event index (4,000 events). Pinned here so a registry entry that
 * drifts from the data fails a test instead of silently returning zero rows.
 *
 * Note both spellings of Science & Technology: the upstream vocabulary really
 * does carry two, and the drifted one was unreachable before this registry.
 */
const REAL_KALSHI_CATEGORIES = [
  'Elections', 'Sports', 'Financials', 'Politics', 'Economics', 'Entertainment',
  'Companies', 'Crypto', 'Science and Technology', 'Climate and Weather',
  'Mentions', 'harrison-test', 'Social', 'AI', 'World', 'Business',
  'Commodities', 'Health', 'Science & Technology',
];

describe('theme registry', () => {
  test('every metaCategory is a canonical value, with exact case', () => {
    // meta_category is case-sensitive upstream: a wrong case returns zero rows
    // rather than an error, so the strings are pinned rather than normalised.
    for (const theme of THEMES) {
      expect(META_CATEGORIES).toContain(theme.metaCategory);
    }
  });

  test('every canonical meta category is reachable from some theme', () => {
    const covered = new Set(THEMES.map((t) => t.metaCategory));
    for (const meta of META_CATEGORIES) expect(covered).toContain(meta);
  });

  test('theme ids are lowercase and unique', () => {
    const ids = THEMES.map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const id of ids) expect(id).toBe(id.toLowerCase());
  });

  test('every theme claims at least one real Kalshi category', () => {
    for (const theme of THEMES) {
      expect(theme.kalshiCategories.length).toBeGreaterThan(0);
      for (const category of theme.kalshiCategories) {
        expect(REAL_KALSHI_CATEGORIES).toContain(category);
      }
    }
  });

  test('both spellings of Science & Technology are reachable', () => {
    const tech = findTheme('tech-science');
    expect(tech?.kalshiCategories).toContain('Science and Technology');
    expect(tech?.kalshiCategories).toContain('Science & Technology');
  });

  test('every real category is claimed exactly once, or named as unmapped', () => {
    const claims = new Map<string, string[]>();
    for (const theme of THEMES) {
      for (const category of theme.kalshiCategories) {
        claims.set(category, [...(claims.get(category) ?? []), theme.id]);
      }
    }
    for (const category of REAL_KALSHI_CATEGORIES) {
      if ((UNMAPPED_KALSHI_CATEGORIES as readonly string[]).includes(category)) {
        expect(claims.get(category)).toBeUndefined();
        continue;
      }
      // Exactly one: a double claim would make two themes return overlapping
      // results from the local index.
      expect(claims.get(category) ?? []).toHaveLength(1);
    }
  });

  test('transportation is dropped, not aliased', () => {
    // It mapped to a category matching 0 of 4,000 rows. Aliasing it to
    // something adjacent would silently return unrelated markets.
    expect(findTheme('transportation')).toBeUndefined();
    expect(isThemeId('transportation')).toBe(false);
  });

  test('legacy ids resolve to their canonical theme', () => {
    expect(findTheme('entertainment')?.id).toBe('culture');
    expect(findTheme('social')?.id).toBe('culture');
    expect(findTheme('companies')?.id).toBe('finance');
    expect(findTheme('financials')?.id).toBe('finance');
    expect(findTheme('science')?.id).toBe('tech-science');
    expect(findTheme('health')?.id).toBe('tech-science');
    expect(findTheme('world')?.id).toBe('politics');
  });

  test('findTheme tolerates case and surrounding whitespace', () => {
    expect(findTheme('  Crypto ')?.id).toBe('crypto');
    expect(findTheme('government shutdown')).toBeUndefined();
    expect(isThemeId(TOP50)).toBe(true);
    expect(isThemeId('bitcoin')).toBe(false);
  });

  test('allThemeIds covers canonical ids, top50 and aliases', () => {
    const ids = allThemeIds();
    expect(ids).toContain(TOP50);
    expect(ids).toContain('tech-science');
    expect(ids).toContain('commodities');
    expect(ids).toContain('entertainment');
  });

  test('themeCategoryLabels resolves aliases to their target categories', () => {
    const labels = themeCategoryLabels();
    expect(labels['science']).toEqual(labels['tech-science']!);
    expect(labels['crypto']).toEqual(['Crypto']);
  });

  test('parseThemeQuery splits theme:subtheme', () => {
    expect(parseThemeQuery('crypto:btc').theme?.id).toBe('crypto');
    expect(parseThemeQuery('crypto:btc').subtheme).toBe('btc');
    expect(parseThemeQuery('sports:baseball').theme?.id).toBe('sports');
    expect(parseThemeQuery('crypto').theme?.id).toBe('crypto');
    expect(parseThemeQuery('crypto').subtheme).toBeUndefined();
    // Free text, and an unknown theme prefix, both resolve to no theme.
    expect(parseThemeQuery('bitcoin').theme).toBeUndefined();
    expect(parseThemeQuery('nonsense:sub').theme).toBeUndefined();
  });
});
