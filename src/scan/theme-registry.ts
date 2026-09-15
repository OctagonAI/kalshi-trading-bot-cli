/**
 * Theme registry — one vocabulary, three backends.
 *
 * A theme is a word the user types (`search crypto`, `scan --theme crypto`).
 * Resolving it needs three different vocabularies, which genuinely differ:
 *
 *   - `metaCategory`     Octagon's cross-venue taxonomy. Sent to
 *                        /v1/predictions/markets/events/search. CASE-SENSITIVE:
 *                        `meta_category=crypto` returns zero rows rather than an
 *                        error, so the value must always come from this table
 *                        and never from raw user input.
 *   - `kalshiCategories` Exact Kalshi category labels, matched against
 *                        event_index.category for the local paths (scan, watch,
 *                        TUI browse). An array because the upstream vocabulary
 *                        has drifted: "Science and Technology" and
 *                        "Science & Technology" are both live.
 *   - `tags`             Kalshi series tags, matched as whole comma-delimited
 *                        tokens against event_index.tags. Powers `theme:subtheme`
 *                        and rescues rows whose category is unhelpful.
 *
 * This module imports nothing on purpose. controllers/browse.ts previously
 * inlined a copy of the category map "to avoid heavy theme-resolver import" —
 * that concern is real (theme-resolver pulls in bun:sqlite, the Kalshi client
 * and the db layer), and a leaf module removes it by construction.
 */

export const META_CATEGORIES = [
  'Politics',
  'Elections',
  'Economics',
  'Finance',
  'Crypto',
  'Commodities',
  'Sports',
  'Culture',
  'Tech & Science',
  'Climate',
  'Mentions',
] as const;

export type MetaCategory = (typeof META_CATEGORIES)[number];

export interface Theme {
  /** Lowercase, user-facing. What someone types after `search`. */
  id: string;
  /** Octagon's cross-venue taxonomy. EXACT case — see the header note. */
  metaCategory: MetaCategory;
  /** Exact Kalshi category labels for the local index. */
  kalshiCategories: string[];
  /** Kalshi series tag labels for the local index. */
  tags: string[];
}

/** Special theme handled by ThemeResolver, not by category lookup. */
export const TOP50 = 'top50';

/**
 * Real Kalshi categories deliberately left out of the vocabulary.
 * `harrison-test` is upstream test data. Rows with a NULL category are
 * unreachable by any category filter and are found by free text only.
 */
export const UNMAPPED_KALSHI_CATEGORIES = ['harrison-test'] as const;

export const THEMES: Theme[] = [
  {
    id: 'politics',
    metaCategory: 'Politics',
    kalshiCategories: ['Politics', 'World'],
    tags: ['Trump', 'SCOTUS & courts', 'International', 'Local'],
  },
  {
    id: 'elections',
    metaCategory: 'Elections',
    kalshiCategories: ['Elections'],
    tags: ['US Elections', 'House', 'Senate', 'Governor', 'International elections', 'Primaries'],
  },
  {
    id: 'economics',
    metaCategory: 'Economics',
    kalshiCategories: ['Economics'],
    tags: ['Jobs & Economy', 'Fed', 'Inflation', 'GDP', 'Housing', 'Growth'],
  },
  {
    id: 'finance',
    metaCategory: 'Finance',
    kalshiCategories: ['Financials', 'Companies', 'Business'],
    tags: ['KPIs', 'Companies', 'Industries', 'Earnings', 'IPOs'],
  },
  {
    id: 'crypto',
    metaCategory: 'Crypto',
    kalshiCategories: ['Crypto'],
    tags: ['BTC'],
  },
  {
    id: 'commodities',
    metaCategory: 'Commodities',
    kalshiCategories: ['Commodities'],
    tags: ['Oil and energy'],
  },
  {
    id: 'sports',
    metaCategory: 'Sports',
    kalshiCategories: ['Sports'],
    tags: ['Football', 'Basketball', 'Soccer', 'Hockey', 'Baseball', 'Golf', 'MMA'],
  },
  {
    id: 'culture',
    metaCategory: 'Culture',
    kalshiCategories: ['Entertainment', 'Social'],
    tags: ['Music', 'Oscars', 'Movies', 'Television', 'Awards', 'Live Music'],
  },
  {
    // `Health` lives here: Octagon's closed set has no Health meta category, and
    // the Kalshi Health category plus the Public Health / Medicine tags are ~19
    // events that would otherwise be unreachable from any theme.
    id: 'tech-science',
    metaCategory: 'Tech & Science',
    kalshiCategories: ['Science and Technology', 'Science & Technology', 'AI', 'Health'],
    tags: ['AI', 'Tech', 'Compute', 'Space', 'Product launches', 'Public Health', 'Medicine'],
  },
  {
    id: 'climate',
    metaCategory: 'Climate',
    kalshiCategories: ['Climate and Weather'],
    tags: [],
  },
  {
    id: 'mentions',
    metaCategory: 'Mentions',
    kalshiCategories: ['Mentions'],
    tags: [],
  },
];

/**
 * Legacy theme ids folded onto canonical ones, so anything that worked before
 * keeps working. `transportation` is deliberately absent: it mapped to a
 * category that matches zero rows, and aliasing it to something adjacent would
 * silently return unrelated markets.
 */
const ALIASES: Record<string, string> = {
  entertainment: 'culture',
  social: 'culture',
  companies: 'finance',
  financials: 'finance',
  science: 'tech-science',
  health: 'tech-science',
  ai: 'tech-science',
  world: 'politics',
  weather: 'climate',
};

const BY_ID = new Map(THEMES.map((t) => [t.id, t]));

/** Resolve a user-typed word to a theme. Tolerates case and surrounding space. */
export function findTheme(input: string): Theme | undefined {
  const key = input.trim().toLowerCase();
  return BY_ID.get(ALIASES[key] ?? key);
}

export function isThemeId(input: string): boolean {
  const key = input.trim().toLowerCase();
  return key === TOP50 || findTheme(key) !== undefined;
}

/** Canonical ids first, then aliases — for autocomplete and help text. */
export function allThemeIds(): string[] {
  return [TOP50, ...THEMES.map((t) => t.id), ...Object.keys(ALIASES)];
}

/** `{ themeId: kalshiCategories }` — the successor to the old CATEGORY_MAP. */
export function themeCategoryLabels(): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const t of THEMES) out[t.id] = t.kalshiCategories;
  for (const [alias, target] of Object.entries(ALIASES)) {
    const theme = BY_ID.get(target);
    if (theme) out[alias] = theme.kalshiCategories;
  }
  return out;
}

/**
 * Split `theme:subtheme` into its parts. `crypto:btc` → the crypto theme plus
 * the raw subtheme `btc`. Free text yields `{ theme: undefined }`.
 */
export function parseThemeQuery(input: string): { theme?: Theme; subtheme?: string } {
  const trimmed = input.trim();
  const colon = trimmed.indexOf(':');
  if (colon === -1) return { theme: findTheme(trimmed) };
  const theme = findTheme(trimmed.slice(0, colon));
  if (!theme) return {};
  const subtheme = trimmed.slice(colon + 1).trim();
  return subtheme ? { theme, subtheme } : { theme };
}
