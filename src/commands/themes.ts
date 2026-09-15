import { getDb } from '../db/index.js';
import { getActiveThemes } from '../db/themes.js';
import { CATEGORY_MAP, fetchSubcategories } from '../scan/theme-resolver.js';
import { formatTable } from './scan-formatters.js';
import { wrapSuccess } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';

export interface ThemeInfo {
  id: string;
  name: string;
  type: 'built-in' | 'category' | 'custom';
  subcategories?: string[];
  tickerCount?: number;
}

export interface ThemesResult {
  themes: ThemeInfo[];
}

export async function handleThemes(args: ParsedArgs): Promise<CLIResponse<ThemesResult>> {
  const themes: ThemeInfo[] = [];

  // Special built-in theme
  themes.push({ id: 'top50', name: 'Top 50 markets by 24h volume', type: 'built-in' });

  // Fetch subcategories from Kalshi API
  let subcatMap: Record<string, string[]> = {};
  try {
    subcatMap = await fetchSubcategories();
  } catch {
    // API unavailable — show categories without subcategories
  }

  // All Kalshi categories with their subcategories. A theme can span several
  // upstream labels (e.g. both spellings of "Science & Technology"), so the
  // subcategory tags are the union across them.
  for (const [id, labels] of Object.entries(CATEGORY_MAP)) {
    const subs = [...new Set(labels.flatMap((l) => subcatMap[l] ?? []))];
    themes.push({
      id,
      name: labels.join(', '),
      type: 'category',
      ...(subs.length > 0 ? { subcategories: subs } : {}),
    });
  }

  // Custom themes from DB
  try {
    const db = getDb();
    const custom = getActiveThemes(db);
    for (const t of custom) {
      const tickers = t.tickers ? JSON.parse(t.tickers) as string[] : [];
      themes.push({
        id: t.theme_id,
        name: t.name,
        type: 'custom',
        tickerCount: tickers.length,
      });
    }
  } catch {
    // DB not initialized yet — just show built-in themes
  }

  return wrapSuccess('themes', { themes });
}

/** Wrap a comma-separated list into lines of at most `maxWidth` characters */
function wrapSubs(subs: string[], maxWidth: number): string[] {
  if (subs.length === 0) return [''];
  const wrapped: string[] = [];
  let line = '';
  for (const s of subs) {
    const addition = line ? `, ${s}` : s;
    if (line && (line + addition).length > maxWidth) {
      wrapped.push(line);
      line = s;
    } else {
      line += addition;
    }
  }
  if (line) wrapped.push(line);
  return wrapped;
}

export function formatThemesHuman(data: ThemesResult): string {
  const lines: string[] = [];
  const SUB_WIDTH = 55;

  const rows: string[][] = [];

  // Built-in special themes
  for (const t of data.themes.filter((t) => t.type === 'built-in')) {
    rows.push([t.id, t.name]);
  }

  // Categories — main row, then one subcategory per line
  for (const t of data.themes.filter((t) => t.type === 'category')) {
    rows.push([t.id, t.name]);
    for (const s of t.subcategories ?? []) {
      rows.push(['', `  ${t.id}:${s.toLowerCase()}`]);
    }
  }

  // Custom themes
  for (const t of data.themes.filter((t) => t.type === 'custom')) {
    const extra = t.tickerCount !== undefined ? `${t.name} (${t.tickerCount} tickers)` : t.name;
    rows.push([t.id, extra]);
  }

  lines.push(formatTable(['Theme', 'Description / Subcategories'], rows));

  lines.push('');
  lines.push('Usage: search crypto');
  lines.push('       search crypto:btc');
  lines.push('       analyze <TICKER>');

  return lines.join('\n');
}
