/**
 * Editorial themes registry — user-curated narrative buckets (e.g. "AI Race
 * Milestones", "Iran Escalation") that map to lists of Kalshi series. Distinct
 * from the existing `themes` table (which holds Kalshi-API watch lists) and
 * from Octagon's nightly k-means clusters (which are embedding-derived).
 */
import type { Database } from 'bun:sqlite';

export interface EditorialThemeRow {
  name: string;
  description: string | null;
  search_volume: number | null;
  created_at: number;
  updated_at: number;
}

export interface EditorialThemeWithSeries extends EditorialThemeRow {
  series: string[];
}

export function listEditorialThemes(db: Database): EditorialThemeRow[] {
  return db.query(
    'SELECT name, description, search_volume, created_at, updated_at FROM editorial_themes ORDER BY name',
  ).all() as EditorialThemeRow[];
}

export function getEditorialTheme(db: Database, name: string): EditorialThemeWithSeries | null {
  const row = db.query(
    'SELECT name, description, search_volume, created_at, updated_at FROM editorial_themes WHERE name = $name',
  ).get({ $name: name }) as EditorialThemeRow | undefined;
  if (!row) return null;
  const series = db.query(
    'SELECT series_ticker FROM editorial_theme_series WHERE theme_name = $name ORDER BY series_ticker',
  ).all({ $name: name }) as Array<{ series_ticker: string }>;
  return { ...row, series: series.map((s) => s.series_ticker) };
}

export function upsertEditorialTheme(
  db: Database,
  args: { name: string; description?: string | null; search_volume?: number | null },
): void {
  const now = Date.now();
  db.query(
    `INSERT INTO editorial_themes (name, description, search_volume, created_at, updated_at)
     VALUES ($name, $description, $search_volume, $now, $now)
     ON CONFLICT(name) DO UPDATE SET
       description = COALESCE($description, description),
       search_volume = COALESCE($search_volume, search_volume),
       updated_at = $now`,
  ).run({
    $name: args.name,
    $description: args.description ?? null,
    $search_volume: args.search_volume ?? null,
    $now: now,
  });
}

export function deleteEditorialTheme(db: Database, name: string): boolean {
  const before = (db.query('SELECT COUNT(*) as n FROM editorial_themes WHERE name = $name').get({ $name: name }) as { n: number }).n;
  db.query('DELETE FROM editorial_themes WHERE name = $name').run({ $name: name });
  return before > 0;
}

export function addSeriesToTheme(db: Database, themeName: string, seriesTickers: string[]): number {
  const stmt = db.query(
    `INSERT OR IGNORE INTO editorial_theme_series (theme_name, series_ticker) VALUES ($name, $series)`,
  );
  let added = 0;
  for (const s of seriesTickers) {
    const upper = s.trim().toUpperCase();
    if (!upper) continue;
    const changes = stmt.run({ $name: themeName, $series: upper });
    if (changes.changes > 0) added += 1;
  }
  return added;
}

export function removeSeriesFromTheme(db: Database, themeName: string, seriesTickers: string[]): number {
  const stmt = db.query(
    `DELETE FROM editorial_theme_series WHERE theme_name = $name AND series_ticker = $series`,
  );
  let removed = 0;
  for (const s of seriesTickers) {
    const upper = s.trim().toUpperCase();
    const changes = stmt.run({ $name: themeName, $series: upper });
    removed += changes.changes;
  }
  return removed;
}

export function setSearchVolume(db: Database, themeName: string, volume: number): boolean {
  const result = db.query(
    `UPDATE editorial_themes SET search_volume = $vol, updated_at = $now WHERE name = $name`,
  ).run({ $name: themeName, $vol: volume, $now: Date.now() });
  return result.changes > 0;
}

/**
 * Returns series tickers that appear in more than one theme. Useful for
 * cross-theme dedupe audits.
 */
export function findSeriesOverlaps(db: Database): Array<{ series_ticker: string; themes: string[] }> {
  const rows = db.query(
    `SELECT series_ticker, GROUP_CONCAT(theme_name, '|') as themes_csv
     FROM editorial_theme_series
     GROUP BY series_ticker
     HAVING COUNT(*) > 1
     ORDER BY series_ticker`,
  ).all() as Array<{ series_ticker: string; themes_csv: string }>;
  return rows.map((r) => ({ series_ticker: r.series_ticker, themes: r.themes_csv.split('|') }));
}
