/**
 * Editorial themes — narrative buckets that the user curates (e.g. "AI Race
 * Milestones", "Iran Escalation"), mapped to lists of Kalshi series, optionally
 * annotated with monthly SEO search volume.
 *
 * Subcommands (under the existing `themes` slot, when first positional is one
 * of these keywords):
 *   themes list                          List all editorial themes
 *   themes show <name>                   Drill into one theme
 *   themes create <name> [--label desc] [--search-volume N] [--series KXA,KXB]
 *   themes delete <name>
 *   themes add-series <name> KX-A,KX-B
 *   themes remove-series <name> KX-A
 *   themes set-search-volume <name> N
 *   themes import [<path>]               Default: data/themes_seo.json
 *   themes export <path>
 *   themes report [--min-volume N] [--min-search N]   25-theme dashboard
 *   themes audit                         Flag dead themes (high SEO / zero vol)
 *   themes overlap                       Cross-theme dedupe report
 */
import { readFileSync, writeFileSync } from 'fs';
import { resolve as resolvePath } from 'path';
import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import { getDb } from '../db/index.js';
import {
  listEditorialThemes,
  getEditorialTheme,
  upsertEditorialTheme,
  deleteEditorialTheme,
  addSeriesToTheme,
  removeSeriesFromTheme,
  setSearchVolume,
  findSeriesOverlaps,
  type EditorialThemeRow,
  type EditorialThemeWithSeries,
} from '../db/editorial-themes.js';
import { fetchUniverse, rollupBySeries, type SeriesRollup } from './series.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtVol(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return v.toFixed(0);
}

function fmtSearchVol(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-';
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}k`;
  return String(v);
}

// ─── Result types ───────────────────────────────────────────────────────────

export type EditorialThemeResult =
  | { kind: 'list'; data: EditorialThemeRow[] }
  | { kind: 'show'; theme: EditorialThemeWithSeries }
  | { kind: 'mutation'; action: string; theme?: string; affected?: number; message: string }
  | { kind: 'overlap'; data: Array<{ series_ticker: string; themes: string[] }> }
  | { kind: 'audit'; data: Array<{ name: string; search_volume: number | null; active_markets: number; total_volume_24h: number; status: string }> }
  | { kind: 'report'; data: Array<{ name: string; search_volume: number | null; series_count: number; series: Array<SeriesRollup & { theme_match: boolean }>; active_markets: number; total_volume_24h: number }> };

// ─── Handler dispatch ───────────────────────────────────────────────────────

export async function handleEditorialThemes(args: ParsedArgs): Promise<CLIResponse<EditorialThemeResult>> {
  const db = getDb();
  const sub = args.positionalArgs[0]?.toLowerCase();
  const rest = args.positionalArgs.slice(1);

  try {
    switch (sub) {
      case 'list':
      case undefined:
        return listHandler(db, sub === undefined);
      case 'show':
        return showHandler(db, rest[0]);
      case 'create':
        return createHandler(db, rest, args);
      case 'delete':
        return deleteHandler(db, rest[0]);
      case 'add-series':
        return addSeriesHandler(db, rest);
      case 'remove-series':
        return removeSeriesHandler(db, rest);
      case 'set-search-volume':
        return setSearchVolumeHandler(db, rest);
      case 'import':
        return importHandler(db, rest[0]);
      case 'export':
        return exportHandler(db, rest[0]);
      case 'overlap':
        return overlapHandler(db);
      case 'audit':
        return await auditHandler(db, args);
      case 'report':
        return await reportHandler(db, args);
      default:
        return wrapError('themes', 'UNKNOWN_SUB', `Unknown subcommand: ${sub}. Try: list, show, create, delete, add-series, remove-series, set-search-volume, import, export, report, audit, overlap`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('themes', 'INTERNAL', message);
  }
}

// ─── Handlers ───────────────────────────────────────────────────────────────

function listHandler(db: ReturnType<typeof getDb>, fellThroughBare: boolean): CLIResponse<EditorialThemeResult> {
  const themes = listEditorialThemes(db);
  // If user typed bare `themes` and registry is empty, suggest the import command.
  if (fellThroughBare && themes.length === 0) {
    return wrapError('themes', 'EMPTY_REGISTRY', 'No editorial themes registered. Run `themes import` to seed from data/themes_seo.json, or `themes create <name>` to add one.');
  }
  return wrapSuccess('themes', { kind: 'list', data: themes });
}

function showHandler(db: ReturnType<typeof getDb>, name?: string): CLIResponse<EditorialThemeResult> {
  if (!name) return wrapError('themes', 'MISSING_NAME', 'Usage: themes show <name>');
  const theme = getEditorialTheme(db, name);
  if (!theme) return wrapError('themes', 'NOT_FOUND', `No editorial theme named "${name}". Try \`themes list\`.`);
  return wrapSuccess('themes', { kind: 'show', theme });
}

function createHandler(db: ReturnType<typeof getDb>, positional: string[], args: ParsedArgs): CLIResponse<EditorialThemeResult> {
  const name = positional.join(' ').trim();
  if (!name) return wrapError('themes', 'MISSING_NAME', 'Usage: themes create <name> [--label "desc"] [--search-volume N] [--tickers KX-A,KX-B]');
  upsertEditorialTheme(db, {
    name,
    description: args.labelContains ?? null,
    search_volume: args.minVolume ?? null,
  });
  let added = 0;
  if (args.tickers) {
    added = addSeriesToTheme(db, name, args.tickers.split(','));
  }
  return wrapSuccess('themes', {
    kind: 'mutation',
    action: 'create',
    theme: name,
    affected: added,
    message: `Created theme "${name}"${added ? ` with ${added} series` : ''}.`,
  });
}

function deleteHandler(db: ReturnType<typeof getDb>, name?: string): CLIResponse<EditorialThemeResult> {
  if (!name) return wrapError('themes', 'MISSING_NAME', 'Usage: themes delete <name>');
  const existed = deleteEditorialTheme(db, name);
  if (!existed) return wrapError('themes', 'NOT_FOUND', `No editorial theme named "${name}".`);
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'delete', theme: name, message: `Deleted theme "${name}".`,
  });
}

function addSeriesHandler(db: ReturnType<typeof getDb>, positional: string[]): CLIResponse<EditorialThemeResult> {
  if (positional.length < 2) return wrapError('themes', 'MISSING_ARGS', 'Usage: themes add-series <theme_name> <SERIES,SERIES,...>');
  const themeName = positional[0];
  const list = positional.slice(1).join(',').split(',');
  if (!getEditorialTheme(db, themeName)) {
    return wrapError('themes', 'NOT_FOUND', `No editorial theme named "${themeName}". Create it first with \`themes create\`.`);
  }
  const added = addSeriesToTheme(db, themeName, list);
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'add-series', theme: themeName, affected: added,
    message: `Added ${added} series to "${themeName}".`,
  });
}

function removeSeriesHandler(db: ReturnType<typeof getDb>, positional: string[]): CLIResponse<EditorialThemeResult> {
  if (positional.length < 2) return wrapError('themes', 'MISSING_ARGS', 'Usage: themes remove-series <theme_name> <SERIES,SERIES,...>');
  const themeName = positional[0];
  const list = positional.slice(1).join(',').split(',');
  const removed = removeSeriesFromTheme(db, themeName, list);
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'remove-series', theme: themeName, affected: removed,
    message: `Removed ${removed} series from "${themeName}".`,
  });
}

function setSearchVolumeHandler(db: ReturnType<typeof getDb>, positional: string[]): CLIResponse<EditorialThemeResult> {
  if (positional.length < 2) return wrapError('themes', 'MISSING_ARGS', 'Usage: themes set-search-volume <name> <number>');
  const themeName = positional[0];
  const volume = Number(positional[1]);
  if (!Number.isFinite(volume) || volume < 0) {
    return wrapError('themes', 'INVALID_VOLUME', `Invalid search volume: "${positional[1]}"`);
  }
  const ok = setSearchVolume(db, themeName, volume);
  if (!ok) return wrapError('themes', 'NOT_FOUND', `No editorial theme named "${themeName}".`);
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'set-search-volume', theme: themeName,
    message: `Set search_volume=${volume} on "${themeName}".`,
  });
}

interface ThemesImportFile {
  themes: Array<{
    name: string;
    description?: string;
    search_volume?: number;
    series?: string[];
  }>;
}

function importHandler(db: ReturnType<typeof getDb>, path?: string): CLIResponse<EditorialThemeResult> {
  const importPath = path ?? resolvePath(import.meta.dir, '..', '..', 'data', 'themes_seo.json');
  let raw: string;
  try {
    raw = readFileSync(importPath, 'utf-8');
  } catch (err) {
    return wrapError('themes', 'READ_ERROR', `Cannot read ${importPath}: ${err instanceof Error ? err.message : String(err)}`);
  }
  let parsed: ThemesImportFile;
  try {
    parsed = JSON.parse(raw) as ThemesImportFile;
  } catch (err) {
    return wrapError('themes', 'PARSE_ERROR', `Invalid JSON in ${importPath}: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (!parsed.themes || !Array.isArray(parsed.themes)) {
    return wrapError('themes', 'BAD_SHAPE', `Expected { "themes": [...] } in ${importPath}`);
  }
  let createdOrUpdated = 0;
  let seriesAdded = 0;
  for (const t of parsed.themes) {
    if (!t.name) continue;
    upsertEditorialTheme(db, {
      name: t.name,
      description: t.description ?? null,
      search_volume: t.search_volume ?? null,
    });
    createdOrUpdated += 1;
    if (Array.isArray(t.series) && t.series.length > 0) {
      seriesAdded += addSeriesToTheme(db, t.name, t.series);
    }
  }
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'import',
    message: `Imported ${createdOrUpdated} themes (${seriesAdded} new series mappings) from ${importPath}.`,
  });
}

function exportHandler(db: ReturnType<typeof getDb>, path?: string): CLIResponse<EditorialThemeResult> {
  if (!path) return wrapError('themes', 'MISSING_PATH', 'Usage: themes export <path>');
  const themes = listEditorialThemes(db);
  const out: ThemesImportFile = {
    themes: themes.map((t) => {
      const detail = getEditorialTheme(db, t.name);
      return {
        name: t.name,
        description: t.description ?? undefined,
        search_volume: t.search_volume ?? undefined,
        series: detail?.series ?? [],
      };
    }),
  };
  try {
    writeFileSync(path, JSON.stringify(out, null, 2));
  } catch (err) {
    return wrapError('themes', 'WRITE_ERROR', `Cannot write ${path}: ${err instanceof Error ? err.message : String(err)}`);
  }
  return wrapSuccess('themes', {
    kind: 'mutation', action: 'export',
    message: `Exported ${out.themes.length} themes to ${path}.`,
  });
}

function overlapHandler(db: ReturnType<typeof getDb>): CLIResponse<EditorialThemeResult> {
  const data = findSeriesOverlaps(db);
  return wrapSuccess('themes', { kind: 'overlap', data });
}

async function reportHandler(db: ReturnType<typeof getDb>, args: ParsedArgs): Promise<CLIResponse<EditorialThemeResult>> {
  const themes = listEditorialThemes(db);
  if (themes.length === 0) {
    return wrapError('themes', 'EMPTY_REGISTRY', 'No editorial themes registered. Run `themes import` first.');
  }
  // Single universe fetch — rollup gives us volume data per series; we then
  // attribute series back to themes.
  const universe = await fetchUniverse({});
  const allRollups = rollupBySeries(universe);
  const rollupByTicker = new Map(allRollups.map((r) => [r.series_ticker, r]));

  const minSearch = args.minReturn !== undefined ? Math.floor(args.minReturn * 1_000_000) : (args.windowDays ?? 0);
  // ^ reuse `--min-return` parsed as fraction of millions, or `--window-days` as raw integer;
  //   safer to expose a clean flag in dispatch. For now use args.windowDays as int floor.
  const minVolume = args.minVolume ?? 0;

  const out: Array<{
    name: string;
    search_volume: number | null;
    series_count: number;
    series: Array<SeriesRollup & { theme_match: boolean }>;
    active_markets: number;
    total_volume_24h: number;
  }> = [];

  for (const t of themes) {
    if (t.search_volume != null && t.search_volume < minSearch) continue;
    const detail = getEditorialTheme(db, t.name);
    const seriesList: Array<SeriesRollup & { theme_match: boolean }> = [];
    let activeMarkets = 0;
    let totalVolume = 0;
    for (const seriesTicker of detail?.series ?? []) {
      const r = rollupByTicker.get(seriesTicker.toUpperCase());
      if (r) {
        seriesList.push({ ...r, theme_match: true });
        activeMarkets += r.active_count;
        totalVolume += r.total_volume_24h;
      } else {
        // Theme references a series with no current active markets — record a stub
        seriesList.push({
          series_ticker: seriesTicker.toUpperCase(),
          market_count: 0,
          active_count: 0,
          total_volume_24h: 0,
          total_open_interest: 0,
          dominant_category: null,
          sample_titles: [],
          earliest_close: null,
          latest_close: null,
          theme_match: false,
        });
      }
    }
    if (totalVolume < minVolume) continue;
    seriesList.sort((a, b) => b.total_volume_24h - a.total_volume_24h);
    out.push({
      name: t.name,
      search_volume: t.search_volume,
      series_count: seriesList.length,
      series: seriesList,
      active_markets: activeMarkets,
      total_volume_24h: totalVolume,
    });
  }

  out.sort((a, b) => (b.search_volume ?? 0) - (a.search_volume ?? 0));
  return wrapSuccess('themes', { kind: 'report', data: out });
}

async function auditHandler(db: ReturnType<typeof getDb>, _args: ParsedArgs): Promise<CLIResponse<EditorialThemeResult>> {
  const report = await reportHandler(db, _args);
  if (!report.ok || report.data.kind !== 'report') return report;
  // Tag each theme with a status:
  //   STALE     — has SEO interest but 0 active markets across all series
  //   THIN      — active_markets >0 but total_volume_24h <$1000
  //   NO_INVENTORY — no series mapped at all
  //   TRADEABLE — active_markets>0 and volume>$1000
  //   UNKNOWN_DEMAND — no search_volume recorded
  const STALE_VOL_FLOOR = 1000;
  const auditRows = report.data.data.map((r) => {
    let status = 'TRADEABLE';
    if (r.series_count === 0) status = 'NO_INVENTORY';
    else if (r.active_markets === 0) status = 'STALE';
    else if (r.total_volume_24h < STALE_VOL_FLOOR) status = 'THIN';
    if (r.search_volume == null && status === 'TRADEABLE') status = 'TRADEABLE_NO_SEO';
    return {
      name: r.name,
      search_volume: r.search_volume,
      active_markets: r.active_markets,
      total_volume_24h: r.total_volume_24h,
      status,
    };
  });
  // Sort: STALE / NO_INVENTORY first (the warnings), then TRADEABLE by volume desc
  const order: Record<string, number> = { STALE: 0, NO_INVENTORY: 0, THIN: 1, TRADEABLE_NO_SEO: 2, TRADEABLE: 3 };
  auditRows.sort((a, b) => {
    const oa = order[a.status] ?? 99;
    const ob = order[b.status] ?? 99;
    if (oa !== ob) return oa - ob;
    return (b.search_volume ?? 0) - (a.search_volume ?? 0);
  });
  return wrapSuccess('themes', { kind: 'audit', data: auditRows });
}

// ─── Formatters ─────────────────────────────────────────────────────────────

export function formatEditorialThemesHuman(result: EditorialThemeResult): string {
  switch (result.kind) {
    case 'list':       return formatList(result.data);
    case 'show':       return formatShow(result.theme);
    case 'mutation':   return result.message;
    case 'overlap':    return formatOverlap(result.data);
    case 'audit':      return formatAudit(result.data);
    case 'report':     return formatReport(result.data);
  }
}

function formatList(themes: EditorialThemeRow[]): string {
  const lines: string[] = [];
  lines.push(`Editorial themes — ${themes.length}`);
  lines.push('');
  if (themes.length === 0) {
    lines.push('No themes registered. Run `themes import` to seed from data/themes_seo.json.');
    return lines.join('\n');
  }
  const rows: string[][] = themes.map((t) => [
    t.name,
    fmtSearchVol(t.search_volume),
    truncate(t.description ?? '', 60),
  ]);
  lines.push(formatTable(['Name', 'Monthly searches', 'Description'], rows));
  lines.push('');
  lines.push('Use `themes show <name>` to drill in, `themes report` for the full dashboard.');
  return lines.join('\n');
}

function formatShow(theme: EditorialThemeWithSeries): string {
  const lines: string[] = [];
  lines.push(`Editorial theme: ${theme.name}`);
  lines.push(`  Description    ${theme.description ?? '-'}`);
  lines.push(`  Search volume  ${fmtSearchVol(theme.search_volume)}/month`);
  lines.push(`  Series         ${theme.series.length} mapped`);
  if (theme.series.length > 0) {
    lines.push('');
    lines.push('  ' + theme.series.join(', '));
  }
  return lines.join('\n');
}

function formatOverlap(rows: Array<{ series_ticker: string; themes: string[] }>): string {
  const lines: string[] = [];
  lines.push(`Cross-theme overlap audit — ${rows.length} series appear in 2+ themes`);
  lines.push('');
  if (rows.length === 0) {
    lines.push('No overlaps. Every series is in exactly one theme.');
    return lines.join('\n');
  }
  const tableRows: string[][] = rows.map((r) => [r.series_ticker, r.themes.join(' · ')]);
  lines.push(formatTable(['Series', 'Themes'], tableRows));
  lines.push('');
  lines.push('Use `basket build --dedupe-series` to suppress duplicates when constructing cross-theme baskets.');
  return lines.join('\n');
}

function formatAudit(rows: Array<{ name: string; search_volume: number | null; active_markets: number; total_volume_24h: number; status: string }>): string {
  const lines: string[] = [];
  lines.push(`Theme audit — ${rows.length} themes, dead/thin themes flagged first`);
  lines.push('');
  const tableRows: string[][] = rows.map((r) => [
    r.name,
    r.status,
    fmtSearchVol(r.search_volume),
    String(r.active_markets),
    fmtVol(r.total_volume_24h),
  ]);
  lines.push(formatTable(['Theme', 'Status', 'Searches', 'Active mkts', '24h Vol'], tableRows));
  lines.push('');
  lines.push('STALE = high SEO + zero active markets · THIN = <$1000/day · TRADEABLE = ready.');
  return lines.join('\n');
}

function formatReport(rows: Array<{ name: string; search_volume: number | null; series_count: number; series: Array<SeriesRollup & { theme_match: boolean }>; active_markets: number; total_volume_24h: number }>): string {
  const lines: string[] = [];
  lines.push(`Editorial theme dashboard — ${rows.length} themes, sorted by search volume`);
  lines.push('');
  const tableRows: string[][] = rows.map((r) => [
    r.name,
    fmtSearchVol(r.search_volume),
    String(r.series_count),
    String(r.active_markets),
    fmtVol(r.total_volume_24h),
    r.series.slice(0, 2).map((s) => s.series_ticker).join(', '),
  ]);
  lines.push(formatTable(
    ['Theme', 'Searches', 'Series', 'Active mkts', '24h Vol', 'Top series'],
    tableRows,
  ));
  lines.push('');
  lines.push('Use `themes show <name>` for full series list, `themes audit` to flag dead themes.');
  return lines.join('\n');
}
