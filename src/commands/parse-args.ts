const SUBCOMMANDS = [
  // Core 6 commands
  'search', 'portfolio', 'analyze', 'watch',
  'buy', 'sell', 'cancel', 'help',
  // Legacy aliases (kept for backward compat)
  'edge',
  'alerts', 'config', 'clear-cache', 'chat', 'init', 'status', 'themes',
  // Backtest
  'backtest',
] as const;

export type Subcommand = (typeof SUBCOMMANDS)[number];

export interface ParsedArgs {
  subcommand: Subcommand;
  positionalArgs: string[];
  json: boolean;
  theme?: string;
  ticker?: string;
  interval?: number;
  since?: string;
  minConfidence?: string;
  minEdge?: number;
  live: boolean;
  refresh: boolean;
  report: boolean;
  side?: 'yes' | 'no';
  dryRun: boolean;
  verbose: boolean;
  performance: boolean;
  // Backtest-specific
  resolved: boolean;
  unresolved: boolean;
  days?: number;
  category?: string;
  exportPath?: string;
  parseErrors: string[];
}

export function parseArgs(argv: string[] = process.argv.slice(2)): ParsedArgs {
  const positionalArgs: string[] = [];
  let json = false;
  let theme: string | undefined;
  let ticker: string | undefined;
  let interval: number | undefined;
  let since: string | undefined;
  let minConfidence: string | undefined;
  let minEdge: number | undefined;
  let live = false;
  let refresh = false;
  let report = false;
  let side: 'yes' | 'no' | undefined;
  const parseErrors: string[] = [];
  let dryRun = false;
  let verbose = false;
  let performance = false;
  let resolved = false;
  let unresolved = false;
  let days: number | undefined;
  let category: string | undefined;
  let exportPath: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];

    if (arg === '--json') {
      json = true;
    } else if (arg === '--theme') {
      const val = argv[++i];
      if (val != null) {
        theme = val;
      } else {
        parseErrors.push('--theme requires a value');
      }
    } else if (arg === '--ticker') {
      const val = argv[++i];
      if (val != null) {
        ticker = val;
      } else {
        parseErrors.push('--ticker requires a value');
      }
    } else if (arg === '--interval') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric > 0) {
          interval = numeric;
        } else {
          parseErrors.push(`Invalid --interval value: "${raw}" (expected a positive number)`);
        }
      } else {
        parseErrors.push('--interval requires a value');
      }
    } else if (arg === '--since') {
      const val = argv[++i];
      if (val != null) {
        since = val;
      } else {
        parseErrors.push('--since requires a value');
      }
    } else if (arg === '--min-confidence') {
      const val = argv[++i];
      if (val != null) {
        minConfidence = val.toLowerCase();
      } else {
        parseErrors.push('--min-confidence requires a value');
      }
    } else if (arg === '--min-edge') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw.replace('%', ''));
        if (Number.isFinite(numeric)) {
          minEdge = numeric / 100;
        } else {
          parseErrors.push(`Invalid --min-edge value: "${raw}" (expected a number like 5 or 5%)`);
        }
      } else {
        parseErrors.push('--min-edge requires a value (e.g., --min-edge 5 or --min-edge 5%)');
      }
    } else if (arg === '--side') {
      const val = argv[++i];
      if (val == null) {
        parseErrors.push('--side requires a value (expected "yes" or "no")');
      } else {
        const lower = val.toLowerCase();
        if (lower === 'yes' || lower === 'no') {
          side = lower;
        } else {
          parseErrors.push(`Invalid --side value: "${val}" (expected "yes" or "no")`);
        }
      }
    } else if (arg === '--live') {
      live = true;
    } else if (arg === '--refresh') {
      refresh = true;
    } else if (arg === '--report') {
      report = true;
    } else if (arg === '--dry-run') {
      dryRun = true;
    } else if (arg === '--verbose') {
      verbose = true;
    } else if (arg === '--performance') {
      performance = true;
    } else if (arg === '--resolved') {
      resolved = true;
    } else if (arg === '--unresolved') {
      unresolved = true;
    } else if (arg === '--category') {
      const val = argv[++i];
      if (val != null) { category = val; } else { parseErrors.push('--category requires a value'); }
    } else if (arg === '--days') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric > 0) { days = numeric; }
        else { parseErrors.push(`Invalid --days value: "${raw}" (expected a positive number)`); }
      } else { parseErrors.push('--days requires a value'); }
    } else if (arg === '--export') {
      const val = argv[++i];
      if (val != null) { exportPath = val; } else { parseErrors.push('--export requires a value'); }
    } else if (arg.startsWith('--')) {
      parseErrors.push(`Unknown flag: ${arg}`);
    } else {
      positionalArgs.push(arg);
    }
  }

  if (resolved && unresolved) {
    parseErrors.push('Cannot use --resolved and --unresolved together');
  }

  const first = positionalArgs.shift();
  const subcommand: Subcommand =
    first && (SUBCOMMANDS as readonly string[]).includes(first)
      ? (first as Subcommand)
      : 'chat';

  // If first arg wasn't a known subcommand, put it back as a positional
  if (first && !(SUBCOMMANDS as readonly string[]).includes(first)) {
    positionalArgs.unshift(first);
  }

  return { subcommand, positionalArgs, json, theme, ticker, interval, since, minConfidence, minEdge, side, live, refresh, report, dryRun, verbose, performance, resolved, unresolved, days, category, exportPath, parseErrors };
}
