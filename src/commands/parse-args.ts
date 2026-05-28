const SUBCOMMANDS = [
  // Core 6 commands
  'search', 'portfolio', 'analyze', 'watch',
  'buy', 'sell', 'cancel', 'help',
  // Legacy aliases (kept for backward compat)
  'edge',
  'alerts', 'config', 'clear-cache', 'chat', 'init', 'status', 'themes',
  // Backtest
  'backtest',
  // Octagon Kalshi search/clusters/basket (new)
  'similar', 'clusters', 'peers', 'correlate', 'basket',
  // Octagon events + Kalshi series rollup + editorial themes registry
  'events', 'series', 'catalysts',
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
  maxAge?: number;
  category?: string;
  limit?: number;
  exportPath?: string;
  minVolume?: number;
  minPrice?: number;
  maxPrice?: number;
  // Octagon Kalshi search/clusters/basket flags
  topK?: number;
  behavioral: boolean;
  ranked: boolean;
  labelContains?: string;
  closeBefore?: string;
  windowDays?: number;
  correlationInterval?: '1h' | '1d';
  timeframe?: '1w' | '1m' | '3m' | '6m' | '1y';
  weights?: number[];
  bankroll?: number;
  kellyMultiplier?: number;
  n?: number;
  maxPerCluster?: number;
  maxCorrelation?: number;
  minReturn?: number;
  seriesTicker?: string;
  seriesPrefix?: string;
  sortBy?: string;
  probabilities?: string;
  tickers?: string;
  query?: string;
  showCluster: boolean;
  aggregateBy?: 'series';
  activeOnly: boolean;
  sides?: string;          // comma-separated yes/no per ticker for correlate
  cells: boolean;          // include_cell_detail for correlate
  autoProbs: boolean;      // basket size: auto-fetch leg probabilities via markets/edge
  daysToClose?: number;    // ergonomic shortcut: close_before = now + N days
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
  let limit: number | undefined;
  let exportPath: string | undefined;
  let maxAge: number | undefined;
  let minVolume: number | undefined;
  let minPrice: number | undefined;
  let maxPrice: number | undefined;
  // Octagon Kalshi flags
  let topK: number | undefined;
  let behavioral = false;
  let ranked = false;
  let labelContains: string | undefined;
  let closeBefore: string | undefined;
  let windowDays: number | undefined;
  let correlationInterval: '1h' | '1d' | undefined;
  let timeframe: '1w' | '1m' | '3m' | '6m' | '1y' | undefined;
  let weights: number[] | undefined;
  let bankroll: number | undefined;
  let kellyMultiplier: number | undefined;
  let n: number | undefined;
  let maxPerCluster: number | undefined;
  let maxCorrelation: number | undefined;
  let minReturn: number | undefined;
  let seriesTicker: string | undefined;
  let seriesPrefix: string | undefined;
  let sortBy: string | undefined;
  let probabilities: string | undefined;
  let tickers: string | undefined;
  let query: string | undefined;
  let showCluster = false;
  let aggregateBy: 'series' | undefined;
  let activeOnly = false;
  let sides: string | undefined;
  let cells = false;
  let autoProbs = false;
  let daysToClose: number | undefined;

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
    } else if (arg === '--limit') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric > 0) { limit = numeric; }
        else { parseErrors.push(`Invalid --limit value: "${raw}" (expected a positive number)`); }
      } else { parseErrors.push('--limit requires a value'); }
    } else if (arg === '--export') {
      const val = argv[++i];
      if (val != null) { exportPath = val; } else { parseErrors.push('--export requires a value'); }
    } else if (arg === '--max-age') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric > 0) { maxAge = numeric; }
        else { parseErrors.push(`Invalid --max-age value: "${raw}" (expected a positive number)`); }
      } else { parseErrors.push('--max-age requires a value'); }
    } else if (arg === '--min-volume') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric >= 0) { minVolume = numeric; }
        else { parseErrors.push(`Invalid --min-volume value: "${raw}" (expected a non-negative number)`); }
      } else { parseErrors.push('--min-volume requires a value'); }
    } else if (arg === '--min-price') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric >= 0 && numeric <= 100) { minPrice = numeric; }
        else { parseErrors.push(`Invalid --min-price value: "${raw}" (expected 0-100)`); }
      } else { parseErrors.push('--min-price requires a value'); }
    } else if (arg === '--max-price') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric >= 0 && numeric <= 100) { maxPrice = numeric; }
        else { parseErrors.push(`Invalid --max-price value: "${raw}" (expected 0-100)`); }
      } else { parseErrors.push('--max-price requires a value'); }
    } else if (arg === '--top-k') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && Number.isInteger(numeric) && numeric > 0) { topK = numeric; }
        else { parseErrors.push(`Invalid --top-k value: "${raw}" (expected a positive integer)`); }
      } else { parseErrors.push('--top-k requires a value'); }
    } else if (arg === '--behavioral') {
      behavioral = true;
    } else if (arg === '--ranked') {
      ranked = true;
    } else if (arg === '--show-cluster') {
      showCluster = true;
    } else if (arg === '--label') {
      const val = argv[++i];
      if (val != null) { labelContains = val; } else { parseErrors.push('--label requires a value'); }
    } else if (arg === '--close-before') {
      const val = argv[++i];
      if (val != null) { closeBefore = val; } else { parseErrors.push('--close-before requires a value'); }
    } else if (arg === '--window-days') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && Number.isInteger(numeric) && numeric > 0) { windowDays = numeric; }
        else { parseErrors.push(`Invalid --window-days value: "${raw}" (expected a positive integer)`); }
      } else { parseErrors.push('--window-days requires a value'); }
    } else if (arg === '--correlation-interval') {
      const val = argv[++i];
      if (val === '1h' || val === '1d') { correlationInterval = val; }
      else { parseErrors.push(`Invalid --correlation-interval value: "${val}" (expected "1h" or "1d")`); }
    } else if (arg === '--timeframe') {
      const val = argv[++i];
      if (val === '1w' || val === '1m' || val === '3m' || val === '6m' || val === '1y') {
        timeframe = val;
      } else {
        parseErrors.push(`Invalid --timeframe value: "${val}" (expected one of 1w, 1m, 3m, 6m, 1y)`);
      }
    } else if (arg === '--weights') {
      const val = argv[++i];
      if (val != null) {
        const parts = val.split(',').map((s) => Number(s.trim()));
        if (parts.every((p) => Number.isFinite(p))) { weights = parts; }
        else { parseErrors.push(`Invalid --weights value: "${val}" (expected comma-separated numbers)`); }
      } else { parseErrors.push('--weights requires a value (e.g., --weights 0.4,0.4,0.2)'); }
    } else if (arg === '--bankroll') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric > 0) { bankroll = numeric; }
        else { parseErrors.push(`Invalid --bankroll value: "${raw}" (expected a positive number)`); }
      } else { parseErrors.push('--bankroll requires a value'); }
    } else if (arg === '--kelly') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric >= 0 && numeric <= 1) { kellyMultiplier = numeric; }
        else { parseErrors.push(`Invalid --kelly value: "${raw}" (expected 0-1)`); }
      } else { parseErrors.push('--kelly requires a value (e.g., --kelly 0.25)'); }
    } else if (arg === '-n' || arg === '--n') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && Number.isInteger(numeric) && numeric > 0) { n = numeric; }
        else { parseErrors.push(`Invalid -n value: "${raw}" (expected a positive integer)`); }
      } else { parseErrors.push('-n requires a value'); }
    } else if (arg === '--max-per-cluster') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && Number.isInteger(numeric) && numeric > 0) { maxPerCluster = numeric; }
        else { parseErrors.push(`Invalid --max-per-cluster value: "${raw}" (expected a positive integer)`); }
      } else { parseErrors.push('--max-per-cluster requires a value'); }
    } else if (arg === '--max-corr') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && numeric >= -1 && numeric <= 1) { maxCorrelation = numeric; }
        else { parseErrors.push(`Invalid --max-corr value: "${raw}" (expected -1 to 1)`); }
      } else { parseErrors.push('--max-corr requires a value'); }
    } else if (arg === '--min-return') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric)) { minReturn = numeric; }
        else { parseErrors.push(`Invalid --min-return value: "${raw}" (expected a number, e.g. 0.2 for 20%)`); }
      } else { parseErrors.push('--min-return requires a value'); }
    } else if (arg === '--series') {
      const val = argv[++i];
      if (val != null) { seriesTicker = val; } else { parseErrors.push('--series requires a value'); }
    } else if (arg === '--series-prefix') {
      const val = argv[++i];
      if (val != null) { seriesPrefix = val.toUpperCase(); } else { parseErrors.push('--series-prefix requires a value'); }
    } else if (arg === '--sort-by') {
      const val = argv[++i];
      if (val == null) {
        parseErrors.push('--sort-by requires a value');
      } else {
        // Union across both consumers (search edge + search). Each consumer
        // additionally filters to its own subset; this guards against typos at
        // the CLI surface so invalid values don't silently fall through to defaults.
        const VALID_SORT_BY = new Set([
          // search edge (markets-with-edge)
          'edge_pp', 'expected_return', 'total_volume', 'model_probability',
          // search (markets)
          'volume_24h', 'close_time', 'last_price',
        ]);
        if (VALID_SORT_BY.has(val)) {
          sortBy = val;
        } else {
          parseErrors.push(`Invalid --sort-by value: "${val}" (expected one of ${Array.from(VALID_SORT_BY).join(', ')})`);
        }
      }
    } else if (arg === '--probs') {
      const val = argv[++i];
      if (val != null) { probabilities = val; } else { parseErrors.push('--probs requires a value (e.g., --probs KXBTC-...:0.62,KXETH-...:0.58)'); }
    } else if (arg === '--tickers') {
      const val = argv[++i];
      if (val != null) { tickers = val; } else { parseErrors.push('--tickers requires a value (comma-separated list)'); }
    } else if (arg === '-q' || arg === '--query') {
      const val = argv[++i];
      if (val != null) { query = val; } else { parseErrors.push(`${arg} requires a value`); }
    } else if (arg === '--aggregate-by') {
      const val = argv[++i];
      if (val === 'series') { aggregateBy = 'series'; }
      else { parseErrors.push(`Invalid --aggregate-by value: "${val}" (expected "series")`); }
    } else if (arg === '--active-only') {
      activeOnly = true;
    } else if (arg === '--sides') {
      const val = argv[++i];
      if (val != null) { sides = val; } else { parseErrors.push('--sides requires a value (e.g., --sides yes,no,yes)'); }
    } else if (arg === '--cells') {
      cells = true;
    } else if (arg === '--auto-probs') {
      autoProbs = true;
    } else if (arg === '--days-to-close' || arg === '--max-dte') {
      const raw = argv[++i];
      if (raw != null) {
        const numeric = Number(raw);
        if (Number.isFinite(numeric) && Number.isInteger(numeric) && numeric > 0) { daysToClose = numeric; }
        else { parseErrors.push(`Invalid ${arg} value: "${raw}" (expected a positive integer)`); }
      } else { parseErrors.push(`${arg} requires a value`); }
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

  return {
    subcommand, positionalArgs, json, theme, ticker, interval, since, minConfidence, minEdge, side,
    live, refresh, report, dryRun, verbose, performance, resolved, unresolved, days, maxAge, category,
    limit, exportPath, minVolume, minPrice, maxPrice,
    topK, behavioral, ranked, labelContains, closeBefore, windowDays, correlationInterval, timeframe,
    weights, bankroll, kellyMultiplier, n, maxPerCluster, maxCorrelation, minReturn, seriesTicker,
    sortBy, probabilities, tickers, query, showCluster, aggregateBy, activeOnly,
    seriesPrefix, sides, cells, autoProbs, daysToClose,
    parseErrors,
  };
}
