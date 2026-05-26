/**
 * Typed wrappers over Octagon's Kalshi search/clusters/correlation/basket API.
 * Endpoints live under https://api.octagonai.co/v1/prediction-markets/kalshi/*.
 *
 * Mirrors the pattern in octagon-events-api.ts:
 * - Fetch + Authorization: Bearer ${OCTAGON_API_KEY}
 * - 60s AbortController timeout per request
 * - Non-2xx → Error with status + body excerpt
 *
 * All endpoints are stateless from the CLI's perspective — no SQLite caching.
 */

const KALSHI_API_BASE = 'https://api.octagonai.co/v1/prediction-markets/kalshi';
const TIMEOUT_MS = 60_000;

function buildQuery(params?: object): string {
  if (!params) return '';
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === '') continue;
    usp.set(k, String(v));
  }
  const s = usp.toString();
  return s ? `?${s}` : '';
}

async function kalshiApi<T>(
  method: 'GET' | 'POST',
  path: string,
  opts?: {
    params?: object;
    body?: unknown;
  },
): Promise<T> {
  const apiKey = process.env.OCTAGON_API_KEY;
  if (!apiKey) {
    throw new Error('OCTAGON_API_KEY not set. Get one at https://app.octagonai.co');
  }

  const url = `${KALSHI_API_BASE}${path}${method === 'GET' ? buildQuery(opts?.params) : ''}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  let resp: Response;
  try {
    resp = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        ...(method === 'POST' ? { 'Content-Type': 'application/json' } : {}),
      },
      ...(method === 'POST' && opts?.body !== undefined ? { body: JSON.stringify(opts.body) } : {}),
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    let detail = body.slice(0, 300);
    try {
      const parsed = JSON.parse(body) as { detail?: unknown };
      if (typeof parsed.detail === 'string') detail = parsed.detail;
    } catch {
      // body wasn't JSON — fall through with text excerpt
    }
    throw new Error(`Octagon Kalshi API ${resp.status} (${method} ${path}): ${detail}`);
  }

  return (await resp.json()) as T;
}

// ─── Response shapes ────────────────────────────────────────────────────────

export interface KalshiMarketRow {
  market_ticker: string;
  event_ticker: string;
  series_ticker?: string | null;
  title: string;
  subtitle?: string | null;
  yes_subtitle?: string | null;
  no_subtitle?: string | null;
  status: string;
  close_time: string | null;
  last_price?: number | null;
  yes_bid?: number | null;
  yes_ask?: number | null;
  no_bid?: number | null;
  no_ask?: number | null;
  volume?: number | null;
  volume_24h?: number | null;
  liquidity?: number | null;
  open_interest?: number | null;
  category?: string | null;
  event_name?: string | null;
}

export interface PagedResult<T> {
  data: T[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface SimilarMarketRow extends KalshiMarketRow {
  distance: number;
}

export interface ClusterRow {
  cluster_id: number;
  label: string;
  description: string;
  size: number;
  sample_titles: string[];
  created_at: string;
  mean_daily_return?: number;
  daily_volatility?: number;
}

export interface ClusterMembership {
  market_ticker: string;
  thematic: {
    cluster_id: number;
    label: string;
    description: string;
    size: number;
  } | null;
  behavioral: {
    cluster_id: number;
    label: string;
    size: number;
    mean_daily_return?: number;
    daily_volatility?: number;
  } | null;
}

export interface ClusterPeersResponse {
  market_ticker: string;
  kind: 'thematic' | 'behavioral';
  cluster: {
    cluster_id: number;
    label: string;
    description: string;
    size: number;
  };
  data: SimilarMarketRow[];
}

export interface CorrelationResponse {
  tickers: string[];
  matrix: (number | null)[][];
  ranked_pairs: { ticker_a: string; ticker_b: string; correlation: number }[];
  window_days: number;
  interval: string;
  missing: string[];
}

export interface BasketCandle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface BasketCandlesResponse {
  timeframe: string;
  interval_source: string;
  candles: BasketCandle[];
  tickers: string[];
  missing: string[];
}

export interface BasketSummary {
  total_return: number;
  annualized_return: number;
  sharpe: number | null;
  max_drawdown: number;
  win_rate: number;
  first_nav: number;
  final_nav: number;
  observation_count: number;
}

export interface BasketBacktestResponse extends BasketCandlesResponse {
  summary: BasketSummary;
}

export interface BasketSizeLeg {
  market_ticker: string;
  side: 'yes' | 'no';
  model_probability: number;
  price: number;
  edge_pp: number;
  kelly_fraction: number;
  weight: number;
  notional_usd: number;
}

export interface BasketSizeResponse {
  bankroll_usd: number;
  kelly_multiplier: number;
  total_notional: number;
  legs: BasketSizeLeg[];
}

export interface BasketBuildLeg {
  market_ticker: string;
  title: string;
  category: string | null;
  cluster_id: number | null;
  cluster_label: string | null;
  volume_24h: number | null;
  price: number | null;
  side: 'yes' | 'no';
  model_probability: number | null;
  kelly_fraction: number | null;
  weight: number | null;
  notional_usd: number | null;
}

export interface BasketBuildResponse {
  legs: BasketBuildLeg[];
  realized_max_pairwise_correlation: number | null;
  cluster_breakdown: Record<string, number>;
  dropped: { market_ticker: string; reason: string }[];
  universe_size: number;
}

export interface RankedClusterRow {
  cluster_id: number;
  label: string;
  description: string;
  size: number;
  basket_tickers: string[];
  summary: BasketSummary;
}

export interface RankedClustersResponse {
  timeframe: string;
  kind: 'thematic' | 'behavioral';
  top_n_per_cluster: number;
  min_return: number;
  data: RankedClusterRow[];
}

export interface MarketsWithEdgeRow {
  event_ticker: string;
  market_ticker?: string | null;
  title: string;
  series_category: string | null;
  model_probability: number;   // 0-100 (live API returns percentage, not fraction)
  market_probability: number;  // 0-100
  edge_pp: number;             // already in percentage points
  expected_return: number;
  confidence_score: number;
  total_volume: number;
  total_open_interest: number;
  captured_at?: string;
}

export interface MarketsWithEdgeResponse {
  run_id: string;
  captured_at: string | null;
  sort_by: string;
  data: MarketsWithEdgeRow[];
  next_cursor: string | null;
  has_more: boolean;
}

// ─── Group A — Primitives ───────────────────────────────────────────────────

export interface SearchMarketsParams {
  q?: string;
  category?: string;
  series_ticker?: string;
  series_prefix?: string;
  event_ticker?: string;
  close_before?: string;
  min_volume_24h?: number;
  sort_by?: 'volume_24h' | 'close_time' | 'last_price';
  limit?: number;
  cursor?: string;
}

export function searchKalshiMarkets(params: SearchMarketsParams): Promise<PagedResult<KalshiMarketRow>> {
  return kalshiApi<PagedResult<KalshiMarketRow>>('GET', '/markets', { params });
}

export interface SimilarParams {
  anchor_ticker?: string;
  q?: string;
  top_k?: number;
  category?: string;
  min_volume_24h?: number;
  close_before?: string;
}

export interface SimilarResponse {
  anchor_ticker: string | null;
  anchor_query: string | null;
  data: SimilarMarketRow[];
}

export function findSimilarMarkets(params: SimilarParams): Promise<SimilarResponse> {
  return kalshiApi<SimilarResponse>('GET', '/markets/similar', { params });
}

export interface ListClustersParams {
  limit?: number;
  sample_titles?: number;
  label_contains?: string;
}

export function listClusters(params: ListClustersParams = {}): Promise<{ data: ClusterRow[] }> {
  return kalshiApi<{ data: ClusterRow[] }>('GET', '/clusters', { params });
}

export function listBehavioralClusters(params: ListClustersParams = {}): Promise<{ data: ClusterRow[] }> {
  return kalshiApi<{ data: ClusterRow[] }>('GET', '/behavioral-clusters', { params });
}

export function getClusterMarkets(clusterId: number, params: { limit?: number; cursor?: string } = {}): Promise<PagedResult<SimilarMarketRow>> {
  return kalshiApi<PagedResult<SimilarMarketRow>>('GET', `/clusters/${clusterId}/markets`, { params });
}

export function getBehavioralClusterMarkets(clusterId: number, params: { limit?: number; cursor?: string } = {}): Promise<PagedResult<SimilarMarketRow>> {
  return kalshiApi<PagedResult<SimilarMarketRow>>('GET', `/behavioral-clusters/${clusterId}/markets`, { params });
}

export function getMarketClusterMembership(marketTicker: string): Promise<ClusterMembership> {
  return kalshiApi<ClusterMembership>('GET', `/markets/${encodeURIComponent(marketTicker)}/clusters`);
}

export function getClusterPeers(marketTicker: string, params: { kind?: 'thematic' | 'behavioral'; limit?: number } = {}): Promise<ClusterPeersResponse> {
  return kalshiApi<ClusterPeersResponse>('GET', `/markets/${encodeURIComponent(marketTicker)}/cluster-peers`, { params });
}

export interface CorrelationsBody {
  market_tickers: string[];
  sides?: ('yes' | 'no')[];
  include_cell_detail?: boolean;
  window_days?: number;
  interval?: '1h' | '1d';
}

export interface CorrelationCellDetail {
  ticker_a: string;
  ticker_b: string;
  correlation: number | null;
  overlap_count: number;
  reason: 'ok' | 'insufficient_overlap' | 'zero_variance';
}

export interface CorrelationResponseWithSides extends CorrelationResponse {
  sides?: ('yes' | 'no')[];
  cells_detail?: CorrelationCellDetail[] | null;
}

export function getCorrelations(body: CorrelationsBody): Promise<CorrelationResponseWithSides> {
  return kalshiApi<CorrelationResponseWithSides>('POST', '/markets/correlations', { body });
}

export interface BasketCandlesBody {
  market_tickers: string[];
  weights?: number[];
  timeframe?: '1w' | '1m' | '3m' | '6m' | '1y';
}

export function getBasketCandles(body: BasketCandlesBody): Promise<BasketCandlesResponse> {
  return kalshiApi<BasketCandlesResponse>('POST', '/baskets/candles', { body });
}

// ─── Group B — Composites ───────────────────────────────────────────────────

export interface BasketSizeBody {
  bankroll_usd: number;
  kelly_multiplier: number;
  legs: { market_ticker: string; side: 'yes' | 'no'; model_probability: number }[];
}

export function getBasketSize(body: BasketSizeBody): Promise<BasketSizeResponse> {
  return kalshiApi<BasketSizeResponse>('POST', '/baskets/size', { body });
}

export interface BasketBuildUniverse {
  q?: string;
  anchor_ticker?: string;
  market_tickers?: string[];   // explicit candidate pool (1–200); takes precedence over q/anchor
  category?: string;
  series_ticker?: string;
  min_volume_24h?: number;
  close_before?: string;
  label_contains_any?: string[];
}

export interface BasketBuildSizing {
  strategy: 'equal' | 'kelly';
  bankroll_usd?: number;
  kelly_multiplier?: number;
  leg_probabilities?: Record<string, number>;
}

export interface BasketBuildBody {
  universe: BasketBuildUniverse;
  n: number;
  max_per_cluster?: number;
  max_pairwise_correlation?: number;
  candidate_pool_size?: number;
  correlation_window_days?: number;
  sizing: BasketBuildSizing;
}

export function buildBasket(body: BasketBuildBody): Promise<BasketBuildResponse> {
  return kalshiApi<BasketBuildResponse>('POST', '/baskets/build', { body });
}

export function backtestBasket(body: BasketCandlesBody): Promise<BasketBacktestResponse> {
  return kalshiApi<BasketBacktestResponse>('POST', '/baskets/backtest', { body });
}

export interface RankedClustersParams {
  timeframe?: '1w' | '1m' | '3m' | '6m' | '1y';
  min_return?: number;
  top_n_per_cluster?: number;
  kind?: 'thematic' | 'behavioral';
  max_clusters?: number;
}

export function getClustersRankedByReturn(params: RankedClustersParams = {}): Promise<RankedClustersResponse> {
  return kalshiApi<RankedClustersResponse>('GET', '/clusters/ranked-by-return', { params });
}

export interface MarketsWithEdgeParams {
  run_id?: string;
  category?: string;
  edge_pp_min?: number;
  edge_pp_max?: number;
  expected_return_min?: number;
  total_volume_min?: number;
  model_probability_min?: number;
  sort_by?: 'edge_pp' | 'expected_return' | 'total_volume' | 'model_probability';
  limit?: number;
  cursor?: string;
}

export function getMarketsWithEdge(params: MarketsWithEdgeParams = {}): Promise<MarketsWithEdgeResponse> {
  return kalshiApi<MarketsWithEdgeResponse>('GET', '/markets-with-edge', { params });
}

// ─── Endpoints added in subsequent sessions ─────────────────────────────────

export interface PerTickerEdgeRow {
  input_ticker: string;
  market_ticker: string | null;
  event_ticker: string | null;
  title: string | null;
  series_category: string | null;
  model_probability: number | null;   // 0-1 fraction per the new endpoint doc
  market_probability: number | null;
  edge_pp: number | null;
  expected_return: number | null;
  confidence_score: number | null;
  total_volume: number | null;
  total_open_interest: number | null;
  status: 'scored' | 'unscored';
  captured_at: string | null;
}

export interface PerTickerEdgeResponse {
  run_id: string;
  captured_at: string;
  data: PerTickerEdgeRow[];
}

export function getMarketsEdge(body: { tickers: string[]; run_id?: string }): Promise<PerTickerEdgeResponse> {
  return kalshiApi<PerTickerEdgeResponse>('POST', '/markets/edge', { body });
}

export function getEventMarkets(
  eventTicker: string,
  params: { limit?: number; cursor?: string; min_volume_24h?: number } = {},
): Promise<{ event_ticker: string; data: KalshiMarketRow[]; next_cursor?: string | null; has_more?: boolean }> {
  return kalshiApi('GET', `/events/${encodeURIComponent(eventTicker)}/markets`, { params });
}

export interface SeriesRollupRow {
  series_ticker: string;
  series_title: string | null;
  market_count: number;
  active_count: number;
  total_volume_24h: number;
  dominant_category: string | null;
  categories: string[];
  last_seen_at: string;
}

export interface SeriesListParams {
  series_prefix?: string;
  category?: string;
  min_volume_24h?: number;
  sort_by?: 'total_volume_24h' | 'market_count' | 'active_count';
  limit?: number;
  cursor?: string;
}

export function listKalshiSeries(params: SeriesListParams = {}): Promise<PagedResult<SeriesRollupRow>> {
  return kalshiApi<PagedResult<SeriesRollupRow>>('GET', '/series', { params });
}

export interface SeriesEventRow {
  event_ticker: string;
  series_ticker: string;
  title: string;
  sub_title?: string | null;
  category?: string | null;
  mutually_exclusive?: boolean | null;
  available_on_brokers?: boolean | null;
  last_updated_ts?: string | null;
  kalshi_url?: string | null;
  kalshi_image_url?: string | null;
  has_report?: boolean;
  close_time?: string | null;
}

export function getSeriesEvents(
  seriesTicker: string,
  params: { limit?: number; cursor?: string; q?: string } = {},
): Promise<{ series_ticker: string; data: SeriesEventRow[]; next_cursor?: string | null; has_more?: boolean }> {
  return kalshiApi('GET', `/series/${encodeURIComponent(seriesTicker)}/events`, { params });
}

export interface ValidateBasketLeg {
  market_ticker: string;
  side: 'yes' | 'no';
  stake_usd: number;
}

export interface ValidateBasketBody {
  legs: ValidateBasketLeg[];
  bankroll_usd?: number;
  correlation_window_days?: number;
  correlation_interval?: '1h' | '1d';
  max_pairwise_correlation?: number;
  calendar_clash_window_days?: number;
}

export interface BasketValidateResponse {
  total_stake_usd: number;
  bankroll_usd: number | null;
  max_leg_pct: number;
  cluster_breakdown_thematic: Record<string, string[]>;
  cluster_breakdown_behavioral: Record<string, string[]>;
  unassigned_market_tickers: string[];
  max_pairwise_correlation: number | null;
  pairwise_correlations: { ticker_a: string; ticker_b: string; correlation: number }[];
  calendar_clashes: { window_start: string; window_end: string; market_tickers: string[] }[];
  duplicate_underliers: { event_ticker: string; market_tickers: string[] }[];
  warnings: string[];
}

export function validateBasket(body: ValidateBasketBody): Promise<BasketValidateResponse> {
  return kalshiApi<BasketValidateResponse>('POST', '/baskets/validate', { body });
}
