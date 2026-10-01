export interface KalshiBalance {
  balance: number;
  portfolio_value: number;
  updated_ts?: number;
}

export interface KalshiMarket {
  ticker: string;
  event_ticker: string;
  market_type: string;
  title: string;
  subtitle: string;
  yes_sub_title: string;
  no_sub_title: string;
  open_time: string;
  close_time: string;
  expected_expiration_time: string;
  expiration_time: string;
  latest_expiration_time: string;
  settlement_timer_seconds: number;
  status: string;
  response_price_units: string;
  notional_value: number;
  tick_size: number;
  yes_bid: number;
  yes_ask: number;
  no_bid: number;
  no_ask: number;
  last_price: number;
  previous_yes_bid: number;
  previous_yes_ask: number;
  previous_price: number;
  volume: number;
  volume_fp?: string;
  volume_24h: number;
  volume_24h_fp?: string;
  liquidity: number;
  open_interest: number;
  result: string;
  settlement_value: string;
  can_close_early: boolean;
  expiration_value: string;
  category: string;
  risk_limit_cents: number;
  strike_type: string;
  floor_strike: number;
  cap_strike: number;
  supports_fractional?: boolean;
  // New API format (yes_*_dollars)
  yes_bid_dollars?: string;
  yes_ask_dollars?: string;
  no_bid_dollars?: string;
  no_ask_dollars?: string;
  last_price_dollars?: string;
  // Legacy API format (dollar_yes_*)
  dollar_yes_bid?: string;
  dollar_yes_ask?: string;
  dollar_no_bid?: string;
  dollar_no_ask?: string;
  dollar_last_price?: string;
}

export interface KalshiEvent {
  event_ticker: string;
  series_ticker: string;
  sub_title: string;
  title: string;
  mutually_exclusive: boolean;
  category: string;
  strike_date: string;
  markets?: KalshiMarket[];
}

export interface KalshiSeries {
  ticker: string;
  frequency: string;
  title: string;
  category: string;
  tags: string[];
  settlement_sources: Array<{ url: string; name: string }>;
  contract_url: string;
}

export interface KalshiOrder {
  order_id: string;
  user_id: string;
  ticker: string;
  client_order_id?: string;
  status: string;
  yes_price_dollars?: string;
  no_price_dollars?: string;
  /** @deprecated old API field */
  yes_price?: number;
  /** @deprecated old API field */
  no_price?: number;
  created_time: string;
  expiration_time?: string | null;
  action: string;
  side: string;
  type: string;
  initial_count_fp?: string;
  remaining_count_fp?: string;
  fill_count_fp?: string;
  /** @deprecated old API field */
  contracts_count?: number;
  /** @deprecated old API field */
  remaining_count?: number;
  maker_fees_dollars?: string;
  taker_fees_dollars?: string;
  maker_fill_cost_dollars?: string;
  taker_fill_cost_dollars?: string;
  order_group_id?: string | null;
  subaccount_number?: number;
  last_update_time?: string;
}

export interface KalshiPosition {
  ticker: string;
  event_ticker: string;
  /** Legacy integer count; production no longer sends it. Read netPosition() instead. */
  position?: number;
  /** Net contracts as a fixed-point string ("10.00"): positive YES, negative NO. */
  position_fp?: string;
  resting_orders_count: number;
  market_exposure: number;
  market_exposure_dollars?: string;
  realized_pnl: number;
  realized_pnl_dollars?: string;
  total_traded: number;
  total_traded_dollars?: string;
  fees_paid: number;
  fees_paid_dollars?: string;
}

export interface KalshiFill {
  trade_id: string;
  order_id: string;
  ticker: string;
  side: string;
  action: string;
  count: number;
  yes_price: number;
  no_price: number;
  is_taker: boolean;
  created_time: string;
}

export interface KalshiOrderbookEntry {
  price: number;
  delta: number;
}

export interface KalshiOrderbook {
  ticker: string;
  yes: KalshiOrderbookEntry[];
  no: KalshiOrderbookEntry[];
}

export interface KalshiCandlestick {
  ts: number;
  yes_bid: { close: number; high: number; low: number; open: number };
  yes_ask: { close: number; high: number; low: number; open: number };
  last_price: { close: number; high: number; low: number; open: number };
  volume: number;
  open_interest: number;
}

export interface KalshiSettlement {
  ticker: string;
  settled_time: string;
  market_result: string;
  no_count: number;
  no_total_cost: number;
  yes_count: number;
  yes_total_cost: number;
  revenue: number;
}

export interface KalshiExchangeStatus {
  exchange_active: boolean;
  trading_active: boolean;
}

export interface KalshiDollarOrderRequest {
  ticker: string;
  action: "buy" | "sell";
  side: "yes" | "no";
  type: "limit" | "market";
  count: number;
  dollar_price?: string;
  expiration_ts?: number;
  client_order_id?: string;
}

export interface KalshiExchangeSchedule {
  schedule: Array<{
    open_time: string;
    close_time: string;
    maintenance_windows?: Array<{ start_time: string; end_time: string }>;
  }>;
}
