import { callKalshiApi, fetchAllPages, supportsFractional } from "../tools/kalshi/api.js";
import type {
  KalshiBalance,
  KalshiMarket,
  KalshiPosition,
} from "../tools/kalshi/types.js";
import { getBotSetting } from "../utils/bot-config.js";

export interface KellySizeParams {
  edge: number; // octagon_prob - market_prob (signed)
  marketProb: number; // current Kalshi market probability
  multiplier?: number; // Kelly fraction, default 0.5 (half-Kelly)
  maxPositionPct?: number; // max % of bankroll per position, default 0.10
  minEdgeThreshold?: number; // min absolute edge to size, default 0.05 (5%)
  market?: KalshiMarket; // for liquidity adjustment (spread, volume)
}

export interface KellyResult {
  side: 'yes' | 'no'; // which side to buy
  fraction: number; // raw Kelly fraction (before multiplier)
  adjustedFraction: number; // after multiplier + liquidity adj
  contracts: number; // rounded to supported increment
  dollarAmountCents: number; // contracts * entry price in cents
  entryPriceCents: number; // actual entry price used (ask, not midpoint)
  availableBankroll: number; // cash - open exposure (cents)
  openExposure: number; // sum of market_exposure from positions (cents)
  cashBalance: number; // cents
  portfolioValue: number; // cents
  liquidityAdjusted: boolean;
  skippedReason?: string; // if contracts=0, explains why
}

export interface LiveBankroll {
  cashBalance: number; // cents
  portfolioValue: number; // cents
  openExposure: number; // cents
  availableBankroll: number; // cents
}

/**
 * Fetch live bankroll from Kalshi API.
 * Returns balances and open exposure in cents.
 */
export async function fetchLiveBankroll(): Promise<LiveBankroll> {
  const balanceRes = await callKalshiApi("GET", "/portfolio/balance");
  const balance = balanceRes as unknown as KalshiBalance;

  // Paginated: /portfolio/positions takes cursor + limit (max 1000) and returns
  // a cursor for the next page. A single un-cursored call understates
  // openExposure for any account past the first page — and that number feeds
  // Kelly sizing and the risk gate, so positions would be sized against a
  // bankroll that looks freer than it is. fetchAllPages warns if it hits the
  // page cap with more pages outstanding.
  const positions = await fetchAllPages<KalshiPosition>(
    "/portfolio/positions",
    { limit: 1000 },
    "market_positions",
    20,
  );

  const cashBalance = balance.balance;
  const portfolioValue = balance.portfolio_value;
  const openExposure = positions.reduce((sum, p) => {
    if (p.market_exposure_dollars != null) {
      const parsed = parseFloat(String(p.market_exposure_dollars).trim());
      if (Number.isFinite(parsed)) {
        return sum + Math.round(parsed * 100);
      }
    }
    return sum + (p.market_exposure ?? 0);
  }, 0);
  const availableBankroll = Math.max(0, cashBalance - openExposure);

  return { cashBalance, portfolioValue, openExposure, availableBankroll };
}

/** Parse a dollar-string or integer-cent price field to a decimal (0-1). */
function parsePriceField(dollarStr: string | undefined, legacyDollarStr: string | undefined, centVal: number | undefined): number {
  const d = dollarStr != null ? parseFloat(dollarStr) : legacyDollarStr != null ? parseFloat(legacyDollarStr) : NaN;
  if (Number.isFinite(d)) return d;
  if (centVal != null && Number.isFinite(centVal)) return centVal / 100;
  return NaN;
}

/** Get 24h volume from a market, handling both volume_24h_fp (string) and legacy volume_24h (number). */
export function getVolume24h(market: KalshiMarket): number {
  if (market.volume_24h_fp != null) {
    const v = parseFloat(market.volume_24h_fp);
    if (Number.isFinite(v)) return v;
  }
  if (market.volume_24h != null && Number.isFinite(market.volume_24h)) return market.volume_24h;
  return 0;
}

/** Get the bid/ask spread in cents from a market, handling dollar-string fields. */
export function getSpreadCents(market: KalshiMarket): number {
  const bid = parsePriceField(market.yes_bid_dollars, market.dollar_yes_bid, market.yes_bid);
  const ask = parsePriceField(market.yes_ask_dollars, market.dollar_yes_ask, market.yes_ask);
  if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.round((ask - bid) * 100);
  return 99; // unknown spread → treat as very wide
}

/**
 * Compute Kelly-optimal position size using live Kalshi portfolio data.
 * All amounts in cents (Kalshi's native unit).
 *
 * For YES bets (edge > 0): f* = edge / (1 - marketProb)
 * For NO bets  (edge < 0): f* = |edge| / marketProb
 */
export async function kellySize(params: KellySizeParams): Promise<KellyResult> {
  const { edge, marketProb, market } = params;
  const multiplier = params.multiplier ?? (getBotSetting('risk.kelly_multiplier') as number);
  const maxPositionPct = params.maxPositionPct ?? (getBotSetting('risk.max_position_pct') as number);
  const minEdgeThreshold = params.minEdgeThreshold ?? (getBotSetting('risk.min_edge_threshold') as number);

  const bankroll = await fetchLiveBankroll();
  const { cashBalance, portfolioValue, openExposure, availableBankroll } =
    bankroll;

  const side: 'yes' | 'no' = edge >= 0 ? 'yes' : 'no';

  // Compute executable probability from the ask price we'd actually trade at.
  // YES buy → yes_ask; NO buy → no_ask expressed as YES-equivalent (1 - no_ask)
  let executableProb: number | null = null;
  if (market) {
    if (side === 'yes') {
      const ask = parsePriceField(market.yes_ask_dollars, market.dollar_yes_ask, market.yes_ask);
      if (Number.isFinite(ask) && ask > 0) executableProb = ask;
    } else {
      const noAsk = parsePriceField(market.no_ask_dollars, market.dollar_no_ask, market.no_ask);
      if (Number.isFinite(noAsk) && noAsk > 0) executableProb = 1 - noAsk;
    }
  }
  // Fall back to midpoint if no executable quote is available
  const pricingProb = executableProb ?? marketProb;

  // Recompute edge relative to executable quote to avoid overstating edge;
  // when no executable quote is available, use the original edge directly
  // to avoid floating-point roundtrip error from (marketProb + edge) - marketProb.
  const executableEdge = executableProb != null
    ? (marketProb + edge) - executableProb
    : edge;
  const absEdge = Math.abs(executableEdge);

  // Entry price from executable quote — computed early so it's available even when sizing is skipped
  let entryPriceCents: number;
  if (side === 'yes') {
    entryPriceCents = executableProb != null ? Math.round(executableProb * 100) : Math.round(marketProb * 100);
  } else {
    entryPriceCents = executableProb != null ? Math.round((1 - executableProb) * 100) : Math.round((1 - marketProb) * 100);
  }

  const makeResult = (overrides: Partial<KellyResult> = {}): KellyResult => ({
    side,
    fraction: 0,
    adjustedFraction: 0,
    contracts: 0,
    dollarAmountCents: 0,
    entryPriceCents,
    availableBankroll,
    openExposure,
    cashBalance,
    portfolioValue,
    liquidityAdjusted: false,
    ...overrides,
  });

  // Minimum edge threshold — don't size if edge is within model error
  if (absEdge < minEdgeThreshold) {
    return makeResult({ skippedReason: `Edge ${(absEdge * 100).toFixed(1)}% below ${(minEdgeThreshold * 100).toFixed(0)}% threshold` });
  }

  // Guard against extreme probabilities that would cause division by zero
  if (pricingProb <= 0 || pricingProb >= 1) {
    return makeResult({ skippedReason: 'Extreme probability — cannot size' });
  }

  // Kelly formula for binary outcome using executable quote
  // YES: f* = executableEdge / (1 - pricingProb)  — cost is pricingProb, payoff is (1 - pricingProb)
  // NO:  f* = |executableEdge| / pricingProb       — cost is (1 - pricingProb), payoff is pricingProb
  const fraction = side === 'yes'
    ? executableEdge / (1 - pricingProb)
    : absEdge / pricingProb;

  let adjustedFraction = fraction * multiplier;
  let liquidityAdjusted = false;

  // Liquidity adjustment: wide spread or low volume → apply haircut
  if (market) {
    const spreadCents = getSpreadCents(market);
    const liqSpreadThreshold = getBotSetting('risk.liquidity_spread_threshold') as number;
    const liqVolumeThreshold = getBotSetting('risk.liquidity_volume_threshold') as number;
    const liqHaircut = getBotSetting('risk.liquidity_haircut') as number;
    if (spreadCents > liqSpreadThreshold || getVolume24h(market) < liqVolumeThreshold) {
      adjustedFraction *= liqHaircut;
      liquidityAdjusted = true;
    }
  }

  // Dollar amount before position cap
  let dollarAmountCents = Math.floor(adjustedFraction * availableBankroll);

  // Cap at maxPositionPct of available bankroll
  const maxDollar = Math.floor(maxPositionPct * availableBankroll);
  dollarAmountCents = Math.min(dollarAmountCents, maxDollar);

  let contracts = 0;
  if (entryPriceCents > 0 && dollarAmountCents > 0) {
    if (market && supportsFractional(market) && market.tick_size > 0) {
      const rawContracts = dollarAmountCents / entryPriceCents;
      contracts =
        Math.floor(rawContracts / market.tick_size) * market.tick_size;
    } else {
      contracts = Math.floor(dollarAmountCents / entryPriceCents);
    }
  }

  const skippedReason = contracts === 0
    ? (availableBankroll === 0
      ? 'No available bankroll'
      : entryPriceCents === 0
        ? 'Entry price rounds to zero'
        : 'Position too small for bankroll size')
    : undefined;

  // Recalculate dollar amount based on actual contracts
  dollarAmountCents = contracts * entryPriceCents;

  return makeResult({
    fraction,
    adjustedFraction,
    contracts,
    dollarAmountCents,
    entryPriceCents,
    liquidityAdjusted,
    skippedReason,
  });
}
