import { getDb } from '../db/index.js';
import { formatBoxHeader } from './formatters.js';
import { insertEdge } from '../db/edge.js';
import { getLatestReport } from '../db/octagon-cache.js';
import { auditTrail } from '../audit/index.js';
import { EdgeComputer } from '../scan/edge-computer.js';
import { OctagonClient } from '../scan/octagon-client.js';
import { createOctagonInvoker } from '../scan/invoker.js';
import * as readline from 'node:readline';
import { callKalshiApi, KalshiApiError } from '../tools/kalshi/api.js';
import type { KalshiMarket, KalshiEvent, KalshiOrder, KalshiPosition } from '../tools/kalshi/types.js';
import { openPosition, closePosition, getOpenPositions } from '../db/positions.js';
import { logTrade } from '../db/trades.js';
import { formatRawReport, parseMarketProb, parsePriceField } from '../controllers/browse.js';
import type { PriceDriver, Catalyst, Source } from '../scan/types.js';
import { kellySize, getVolume24h } from '../risk/kelly.js';
import type { KellyResult } from '../risk/kelly.js';
import { riskGate } from '../risk/gate.js';
import { getBotSetting } from '../utils/bot-config.js';
import type { RiskGateResult } from '../risk/gate.js';
import { formatTable } from './scan-formatters.js';

export interface AnalyzeData {
  ticker: string;
  eventTicker: string;
  title: string;
  expirationTime: string | null;
  modelLastUpdated: string | null;
  modelProb: number;
  marketProb: number;
  edge: number;
  edgePp: string;
  confidence: string;
  mispricingSignal: string;
  signal: string;
  drivers: PriceDriver[];
  catalysts: Catalyst[];
  sources: Source[];
  kelly: KellyResult;
  riskGate: RiskGateResult;
  liquidityGrade: string;
  fromCache: boolean;
  reportAge: string | null;
  reportId: string;
  rawReport: string;
  existingPosition?: { direction: 'yes' | 'no'; size: number } | null;
  closePriceCents?: number | null;
}


function deriveLiquidityGrade(market: KalshiMarket): string {
  const bid = parsePriceField(market.yes_bid_dollars, market.dollar_yes_bid, market.yes_bid);
  const ask = parsePriceField(market.yes_ask_dollars, market.dollar_yes_ask, market.yes_ask);
  const spreadCents = Number.isFinite(bid) && Number.isFinite(ask) ? Math.round((ask - bid) * 100) : 99;
  const volume = getVolume24h(market);
  if (spreadCents <= 2 && volume >= 5000) return 'Excellent';
  if (spreadCents <= 4 && volume >= 1000) return 'Good';
  return 'Poor';
}

function formatAge(epochSeconds: number): string {
  const ageMs = Date.now() - epochSeconds * 1000;
  const mins = Math.floor(ageMs / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function getVolume(m: KalshiMarket): number {
  if (m.volume_fp != null) {
    const v = parseFloat(m.volume_fp);
    if (Number.isFinite(v)) return v;
  }
  return m.volume || 0;
}

/**
 * Resolve a user-provided ticker to a market ticker.
 * Accepts: market ticker, event ticker, or series ticker.
 * Returns the resolved KalshiMarket (picking the most active open market for events/series).
 */
export async function resolveMarket(input: string): Promise<KalshiMarket> {
  // 1. Try as a market ticker first
  try {
    const res = await callKalshiApi('GET', `/markets/${input}`);
    const m = (res.market ?? res) as KalshiMarket;
    if (m.ticker) return m;
  } catch (err) {
    if (!(err instanceof KalshiApiError && err.statusCode === 404)) throw err;
  }

  // 2. Try as an event ticker
  try {
    const res = await callKalshiApi('GET', `/events/${input}`, {
      params: { with_nested_markets: true },
    });
    const ev = (res.event ?? res) as KalshiEvent;
    const markets = (ev.markets ?? []).filter(
      (m: KalshiMarket) => m.status === 'open' || m.status === 'active',
    );
    if (markets.length > 0) {
      markets.sort((a, b) => getVolume(b) - getVolume(a));
      return markets[0];
    }
  } catch (err) {
    if (!(err instanceof KalshiApiError && err.statusCode === 404)) throw err;
  }

  // 3. Try as a series ticker — fetch recent events, then get their markets
  try {
    const res = await callKalshiApi('GET', '/events', {
      params: { series_ticker: input, status: 'open', limit: 5 },
    });
    const events = (res.events ?? []) as KalshiEvent[];
    const allMarkets: KalshiMarket[] = [];
    for (const ev of events) {
      if (!ev.event_ticker) continue;
      try {
        const evRes = await callKalshiApi('GET', `/events/${ev.event_ticker}`, {
          params: { with_nested_markets: true },
        });
        const fullEv = (evRes.event ?? evRes) as KalshiEvent;
        for (const m of (fullEv.markets ?? []) as KalshiMarket[]) {
          if (m.status === 'open' || m.status === 'active') {
            allMarkets.push(m);
          }
        }
      } catch {
        // skip events that fail to fetch
      }
    }
    if (allMarkets.length > 0) {
      allMarkets.sort((a, b) => getVolume(b) - getVolume(a));
      return allMarkets[0];
    }
  } catch (err) {
    if (!(err instanceof KalshiApiError && err.statusCode === 404)) throw err;
  }

  throw new Error(`Could not find a market for "${input}". Try a full market ticker (e.g. KXBTC-26MAR14-T50049), event ticker (e.g. KXBTC-26MAR14), or series ticker (e.g. KXBTC).`);
}

export async function handleAnalyze(
  ticker: string,
  refresh = false,
  providedPosition?: { direction: 'yes' | 'no'; size: number } | null,
): Promise<AnalyzeData> {
  const db = getDb();

  // Resolve input to a market — accepts market, event, or series tickers
  const market = await resolveMarket(ticker);
  const resolvedTicker = market.ticker;
  const eventTicker = market.event_ticker;
  const marketProb = parseMarketProb(market);
  if (marketProb === null) {
    throw new Error(`No last traded price for ${resolvedTicker} — market may have no trades yet.`);
  }

  const invoker = createOctagonInvoker();
  const octagonClient = new OctagonClient(invoker, db, auditTrail);
  const edgeComputer = new EdgeComputer(db, auditTrail);

  // Use cache by default; only refresh when explicitly requested
  let variant: 'cache' | 'refresh' = refresh ? 'refresh' : 'cache';
  let report = await octagonClient.fetchReport(resolvedTicker, eventTicker, variant);

  // If cache returned no meaningful data, auto-fetch fresh
  let usedFresh = refresh;
  if (!refresh && report.cacheMiss) {
    try {
      report = await octagonClient.fetchReport(resolvedTicker, eventTicker, 'refresh');
      usedFresh = true;
    } catch (err) {
      // Auto-refresh failed — continue with cache-miss report rather than crashing
      // The user can explicitly --refresh to retry
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  ⚠ Auto-refresh failed: ${msg}`);
      console.error(`  Showing cached data. Run \`analyze ${ticker} --refresh\` to retry.`);
    }
  }

  const fromCache = !usedFresh;
  const latestDbReport = getLatestReport(db, resolvedTicker);
  const reportAge = latestDbReport ? formatAge(latestDbReport.fetched_at) : null;

  const snapshot = edgeComputer.computeEdge(resolvedTicker, report, marketProb);

  // Persist edge
  insertEdge(db, {
    ticker: snapshot.ticker,
    event_ticker: snapshot.eventTicker,
    timestamp: snapshot.timestamp,
    model_prob: snapshot.modelProb,
    market_prob: snapshot.marketProb,
    edge: snapshot.edge,
    octagon_report_id: snapshot.octagonReportId,
    drivers_json: JSON.stringify(snapshot.drivers),
    sources_json: JSON.stringify(snapshot.sources),
    catalysts_json: JSON.stringify(snapshot.catalysts),
    cache_hit: fromCache ? 1 : 0,
    cache_miss: report.cacheMiss ? 1 : 0,
    confidence: snapshot.confidence,
  });

  // Kelly sizing — wrapped in try/catch for demo mode (portfolio endpoints may 401)
  let kelly: KellyResult;
  try {
    kelly = await kellySize({
      edge: snapshot.edge,
      marketProb,
      market,
      multiplier: getBotSetting('risk.kelly_multiplier') as number | undefined,
      minEdgeThreshold: getBotSetting('risk.min_edge_threshold') as number | undefined,
    });
  } catch {
    kelly = {
      side: snapshot.edge >= 0 ? 'yes' : 'no',
      fraction: 0,
      adjustedFraction: 0,
      contracts: 0,
      dollarAmountCents: 0,
      entryPriceCents: 0,
      availableBankroll: 0,
      openExposure: 0,
      cashBalance: 0,
      portfolioValue: 0,
      liquidityAdjusted: false,
    };
  }

  // Risk gate
  const gate = riskGate({ ticker: resolvedTicker, eventTicker, kelly, market, db });

  // Use caller-provided position or fetch from API when not provided
  let existingPosition: { direction: 'yes' | 'no'; size: number } | null =
    providedPosition !== undefined ? (providedPosition ?? null) : null;
  if (providedPosition === undefined) {
    try {
      const posData = await callKalshiApi('GET', '/portfolio/positions', {
        params: { ticker: resolvedTicker },
      });
      const positions = (posData.market_positions ?? posData.positions ?? []) as KalshiPosition[];
      const match = positions.find((p) => p.ticker === resolvedTicker);
      if (match) {
        const rawPos = parseFloat(String(match.position ?? '0'));
        if (rawPos !== 0) {
          existingPosition = {
            direction: rawPos > 0 ? 'yes' : 'no',
            size: Math.abs(Math.round(rawPos)),
          };
        }
      }
    } catch {
      // Position fetch failed (e.g. demo mode) — continue without
    }
  }

  // Build signal — position-aware
  const side = snapshot.edge > 0 ? 'YES' : 'NO';
  const yesAsk = parsePriceField(market.yes_ask_dollars, market.dollar_yes_ask, market.yes_ask);
  const noAsk = parsePriceField(market.no_ask_dollars, market.dollar_no_ask, market.no_ask);
  const yesBid = parsePriceField(market.yes_bid_dollars, market.dollar_yes_bid, market.yes_bid);
  const noBid = parsePriceField(market.no_bid_dollars, market.dollar_no_bid, market.no_bid);
  const entryPrice = (snapshot.edge > 0 ? yesAsk : noAsk);

  let signal: string;
  if (existingPosition) {
    const holdDir = existingPosition.direction.toUpperCase();
    const edgeReversed =
      (existingPosition.direction === 'yes' && snapshot.edge < -0.03) ||
      (existingPosition.direction === 'no' && snapshot.edge > 0.03);
    if (edgeReversed) {
      const closePrice = existingPosition.direction === 'yes' ? yesBid : noBid;
      signal = Number.isFinite(closePrice)
        ? `SELL ${holdDir} @ $${closePrice.toFixed(2)} (close position)`
        : `SELL ${holdDir} (close position)`;
    } else {
      signal = `HOLD (long ${holdDir} ×${existingPosition.size})`;
    }
  } else {
    signal = Number.isFinite(entryPrice) ? `BUY ${side} @ $${entryPrice.toFixed(2)}` : `BUY ${side}`;
  }
  const edgePp = `${snapshot.edge >= 0 ? '+' : ''}${(snapshot.edge * 100).toFixed(0)}pp`;

  const mispricingSignal = snapshot.edge > 0.02
    ? 'underpriced'
    : snapshot.edge < -0.02
      ? 'overpriced'
      : 'fair_value';

  // Audit
  auditTrail.log({
    type: 'RECOMMENDATION',
    ticker: resolvedTicker,
    action: signal,
    size: kelly.contracts,
    kelly: kelly.adjustedFraction,
    risk_gate: gate.passed ? 'PASSED' : 'FAILED',
  });

  // Model last-updated timestamp
  const modelUpdatedAt = latestDbReport
    ? new Date(latestDbReport.fetched_at * 1000).toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
    : null;

  return {
    ticker: resolvedTicker,
    eventTicker,
    title: market.title || market.subtitle || resolvedTicker,
    expirationTime: market.expiration_time || market.expected_expiration_time || market.close_time || null,
    modelLastUpdated: modelUpdatedAt,
    modelProb: snapshot.modelProb,
    marketProb,
    edge: snapshot.edge,
    edgePp,
    confidence: snapshot.confidence,
    mispricingSignal,
    signal,
    drivers: snapshot.drivers,
    catalysts: snapshot.catalysts,
    sources: snapshot.sources,
    kelly,
    riskGate: gate,
    liquidityGrade: deriveLiquidityGrade(market),
    fromCache,
    reportAge,
    reportId: report.reportId,
    rawReport: report.rawResponse,
    existingPosition,
    closePriceCents: existingPosition
      ? Math.round((existingPosition.direction === 'yes' ? yesBid : noBid) * 100) || null
      : null,
  };
}

export function formatAnalyzeHuman(data: AnalyzeData): string {
  const lines: string[] = [];

  lines.push(...formatBoxHeader('MARKET ANALYSIS'));
  lines.push('');
  lines.push(`  Title:      ${data.title}`);
  lines.push(`  Ticker:     ${data.ticker}`);
  lines.push(`  Event:      ${data.eventTicker}`);
  if (data.expirationTime) {
    const exp = new Date(data.expirationTime);
    lines.push(`  Expires:    ${exp.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} ${exp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZoneName: 'short' })}`);
  }
  lines.push(`  Signal:     ${data.signal}`);
  if (data.existingPosition) {
    lines.push(`  Position:   ${data.existingPosition.direction.toUpperCase()} ×${data.existingPosition.size}`);
  }
  lines.push('');

  // Edge & Probabilities
  lines.push(`  Model Prob:  ${(data.modelProb * 100).toFixed(1)}%`);
  lines.push(`  Market Prob: ${(data.marketProb * 100).toFixed(1)}%`);
  lines.push(`  Edge:        ${data.edgePp} (${(data.edge * 100).toFixed(1)}%)`);
  lines.push(`  Confidence:  ${data.confidence}`);
  lines.push(`  Mispricing:  ${data.mispricingSignal}`);
  lines.push('');

  // Price Drivers
  if (data.drivers.length > 0) {
    lines.push('  Price Drivers:');
    for (const d of data.drivers) {
      const src = d.sourceUrl ? ` (${d.sourceUrl})` : '';
      lines.push(`    • [${d.impact.toUpperCase()}/${d.category}] ${d.claim}${src}`);
    }
    lines.push('');
  }

  // Catalyst Calendar
  if (data.catalysts.length > 0) {
    lines.push('  Catalyst Calendar:');
    const catRows = data.catalysts.map((c) => [
      c.date || '-',
      c.event,
      c.impact.toUpperCase(),
      c.potentialMove || '-',
    ]);
    lines.push(formatTable(
      ['Date', 'Event', 'Impact', 'Potential Move'],
      catRows,
    ));
    lines.push('');
  }

  // Kelly Sizing
  lines.push('  Position Sizing (Half-Kelly):');
  lines.push(`    Side:         ${data.kelly.side.toUpperCase()}`);
  lines.push(`    Cash Balance: $${(data.kelly.cashBalance / 100).toFixed(2)}`);
  lines.push(`    Open Exposure: $${(data.kelly.openExposure / 100).toFixed(2)}`);
  lines.push(`    Available:    $${(data.kelly.availableBankroll / 100).toFixed(2)}`);
  lines.push(`    Contracts:    ${data.kelly.contracts}`);
  lines.push(`    Dollar Amount: $${(data.kelly.dollarAmountCents / 100).toFixed(2)}`);
  lines.push(`    Entry Price:  ${data.kelly.entryPriceCents}¢`);
  lines.push(`    Kelly f*:     ${(data.kelly.fraction * 100).toFixed(1)}%`);
  lines.push(`    Adjusted f:   ${(data.kelly.adjustedFraction * 100).toFixed(1)}%`);
  if (data.kelly.liquidityAdjusted) {
    lines.push('    ⚠ Liquidity-adjusted (wide spread or low volume)');
  }
  if (data.kelly.skippedReason) {
    lines.push(`    ⚠ ${data.kelly.skippedReason}`);
  }
  lines.push('');

  // Risk Gate
  const gateIcon = data.riskGate.passed ? '✓' : '✗';
  lines.push(`  Risk Gate: ${gateIcon} ${data.riskGate.passed ? 'PASSED' : 'FAILED'}`);
  for (const check of data.riskGate.checks) {
    const icon = check.passed ? '✓' : '✗';
    lines.push(`    ${icon} ${check.name}: ${check.reason}`);
  }
  lines.push('');
  lines.push(`  Liquidity: ${data.liquidityGrade}`);

  // Sources
  if (data.sources.length > 0) {
    lines.push('');
    lines.push('  Sources:');
    for (const s of data.sources) {
      const title = s.title ? `${s.title}: ` : '';
      lines.push(`    • ${title}${s.url}`);
    }
  }

  // Cache status & model timestamp
  lines.push('');
  if (data.modelLastUpdated) {
    lines.push(`  Model Updated: ${data.modelLastUpdated}`);
  }
  if (data.fromCache && data.reportAge) {
    lines.push(`  Data: cached (${data.reportAge}). Run \`analyze ${data.ticker} --refresh\` for latest (costs 3 credits).`);
  } else if (data.fromCache) {
    lines.push(`  Data: cached. Run \`analyze ${data.ticker} --refresh\` for latest (costs 3 credits).`);
  } else {
    lines.push('  Data: freshly generated.');
  }

  return lines.join('\n');
}

/**
 * Interactive post-analyze menu. Presents options to view the full report,
 * refresh the report, or place the suggested trade.
 */
export async function promptAnalyzeActions(data: AnalyzeData): Promise<void> {
  if (!process.stdin.isTTY) return;

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const ask = (q: string) => new Promise<string>((resolve) => {
    rl.question(q, (ans) => resolve(ans.trim()));
  });

  const menu = [
    '  1) View full report',
    '  2) Refresh report (costs credits)',
    '  3) Make suggested trade',
    '  4) Exit',
  ].join('\n');

  let running = true;
  while (running) {
    console.log(`\n${menu}`);
    const choice = await ask('\n  Choose [1-4]: ');

    switch (choice) {
      case '1': {
        if (data.rawReport) {
          console.log('\n' + formatRawReport(data.rawReport, data.ticker));
        } else {
          console.log('  No report available. Try option 2 to refresh.');
        }
        break;
      }

      case '2': {
        console.log('  Fetching fresh report…');
        try {
          const freshData = await handleAnalyze(data.ticker, true);
          data = freshData;
          console.log(formatAnalyzeHuman(data));
        } catch (err) {
          console.error(`  Refresh failed: ${err instanceof Error ? err.message : String(err)}`);
        }
        break;
      }

      case '3': {
        // Determine if this is a SELL (close position) or BUY (open position)
        const isSell = data.signal.startsWith('SELL');
        const isHold = data.signal.startsWith('HOLD');

        if (isHold) {
          console.log('  Signal is HOLD — no trade suggested.');
          break;
        }

        if (isSell && data.existingPosition) {
          // Close position: sell what we hold
          const sellSide = data.existingPosition.direction;
          const sellSize = data.existingPosition.size;
          const closePrice = data.closePriceCents ?? Math.round(
            (sellSide === 'yes' ? data.marketProb : 1 - data.marketProb) * 100
          );

          console.log(`  Signal: SELL ${sellSize} ${sellSide.toUpperCase()} @ ${closePrice}¢ (close position)`);
          const confirm = await ask('  Execute? [y/n] ');
          if (confirm.toLowerCase() !== 'y' && confirm.toLowerCase() !== 'yes') {
            console.log('  Trade cancelled.');
            break;
          }

          try {
            const orderPayload: Record<string, unknown> = {
              ticker: data.ticker,
              action: 'sell',
              side: sellSide,
              type: 'limit',
              count: sellSize,
            };
            if (sellSide === 'yes') orderPayload.yes_price = closePrice;
            else orderPayload.no_price = closePrice;

            const orderRes = await callKalshiApi('POST', '/portfolio/orders', { body: orderPayload });
            const order = (orderRes.order ?? orderRes) as KalshiOrder;

            const db = getDb();
            const now = Math.floor(Date.now() / 1000);

            // Find matching open DB position for this ticker to close
            const dbPositions = getOpenPositions(db);
            const dbMatch = dbPositions.find(
              (p) => p.ticker === data.ticker && p.direction === sellSide,
            );

            logTrade(db, {
              trade_id: crypto.randomUUID(),
              position_id: dbMatch?.position_id ?? '',
              order_id: order.order_id,
              ticker: data.ticker,
              action: 'sell',
              side: sellSide,
              size: sellSize,
              price: closePrice,
              fill_status: order.status,
              kalshi_response: JSON.stringify(order),
              created_at: now,
            });

            auditTrail.log({
              type: 'TRADE_EXECUTED',
              ticker: data.ticker,
              order_id: order.order_id,
              fill_price: closePrice,
              size: sellSize,
            });

            // If order filled immediately, close the DB position
            if (dbMatch && order.status === 'filled') {
              closePosition(db, dbMatch.position_id, now);
            }

            console.log(`  Sell order placed: ${order.order_id} (${order.status})`);
          } catch (err) {
            console.error(`  Trade failed: ${err instanceof Error ? err.message : String(err)}`);
          }
          break;
        }

        if (!data.riskGate.passed) {
          console.log('  Risk gate FAILED — trade blocked.');
          break;
        }
        if (data.kelly.contracts === 0) {
          console.log(`  Kelly sizing produced 0 contracts${data.kelly.skippedReason ? `: ${data.kelly.skippedReason}` : ''}.`);
          break;
        }

        const side = data.edge > 0 ? 'yes' : 'no';
        const price = data.kelly.entryPriceCents;
        console.log(`  Signal: BUY ${data.kelly.contracts} ${side.toUpperCase()} @ ${price}¢`);
        const confirm = await ask('  Execute? [y/n] ');
        if (confirm.toLowerCase() !== 'y' && confirm.toLowerCase() !== 'yes') {
          console.log('  Trade cancelled.');
          break;
        }

        try {
          const orderPayload: Record<string, unknown> = {
            ticker: data.ticker,
            action: 'buy',
            side,
            type: 'limit',
            count: data.kelly.contracts,
          };
          if (side === 'yes') orderPayload.yes_price = price;
          else orderPayload.no_price = price;

          const orderRes = await callKalshiApi('POST', '/portfolio/orders', { body: orderPayload });
          const order = (orderRes.order ?? orderRes) as KalshiOrder;

          const db = getDb();
          const positionId = crypto.randomUUID();
          const now = Math.floor(Date.now() / 1000);

          openPosition(db, {
            position_id: positionId,
            ticker: data.ticker,
            event_ticker: data.eventTicker,
            direction: side,
            size: data.kelly.contracts,
            entry_price: price,
            entry_edge: data.edge,
            entry_kelly: data.kelly.adjustedFraction,
            current_pnl: 0,
            status: 'open',
            opened_at: now,
          });

          logTrade(db, {
            trade_id: crypto.randomUUID(),
            position_id: positionId,
            order_id: order.order_id,
            ticker: data.ticker,
            action: 'buy',
            side,
            size: data.kelly.contracts,
            price,
            fill_status: order.status,
            kalshi_response: JSON.stringify(order),
            created_at: now,
          });

          auditTrail.log({
            type: 'TRADE_EXECUTED',
            ticker: data.ticker,
            order_id: order.order_id,
            fill_price: price,
            size: data.kelly.contracts,
          });

          console.log(`  Order placed: ${order.order_id} (${order.status})`);
        } catch (err) {
          console.error(`  Trade failed: ${err instanceof Error ? err.message : String(err)}`);
        }
        break;
      }

      case '4':
      default:
        running = false;
        break;
    }
  }

  rl.close();
}
