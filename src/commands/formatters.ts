import type { KalshiMarket, KalshiPosition, KalshiOrder } from '../tools/kalshi/types.js';

// ─── Box header helper ───────────────────────────────────────────────────────

const BOX_WIDTH = 40;

export function formatBoxHeader(title: string): string[] {
  const inner = BOX_WIDTH - 2; // space between ║ walls
  const safeTitle = title.length > inner ? title.slice(0, inner - 1) + '…' : title;
  const pad = inner - safeTitle.length;
  const left = Math.floor(pad / 2);
  const right = pad - left;
  return [
    '',
    '╔' + '═'.repeat(inner) + '╗',
    '║' + ' '.repeat(left) + safeTitle + ' '.repeat(right) + '║',
    '╚' + '═'.repeat(inner) + '╝',
  ];
}

/** Actual Kalshi /portfolio/balance response shape */
export interface KalshiBalanceResponse {
  balance: number;
  portfolio_value: number;
  updated_ts?: number;
  // Legacy fields (may be present in some API versions)
  payout?: number;
  reserved_fees?: number;
  fees?: number;
}

// ─── Value parsers ────────────────────────────────────────────────────────────
// Kalshi API returns prices as "_dollars" string fields (e.g. "0.5600")
// or as integer cents in older API versions. Handle both.

function parseDollars(val: string | number | undefined | null): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === 'number' ? val : parseFloat(val as string);
  return isNaN(n) ? undefined : n;
}

function parsePosition(val: string | number | undefined | null): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === 'number' ? val : parseFloat(val as string);
  return isNaN(n) ? undefined : n;
}

/** Format a dollar amount (already in dollars, not cents) */
function fmtDollars(val: string | number | undefined | null): string {
  const n = parseDollars(val);
  if (n === undefined) return '-';
  return `$${n.toFixed(2)}`;
}

/** Format a price field that may be integer cents OR a dollars string */
function fmtPrice(val: number | string | undefined | null): string {
  if (val === undefined || val === null) return '-';
  if (typeof val === 'string') {
    const n = parseFloat(val);
    if (isNaN(n)) return '-';
    // If the string looks like "0.5600" (dollars format), show as-is
    return `$${n.toFixed(2)}`;
  }
  // Integer cents (old API format): divide by 100
  return `$${(val / 100).toFixed(2)}`;
}

/** Format a dollar amount from cents (integer) */
function fmtCents(cents: number | undefined | null): string {
  if (cents === undefined || cents === null) return '-';
  return `$${(cents / 100).toFixed(2)}`;
}

/** Format a number with commas, safely handling null/undefined */
function fmtNum(n: number | string | undefined | null): string {
  if (n === undefined || n === null) return '-';
  const val = typeof n === 'number' ? n : parseFloat(n as string);
  if (isNaN(val)) return '-';
  if (val === 0) return '0';
  return val.toLocaleString();
}

/** Format ISO date string as short date */
function fmtDate(iso: string | undefined): string {
  if (!iso) return '-';
  try {
    return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' });
  } catch {
    return iso.slice(0, 10);
  }
}

// ─── Access helpers (handle both _dollars and raw field names) ────────────────

function mktYesAsk(m: any): string | number | undefined {
  return m.yes_ask_dollars ?? m.dollar_yes_ask ?? m.yes_ask;
}
function mktNoAsk(m: any): string | number | undefined {
  return m.no_ask_dollars ?? m.dollar_no_ask ?? m.no_ask;
}
function mktYesBid(m: any): string | number | undefined {
  return m.yes_bid_dollars ?? m.dollar_yes_bid ?? m.yes_bid;
}
function mktNoBid(m: any): string | number | undefined {
  return m.no_bid_dollars ?? m.dollar_no_bid ?? m.no_bid;
}
function mktLastPrice(m: any): string | number | undefined {
  return m.last_price_dollars ?? m.dollar_last_price ?? m.last_price;
}
function mktVolume(m: any): string | number | undefined {
  return m.volume_fp ?? m.volume;
}
function mktOpenInterest(m: any): string | number | undefined {
  return m.open_interest_fp ?? m.open_interest;
}

// ─── Formatters ───────────────────────────────────────────────────────────────

export function formatBalance(data: KalshiBalanceResponse): string {
  const lines: string[] = [];
  lines.push('**Account Balance**');
  lines.push('');
  lines.push(`Balance:         ${fmtCents(data.balance)}`);
  lines.push(`Portfolio Value: ${fmtCents(data.portfolio_value ?? 0)}`);
  if (data.payout !== undefined) lines.push(`Payout:          ${fmtCents(data.payout)}`);
  if (data.reserved_fees !== undefined) lines.push(`Reserved Fees:   ${fmtCents(data.reserved_fees)}`);
  if (data.fees !== undefined) lines.push(`Total Fees:      ${fmtCents(data.fees)}`);
  return lines.join('\n');
}

export function formatPositions(positions: any[]): string {
  if (!positions.length) return 'No open positions.';

  const rows = positions.map((p) => {
    // position_fp is the net position (number of contracts)
    const pos = parsePosition(p.position_fp ?? p.position);
    const posStr = pos === undefined ? '-' : pos > 0 ? `+${pos}` : String(pos);
    const pnl = p.realized_pnl_dollars ?? (p.realized_pnl !== undefined ? (p.realized_pnl / 100).toFixed(2) : undefined);
    const exposure = p.market_exposure_dollars ?? (p.market_exposure !== undefined ? (p.market_exposure / 100).toFixed(2) : undefined);

    return [
      p.ticker,
      posStr,
      fmtDollars(pnl),
      fmtDollars(exposure),
    ];
  });

  return formatTable(
    ['Ticker', 'Position', 'Realized P&L', 'Exposure'],
    rows
  );
}

export function formatOrders(orders: KalshiOrder[]): string {
  if (!orders.length) return 'No orders found.';

  const rows = orders.map((o) => {
    const price = o.yes_price_dollars
      ? fmtDollars(o.yes_price_dollars)
      : o.yes_price != null ? fmtCents(o.yes_price) : '-';
    const remaining = o.remaining_count_fp ?? o.remaining_count ?? '-';
    const initial = o.initial_count_fp ?? o.contracts_count ?? '-';
    return [
      o.ticker,
      `${o.action}/${o.side}`,
      price,
      `${remaining}/${initial}`,
      o.status,
      (o.order_id ?? '').slice(0, 8) + '…',
    ];
  });

  return formatTable(
    ['Ticker', 'Action/Side', 'Price', 'Remaining', 'Status', 'Order ID'],
    rows
  );
}

export function formatMarkets(markets: any[]): string {
  if (!markets.length) return 'No markets found.';

  const rows = markets.map((m) => [
    m.ticker,
    truncate(m.title ?? '', 40),
    fmtPrice(mktYesAsk(m)),
    fmtPrice(mktNoAsk(m)),
    fmtNum(mktVolume(m)),
    fmtDate(m.close_time),
  ]);

  return formatTable(
    ['Ticker', 'Title', 'YES Ask', 'NO Ask', 'Volume', 'Closes'],
    rows
  );
}

export function formatMarketDetail(market: any): string {
  const lines: string[] = [];
  lines.push(`**${market.ticker}**`);
  if (market.title) lines.push(market.title);
  if (market.subtitle) lines.push(market.subtitle);
  lines.push('');
  lines.push(`Status:     ${market.status ?? '-'}`);
  lines.push(`YES Bid:    ${fmtPrice(mktYesBid(market))}   YES Ask: ${fmtPrice(mktYesAsk(market))}`);
  lines.push(`NO Bid:     ${fmtPrice(mktNoBid(market))}   NO Ask:  ${fmtPrice(mktNoAsk(market))}`);
  lines.push(`Last Price: ${fmtPrice(mktLastPrice(market))}`);
  lines.push(`Volume:     ${fmtNum(mktVolume(market))}   Open Interest: ${fmtNum(mktOpenInterest(market))}`);
  lines.push(`Closes:     ${fmtDate(market.close_time)}`);
  if (market.result) lines.push(`Result:     ${market.result}`);
  return lines.join('\n');
}

export function formatExchangeStatus(data: Record<string, unknown>): string {
  const active = data.exchange_active ? '✓ Exchange Active' : '✗ Exchange Inactive';
  const trading = data.trading_active ? '✓ Trading Active' : '✗ Trading Paused';
  return `${active}\n${trading}`;
}

export function formatOrderConfirmation(
  ticker: string,
  action: 'buy' | 'sell',
  side: 'yes' | 'no',
  count: number,
  price: number | undefined
): string {
  const priceStr = price !== undefined ? `$${(price / 100).toFixed(2)}` : 'market price';
  const estCost = price !== undefined ? `$${((price / 100) * count).toFixed(2)}` : 'variable';
  const lines = [
    '**Order Preview**',
    '',
    `Ticker:  ${ticker}`,
    `Action:  ${action.toUpperCase()} ${side.toUpperCase()}`,
    `Count:   ${count} contract${count !== 1 ? 's' : ''}`,
    `Price:   ${priceStr}`,
    `Est. Cost: ${estCost}`,
  ];
  return lines.join('\n');
}

export function formatEvents(events: any[]): string {
  if (!events.length) return 'No events found.';

  const rows = events.map((e) => {
    const markets = e.markets ?? [];
    const marketCount = markets.length > 0 ? String(markets.length) : '-';

    // Find the leading outcome (highest YES price) for the top outcome column
    let topOutcome = '-';
    let topPct = '-';
    if (markets.length > 0) {
      // For multi-market events, show the frontrunner
      // For binary events (1 market), show the YES probability
      const sorted = [...markets].sort((a: any, b: any) => {
        const volA = parseFloat(a.volume_fp ?? a.volume ?? '0') || 0;
        const volB = parseFloat(b.volume_fp ?? b.volume ?? '0') || 0;
        return volB - volA;
      });
      const top = sorted[0];
      // Handle both dollar strings ("0.1800") and integer cents (18)
      const rawAsk = top.yes_ask_dollars ?? top.yes_ask;
      let yesAsk = 0;
      if (rawAsk !== undefined && rawAsk !== null) {
        const n = parseFloat(String(rawAsk));
        yesAsk = !isNaN(n) ? (n > 1 ? n / 100 : n) : 0;
      }
      topOutcome = truncate(top.yes_sub_title || top.subtitle || top.ticker?.split('-').pop() || '', 25);
      if (yesAsk > 0) topPct = `${Math.round(yesAsk * 100)}%`;
    }

    return [
      e.event_ticker,
      truncate(e.title ?? '', 35),
      marketCount,
      topOutcome,
      topPct,
    ];
  });

  return formatTable(
    ['Ticker', 'Title', 'Mkts', 'Top Outcome', 'YES'],
    rows
  );
}

export function formatEventDetail(event: any): string {
  const lines: string[] = [];
  lines.push(`**${event.event_ticker}**`);
  if (event.title) lines.push(event.title);
  if (event.sub_title) lines.push(event.sub_title);
  lines.push('');
  lines.push(`Series:   ${event.series_ticker ?? '-'}`);
  lines.push(`Category: ${event.category ?? '-'}`);
  lines.push(`Strike:   ${fmtDate(event.strike_date)}`);
  if (event.mutually_exclusive !== undefined) {
    lines.push(`Mutually Exclusive: ${event.mutually_exclusive ? 'Yes' : 'No'}`);
  }

  const markets = event.markets ?? [];
  if (markets.length > 0) {
    lines.push('');
    lines.push(`**Markets (${markets.length})**`);
    const rows = markets.map((m: any) => [
      m.ticker,
      truncate(m.title ?? m.subtitle ?? '', 35),
      fmtPrice(mktYesAsk(m)),
      fmtPrice(mktNoAsk(m)),
      fmtNum(mktVolume(m)),
    ]);
    lines.push(formatTable(['Ticker', 'Title', 'YES Ask', 'NO Ask', 'Volume'], rows));
  }

  return lines.join('\n');
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function formatTable(headers: string[], rows: string[][]): string {
  const colWidths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => (r[i] ?? '').length))
  );

  const pad = (s: string, w: number) => s.padEnd(w);
  const sep = '─';

  const topBorder = '┌' + colWidths.map((w) => sep.repeat(w + 2)).join('┬') + '┐';
  const headerRow = '│' + headers.map((h, i) => ` ${pad(h, colWidths[i])} `).join('│') + '│';
  const midBorder = '├' + colWidths.map((w) => sep.repeat(w + 2)).join('┼') + '┤';
  const bottomBorder = '└' + colWidths.map((w) => sep.repeat(w + 2)).join('┴') + '┘';

  const dataRows = rows.map(
    (row) => '│' + colWidths.map((w, i) => ` ${pad(row[i] ?? '', w)} `).join('│') + '│'
  );

  return [topBorder, headerRow, midBorder, ...dataRows, bottomBorder].join('\n');
}
