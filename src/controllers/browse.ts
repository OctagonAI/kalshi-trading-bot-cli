import { getDb } from '../db/index.js';
import { getLatestEdge, insertEdge } from '../db/edge.js';
import { getLatestReport, updateReportModelProb } from '../db/octagon-cache.js';
import { auditTrail } from '../audit/index.js';
import { OctagonClient } from '../scan/octagon-client.js';
import { EdgeComputer } from '../scan/edge-computer.js';
import { createOctagonInvoker } from '../scan/invoker.js';
import { callKalshiApi } from '../tools/kalshi/api.js';
import { callOctagon } from '../scan/invoker.js';
import { ensureIndex, onIndexProgress, getRefreshPromise } from '../tools/kalshi/search-index.js';
import { getEventsFromIndex, getTopEventsByVolume, getIndexAge } from '../db/event-index.js';
import { resolveMarket } from '../commands/analyze.js';
import type { KalshiEvent, KalshiMarket } from '../tools/kalshi/types.js';
import { trackEvent } from '../utils/telemetry.js';

/** Maps lowercase theme IDs to exact Kalshi category labels (inlined to avoid heavy theme-resolver import) */
const CATEGORY_MAP: Record<string, string> = {
  climate: 'Climate and Weather',
  companies: 'Companies',
  crypto: 'Crypto',
  economics: 'Economics',
  elections: 'Elections',
  entertainment: 'Entertainment',
  financials: 'Financials',
  health: 'Health',
  mentions: 'Mentions',
  politics: 'Politics',
  science: 'Science and Technology',
  social: 'Social',
  sports: 'Sports',
  transportation: 'Transportation',
  world: 'World',
};

/** Minimal market shape needed by parseMarketProb and isMarketActive */
export interface MarketRow {
  last_price_dollars?: string | null;
  dollar_last_price?: string | null;
  last_price?: number | null;
  yes_bid_dollars?: string | null;
  dollar_yes_bid?: string | null;
  yes_ask_dollars?: string | null;
  dollar_yes_ask?: string | null;
  yes_bid?: number | null;
  yes_ask?: number | null;
  response_price_units?: string | null;
  status?: string | null;
  result?: string | null;
  volume_24h?: number | string | null;
}

/** Parse a dollar or cent price field into a decimal probability (0-1).
 *  Checks both new (yes_bid_dollars) and legacy (dollar_yes_bid) API field names. */
export function parsePriceField(newDollar: string | undefined | null, legacyDollar: string | undefined | null, centVal: number | undefined | null): number {
  if (newDollar != null) {
    const d = parseFloat(String(newDollar).trim());
    if (Number.isFinite(d)) return d;
  }
  if (legacyDollar != null) {
    const d = parseFloat(String(legacyDollar).trim());
    if (Number.isFinite(d)) return d;
  }
  if (centVal != null && Number.isFinite(centVal)) return centVal / 100;
  return NaN;
}

/** Parse a market probability from last traded price.
 *  Returns null if no last_price is available — callers should display "—" or skip the market.
 *  Does NOT fall back to bid/ask mid, which misrepresents where the market is actually trading. */
export function parseMarketProb(m: MarketRow): number | null {
  // Check all three API field name variants: last_price_dollars (new), dollar_last_price (legacy), last_price (cents)
  const dollarStr = m.last_price_dollars ?? m.dollar_last_price;
  if (dollarStr != null) {
    const d = parseFloat(String(dollarStr));
    if (Number.isFinite(d) && d > 0) return d;
  }
  if (m.last_price != null && m.last_price > 0) return m.last_price / 100;
  return null;
}

/** Check if a market is actively tradeable: open/active, not resolved, and has at least one trade */
export function isMarketActive(m: MarketRow): boolean {
  // Must be in a tradeable state
  if (m.status !== 'open' && m.status !== 'active') return false;
  // Must not be resolved
  if (m.result && m.result !== '') return false;
  // Must have recent trading activity (volume_24h > 0)
  // Markets with zero 24h volume have stale last_price from old trades
  const vol24h = typeof m.volume_24h === 'string'
    ? parseFloat(m.volume_24h)
    : (m.volume_24h ?? 0);
  if (m.volume_24h != null && vol24h <= 0) return false;
  // Must have at least one actual trade (last_price > 0)
  // If last_price is absent (old index row), fall through and allow it
  const lastPrice = m.last_price ?? 0;
  const dollarStr = m.last_price_dollars ?? m.dollar_last_price;
  const parsedDollar = dollarStr != null ? parseFloat(String(dollarStr)) : NaN;
  const lastPriceDollar = Number.isFinite(parsedDollar) ? parsedDollar : 0;
  if (lastPrice === 0 && lastPriceDollar === 0) {
    // Transition fallback: if all last_price fields are missing entirely (not zero),
    // allow the market through so old index rows still appear
    if (m.last_price == null && dollarStr == null) return true;
    return false;
  }
  return true;
}

export interface BrowseMarketRow {
  ticker: string;
  title: string;
  marketProb: number | null;
  modelProb: number | null;
  edge: number | null;
  confidence: string | null;
}

export interface BrowseEventRow {
  eventTicker: string;
  title: string;
  category: string;
  markets: BrowseMarketRow[];
  pending?: boolean;
}

export type BrowseAppState = 'idle' | 'loading' | 'event_list' | 'action_menu' | 'view_report';

export interface BrowseState {
  appState: BrowseAppState;
  theme: string;
  events: BrowseEventRow[];
  selectedMarket: BrowseMarketRow | null;
  selectedEventTicker: string | null;
  pendingRecommendTicker: string | null;
  pendingTradeTicker: string | null;
  lastError: string | null;
  progressMessage: string | null;
  reportText: string | null;
}

type ChangeListener = () => void;

/**
 * Format a raw Octagon report string for display.
 * Exported for testability — used internally by BrowseController.formatRawReport.
 */
export function formatRawReport(raw: string, ticker: string): string {
  const header = `── Octagon Report: ${ticker} ──`;
  const cleanMarkdown = (md: string) =>
    md
      .replace(/(?<=[\s:(])\/markets\//g, 'https://octagonai.co/markets/')
      .replace(/###?\s*Why This Matters\s*\(GEO\)\s*\n(?:[\s\S]*?)(?=\n##(?!#)|\n$|$)/g, '');

  try {
    const parsed = JSON.parse(raw);

    // New cache format: full markdown report in latest_report
    if (parsed.latest_report?.markdown_report) {
      return `${header}\n\n${cleanMarkdown(parsed.latest_report.markdown_report)}`;
    }

    // Legacy structured JSON fallback (versions[0])
    const source = parsed.versions?.[0] ?? parsed;
    const lines: string[] = [header, ''];
    if (source.model_probability != null) lines.push(`Model Probability: ${source.model_probability}`);
    if (source.market_probability != null) lines.push(`Market Probability: ${source.market_probability}`);
    if (source.mispricing_signal) lines.push(`Signal: ${source.mispricing_signal}`);
    if (source.key_takeaway) {
      lines.push('');
      lines.push(`Key Takeaway: ${source.key_takeaway}`);
    }
    if (source.resolution_history) {
      lines.push('');
      lines.push('Resolution History:');
      lines.push(String(source.resolution_history));
    }
    if (source.drivers && Array.isArray(source.drivers)) {
      lines.push('');
      lines.push('Drivers:');
      for (const d of source.drivers) {
        lines.push(`  • [${d.impact ?? '?'}] ${d.claim ?? d.description ?? JSON.stringify(d)}`);
      }
    }
    if (source.catalysts && Array.isArray(source.catalysts)) {
      lines.push('');
      lines.push('Catalysts:');
      for (const c of source.catalysts) {
        lines.push(`  • ${c.date ?? '?'} — ${c.event ?? c.description ?? JSON.stringify(c)}`);
      }
    }
    if (source.sources && Array.isArray(source.sources)) {
      lines.push('');
      lines.push('Sources:');
      for (const s of source.sources) {
        const title = s.title ? `${s.title}: ` : '';
        lines.push(`  • ${title}${s.url ?? JSON.stringify(s)}`);
      }
    }
    if (source.outcome_probabilities_json) {
      lines.push('');
      lines.push('Outcome Probabilities:');
      const outcomes = typeof source.outcome_probabilities_json === 'string'
        ? JSON.parse(source.outcome_probabilities_json)
        : source.outcome_probabilities_json;
      if (Array.isArray(outcomes)) {
        for (const o of outcomes) {
          lines.push(`  • ${o.market_ticker}: ${o.model_probability}`);
        }
      }
    }
    if (lines.length <= 3) {
      return `${header}\n\n${JSON.stringify(source, null, 2)}`;
    }
    return lines.join('\n');
  } catch {
    // Not JSON — raw markdown from refresh endpoint
    return `${header}\n\n${cleanMarkdown(raw)}`;
  }
}

export class BrowseController {
  private appStateValue: BrowseAppState = 'idle';
  private themeValue = '';
  private eventsValue: BrowseEventRow[] = [];
  private selectedMarketValue: BrowseMarketRow | null = null;
  private selectedEventTickerValue: string | null = null;
  private pendingRecommendTickerValue: string | null = null;
  private pendingTradeTickerValue: string | null = null;
  private lastErrorValue: string | null = null;
  private progressMessageValue: string | null = null;
  private reportTextValue: string | null = null;
  private readonly pendingReports = new Set<string>(); // event tickers with in-flight reports
  private refreshAllInFlight = false;
  private directReportMode = false; // true when entered via /report <ticker> (not browse)
  private loadToken = 0; // monotonic counter to invalidate stale async responses
  private readonly onError: (message: string) => void;
  private readonly onChange: ChangeListener;

  constructor(onError: (message: string) => void, onChange: ChangeListener) {
    this.onError = onError;
    this.onChange = onChange;
  }

  get state(): BrowseState {
    return {
      appState: this.appStateValue,
      theme: this.themeValue,
      events: this.eventsValue,
      selectedMarket: this.selectedMarketValue,
      selectedEventTicker: this.selectedEventTickerValue,
      pendingRecommendTicker: this.pendingRecommendTickerValue,
      pendingTradeTicker: this.pendingTradeTickerValue,
      lastError: this.lastErrorValue,
      progressMessage: this.progressMessageValue,
      reportText: this.reportTextValue,
    };
  }

  isInBrowseFlow(): boolean {
    return this.appStateValue !== 'idle';
  }

  consumePendingRecommendTicker(): string | null {
    const ticker = this.pendingRecommendTickerValue;
    this.pendingRecommendTickerValue = null;
    return ticker;
  }

  consumePendingTradeTicker(): string | null {
    const ticker = this.pendingTradeTickerValue;
    this.pendingTradeTickerValue = null;
    return ticker;
  }

  /** Whether the current session was started via /report (direct) vs /browse */
  get isDirectReport(): boolean {
    return this.directReportMode;
  }

  startBrowse(theme: string): void {
    trackEvent('browse_action', { action: 'start', theme });
    this.loadToken++;
    this.directReportMode = false;
    this.themeValue = theme;
    this.eventsValue = [];
    this.selectedMarketValue = null;
    this.selectedEventTickerValue = null;
    this.pendingRecommendTickerValue = null;
    this.pendingTradeTickerValue = null;
    this.lastErrorValue = null;
    this.progressMessageValue = null;
    this.refreshAllInFlight = false;
    this.pendingReports.clear();
    this.appStateValue = 'loading';
    this.emitChange();
    void this.loadEvents(theme, this.loadToken);
  }

  /**
   * Enter the report action menu directly for a given ticker.
   * Resolves market/event/series tickers and jumps to the action menu.
   */
  startReport(ticker: string): void {
    this.loadToken++;
    this.directReportMode = true;
    this.eventsValue = [];
    this.selectedMarketValue = null;
    this.selectedEventTickerValue = null;
    this.pendingRecommendTickerValue = null;
    this.pendingTradeTickerValue = null;
    this.lastErrorValue = null;
    this.progressMessageValue = null;
    this.reportTextValue = null;
    this.refreshAllInFlight = false;
    this.pendingReports.clear();
    this.themeValue = ticker;
    this.appStateValue = 'loading';
    this.emitChange();
    void this.resolveAndShowReport(ticker, this.loadToken);
  }

  private async resolveAndShowReport(ticker: string, token: number): Promise<void> {
    try {
      const market = await resolveMarket(ticker.toUpperCase());
      if (token !== this.loadToken) return;

      const db = getDb();
      const marketRow = this.toMarketRow(market, db);
      const eventTicker = market.event_ticker;

      // Store as a single-event list so runReport/handleAction work
      this.eventsValue = [{
        eventTicker,
        title: market.title ?? market.subtitle ?? eventTicker,
        category: market.category ?? '',
        markets: [marketRow],
      }];
      this.selectedMarketValue = marketRow;
      this.selectedEventTickerValue = eventTicker;
      this.appStateValue = 'action_menu';
      this.emitChange();
    } catch (err) {
      if (token !== this.loadToken) return;
      this.onError(`Report failed: ${err instanceof Error ? err.message : String(err)}`);
      this.resetToIdle();
    }
  }

  selectMarket(eventTicker: string, marketTicker: string): void {
    trackEvent('browse_action', { action: 'select_market' });
    for (const ev of this.eventsValue) {
      if (ev.eventTicker === eventTicker) {
        const market = ev.markets.find((m) => m.ticker === marketTicker);
        if (market) {
          this.selectedMarketValue = market;
          this.selectedEventTickerValue = eventTicker;
          this.appStateValue = 'action_menu';
          this.emitChange();
          return;
        }
      }
    }
  }

  handleAction(action: string): void {
    trackEvent('browse_action', { action });
    this.lastErrorValue = null;
    if (action === 'report' || action === 'refresh') {
      const forceRefresh = action === 'refresh';
      if (this.selectedMarketValue && this.selectedEventTickerValue) {
        const ticker = this.selectedMarketValue.ticker;
        const evTicker = this.selectedEventTickerValue;
        // Skip if already pending or bulk refresh is running
        if (this.pendingReports.has(evTicker) || this.refreshAllInFlight) {
          if (this.directReportMode) return; // stay on menu in direct mode
          this.selectedMarketValue = null;
          this.selectedEventTickerValue = null;
          this.appStateValue = 'event_list';
          this.emitChange();
          return;
        }
        // Mark event as pending
        this.pendingReports.add(evTicker);
        for (const ev of this.eventsValue) {
          if (ev.eventTicker === evTicker) ev.pending = true;
        }

        // Show loading message and fetch the report — display it when done
        this.progressMessageValue = forceRefresh
          ? `Generating full research report for ${ticker}... this may take several minutes.`
          : `Fetching cached report for ${ticker}...`;
        this.appStateValue = 'loading';
        this.emitChange();
        void this.runDirectReport(ticker, evTicker, forceRefresh, this.loadToken);
      }
    } else if (action === 'refresh_all') {
      if (this.refreshAllInFlight) return;
      this.selectedMarketValue = null;
      this.selectedEventTickerValue = null;
      this.appStateValue = 'event_list';
      this.emitChange();
      void this.refreshAllReports(this.loadToken);
    } else if (action === 'view_report') {
      if (this.selectedMarketValue) {
        const db = getDb();
        const report = getLatestReport(db, this.selectedMarketValue.ticker);
        if (report?.raw_response) {
          this.reportTextValue = this.formatRawReport(report.raw_response, this.selectedMarketValue.ticker);
          this.appStateValue = 'view_report';
          this.emitChange();
          return;
        }
        // No local report — fetch from Octagon cache instead
        this.handleAction('report');
        return;
      }
      this.selectedMarketValue = null;
      this.selectedEventTickerValue = null;
      this.appStateValue = 'event_list';
      this.emitChange();
    } else if (action === 'trade') {
      if (this.selectedMarketValue) {
        this.pendingTradeTickerValue = this.selectedMarketValue.ticker;
      }
      this.resetToIdle();
    } else if (action === 'no_report') {
      // No-op: no cached report available, stay on action menu
      return;
    } else if (action === 'back') {
      if (this.appStateValue === 'view_report') {
        this.reportTextValue = null;
        this.appStateValue = 'action_menu';
        this.emitChange();
        return;
      }
      if (this.directReportMode) {
        this.resetToIdle();
        return;
      }
      this.selectedMarketValue = null;
      this.selectedEventTickerValue = null;
      this.appStateValue = 'event_list';
      this.emitChange();
    }
  }

  cancelBrowse(): void {
    // Step back from view_report to action_menu instead of full exit
    if (this.appStateValue === 'view_report') {
      this.reportTextValue = null;
      this.appStateValue = 'action_menu';
      this.emitChange();
      return;
    }
    this.loadToken++; // invalidate in-flight loads and reports
    this.refreshAllInFlight = false;
    this.pendingReports.clear();
    this.resetToIdle();
  }

  private async loadEvents(theme: string, token?: number): Promise<void> {
    try {
      const db = getDb();
      let kalshiEvents: KalshiEvent[];

      const indexAge = getIndexAge(db);
      const indexEmpty = indexAge === Infinity;

      // Kick off ensureIndex (always non-blocking now)
      void ensureIndex().catch((err) => {
        console.warn(`[browse] Background index refresh failed: ${err instanceof Error ? err.message : String(err)}`);
      });

      if (theme === 'top50') {
        // Try local index first (instant if populated)
        kalshiEvents = indexEmpty ? [] : getTopEventsByVolume(db, 30);

        // Fallback to API if index is empty (first run)
        if (kalshiEvents.length === 0) {
          this.progressMessageValue = 'Fetching top markets...';
          this.emitChange();
          const data = await callKalshiApi('GET', '/events', {
            params: { status: 'open', with_nested_markets: true, limit: 100 },
          });
          kalshiEvents = (data.events ?? []) as KalshiEvent[];
          kalshiEvents.sort((a, b) => {
            const volA = (a.markets ?? []).reduce((sum: number, m: any) => sum + (parseFloat(m.volume_fp) || 0), 0);
            const volB = (b.markets ?? []).reduce((sum: number, m: any) => sum + (parseFloat(m.volume_fp) || 0), 0);
            return volB - volA;
          });
          kalshiEvents = kalshiEvents.slice(0, 30);
        }
      } else if (indexEmpty) {
        // Non-top50 theme but index is empty — must wait for index
        this.progressMessageValue = 'Building event index for the first time...';
        this.emitChange();

        // Subscribe to progress updates while waiting
        const unsub = onIndexProgress((info) => {
          if (token !== undefined && token !== this.loadToken) { unsub(); return; }
          if (info.phase === 'fetching_events') {
            this.progressMessageValue = `Indexing markets... ${info.fetchedItems} fetched (page ${info.page}/${info.maxPages})`;
          } else if (info.detail) {
            this.progressMessageValue = info.detail;
          }
          this.emitChange();
        });

        try {
          const refreshPromise = getRefreshPromise();
          if (refreshPromise) await refreshPromise;
        } finally {
          unsub();
        }

        if (token !== undefined && token !== this.loadToken) return;

        // Now query the index
        if (CATEGORY_MAP[theme]) {
          const categoryLabel = CATEGORY_MAP[theme];
          kalshiEvents = await this.searchIndex(db, '', categoryLabel);
        } else {
          const searchTerm = theme.includes(':') ? theme.split(':').slice(1).join(':') : theme;
          const categoryLabel = theme.includes(':') ? CATEGORY_MAP[theme.split(':')[0]] : null;
          kalshiEvents = await this.searchIndex(db, searchTerm, categoryLabel);
        }
      } else if (CATEGORY_MAP[theme]) {
        // Pure category (e.g. "elections") — read from local index
        const categoryLabel = CATEGORY_MAP[theme];
        kalshiEvents = await this.searchIndex(db, '', categoryLabel);
      } else {
        // Subcategory (e.g. "politics:iran") or free-text search (e.g. "iran")
        const searchTerm = theme.includes(':') ? theme.split(':').slice(1).join(':') : theme;
        const categoryLabel = theme.includes(':') ? CATEGORY_MAP[theme.split(':')[0]] : null;
        kalshiEvents = await this.searchIndex(db, searchTerm, categoryLabel);
      }

      // Sort all events by total market volume (most active first)
      kalshiEvents.sort((a, b) => {
        const volA = (a.markets ?? []).reduce((sum: number, m: any) => sum + (parseFloat(m.volume) || parseFloat(m.volume_fp) || 0), 0);
        const volB = (b.markets ?? []).reduce((sum: number, m: any) => sum + (parseFloat(m.volume) || parseFloat(m.volume_fp) || 0), 0);
        return volB - volA;
      });

      // Discard stale response if a newer browse was started
      if (token !== undefined && token !== this.loadToken) return;

      this.progressMessageValue = null;
      this.eventsValue = this.kalshiEventsToRows(kalshiEvents, db);
      this.appStateValue = 'event_list';
      this.emitChange();

      // Background: hydrate model probabilities from Octagon cache for each event
      void this.hydrateOutcomeProbs(this.loadToken);
    } catch (err) {
      if (token !== undefined && token !== this.loadToken) return;
      this.progressMessageValue = null;
      this.onError(`Browse failed: ${err instanceof Error ? err.message : String(err)}`);
      this.resetToIdle();
    }
  }

  /** Convert Kalshi events (with nested markets) to BrowseEventRows */
  private kalshiEventsToRows(events: KalshiEvent[], db: ReturnType<typeof getDb>): BrowseEventRow[] {
    const rows: BrowseEventRow[] = [];
    for (const ev of events) {
      const markets = (ev.markets ?? []).filter((m) => isMarketActive(m));
      if (markets.length === 0) continue;
      rows.push({
        eventTicker: ev.event_ticker,
        title: ev.title ?? ev.event_ticker,
        category: ev.category ?? '',
        markets: markets.map((m) => this.toMarketRow(m, db)),
      });
    }
    return rows;
  }

  private toMarketRow(m: KalshiMarket, db: ReturnType<typeof getDb>): BrowseMarketRow {
    const marketProb = parseMarketProb(m);
    let modelProb: number | null = null;
    let edge: number | null = null;
    let confidence: string | null = null;
    try {
      const latestEdge = getLatestEdge(db, m.ticker);
      // Skip edges from cache misses — valid 0.5 probabilities are shown
      if (latestEdge && !latestEdge.cache_miss) {
        modelProb = latestEdge.model_prob;
        edge = latestEdge.edge;
        confidence = latestEdge.confidence ?? null;
      }
    } catch {
      // Edge lookup failed — show without model data
    }
    return {
      ticker: m.ticker,
      title: m.title ?? m.subtitle ?? m.ticker,
      marketProb,
      modelProb,
      edge,
      confidence,
    };
  }

  private async runReport(ticker: string, eventTicker: string, forceRefresh = false, sessionToken?: number): Promise<void> {
    try {
      const db = getDb();
      const octagonClient = new OctagonClient(createOctagonInvoker(), db, auditTrail);
      const edgeComputer = new EdgeComputer(db, auditTrail);

      // Fetch current market data
      const marketRes = await callKalshiApi('GET', `/markets/${ticker}`);
      // Bail if session changed
      if (sessionToken !== undefined && sessionToken !== this.loadToken) return;

      const market = (marketRes.market ?? marketRes) as KalshiMarket;
      const marketProb = parseMarketProb(market);
      if (marketProb === null) {
        this.lastErrorValue = `No last traded price for ${ticker} — market may be untradeable.`;
        this.pendingReports.delete(eventTicker);
        for (const ev of this.eventsValue) {
          if (ev.eventTicker === eventTicker) ev.pending = false;
        }
        this.emitChange();
        return;
      }

      // Fetch octagon report: cache only unless explicitly refreshing
      const variant = forceRefresh ? 'refresh' : 'cache';
      const report = await octagonClient.fetchReport(ticker, eventTicker, variant);
      if (sessionToken !== undefined && sessionToken !== this.loadToken) return;

      // If cache miss and not a forced refresh, bail — no data to show, no credits spent
      if (!forceRefresh && report.cacheMiss) {
        this.lastErrorValue = `No cached report for ${ticker}. Use "Refresh" to generate one (costs credits).`;
        this.pendingReports.delete(eventTicker);
        for (const ev of this.eventsValue) {
          if (ev.eventTicker === eventTicker) ev.pending = false;
        }
        this.emitChange();
        return;
      }

      // Octagon analyzes the entire event — extract all outcome probabilities
      const allOutcomeProbs = await this.extractAllOutcomeProbs(ticker);
      if (sessionToken !== undefined && sessionToken !== this.loadToken) return;

      // Fix the selected market's report and persist the corrected model_prob
      const selectedProb = allOutcomeProbs.get(ticker.toUpperCase());
      if (selectedProb !== null && selectedProb !== undefined) {
        report.modelProb = selectedProb;
        // UX override: once we have a valid model probability extracted from
        // the Octagon response, treat this record as "not a cache miss" for
        // browse display purposes — even if the underlying API call was a
        // cache miss. This differs from how /analyze uses cacheMiss (to decide
        // whether to auto-refresh); here we're masking the API/database cache
        // state so the browse list shows edge data without a stale indicator.
        report.cacheMiss = false;
        if (report.reportId) {
          updateReportModelProb(db, report.reportId, selectedProb);
        }
      }

      const snapshot = edgeComputer.computeEdge(ticker, report, marketProb);

      // Always update the selected market's in-memory row directly
      for (const ev of this.eventsValue) {
        if (ev.eventTicker !== eventTicker) continue;
        const mkt = ev.markets.find(m => m.ticker === ticker);
        if (mkt) {
          mkt.modelProb = snapshot.modelProb;
          mkt.edge = snapshot.edge;
          mkt.confidence = snapshot.confidence;
        }
      }

      // Persist the selected market's edge
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
        cache_hit: snapshot.cacheHit ? 1 : 0,
        cache_miss: report.cacheMiss ? 1 : 0,
        confidence: snapshot.confidence,
      });

      // Update ALL sibling markets in the event with their outcome probabilities
      for (const ev of this.eventsValue) {
        if (ev.eventTicker !== eventTicker) continue;
        for (const mkt of ev.markets) {
          const outcomeProb = allOutcomeProbs.get(mkt.ticker.toUpperCase());
          if (outcomeProb !== null && outcomeProb !== undefined) {
            mkt.modelProb = outcomeProb;
            if (mkt.marketProb !== null) {
              mkt.edge = outcomeProb - mkt.marketProb;
              mkt.confidence = edgeComputer.classifyConfidence(Math.abs(mkt.edge));
            }

            // Persist sibling edges (skip the selected one — already persisted above)
            if (mkt.ticker !== ticker && mkt.marketProb !== null) {
              try {
                insertEdge(db, {
                  ticker: mkt.ticker,
                  event_ticker: eventTicker,
                  timestamp: snapshot.timestamp,
                  model_prob: outcomeProb,
                  market_prob: mkt.marketProb,
                  edge: mkt.edge ?? 0,
                  octagon_report_id: snapshot.octagonReportId,
                  drivers_json: null,
                  sources_json: null,
                  catalysts_json: null,
                  cache_hit: 0,
                  cache_miss: report.cacheMiss ? 1 : 0,
                  confidence: mkt.confidence,
                });
              } catch {
                // DB insert failed for sibling — update in-memory only
              }
            }
          }
        }
      }

      // Clear pending flag — skip if session changed
      if (sessionToken !== undefined && sessionToken !== this.loadToken) return;
      this.pendingReports.delete(eventTicker);
      for (const ev of this.eventsValue) {
        if (ev.eventTicker === eventTicker) ev.pending = false;
      }
      this.emitChange();
    } catch (err) {
      if (sessionToken !== undefined && sessionToken !== this.loadToken) return;
      this.lastErrorValue = `Report failed (${ticker}): ${err instanceof Error ? err.message : String(err)}`;
      this.pendingReports.delete(eventTicker);
      for (const ev of this.eventsValue) {
        if (ev.eventTicker === eventTicker) ev.pending = false;
      }
      this.emitChange();
    }
  }

  /**
   * Run a report in direct mode (/report <ticker>).
   * After completion, show the full report view instead of returning to event list.
   */
  private async runDirectReport(ticker: string, eventTicker: string, forceRefresh: boolean, sessionToken: number): Promise<void> {
    // Run the normal report flow first
    await this.runReport(ticker, eventTicker, forceRefresh, sessionToken);
    if (sessionToken !== this.loadToken) return;

    // After report completes, show the report view if we have a raw_response
    const db = getDb();
    const report = getLatestReport(db, ticker);
    if (report?.raw_response) {
      this.reportTextValue = this.formatRawReport(report.raw_response, ticker);
      this.progressMessageValue = null;
      this.appStateValue = 'view_report';
      this.emitChange();
    } else {
      // No raw report — go back to action menu
      this.progressMessageValue = null;
      // Restore selection for the action menu
      for (const ev of this.eventsValue) {
        const mkt = ev.markets.find(m => m.ticker === ticker);
        if (mkt) {
          this.selectedMarketValue = mkt;
          this.selectedEventTickerValue = ev.eventTicker;
          break;
        }
      }
      this.appStateValue = 'action_menu';
      this.emitChange();
    }
  }

  /**
   * Search the local event index for matching events.
   * Reads markets_json directly from the index — no API calls needed.
   */
  private async searchIndex(
    db: ReturnType<typeof getDb>,
    searchTerm: string,
    categoryLabel: string | null,
  ): Promise<KalshiEvent[]> {
    try {
      await ensureIndex();
      let rows: any[] = [];
      if (categoryLabel && !searchTerm) {
        rows = db.query(
          `SELECT event_ticker FROM event_index WHERE category = ? LIMIT 30`,
        ).all(categoryLabel);
      } else if (categoryLabel) {
        const term = `%${searchTerm.toLowerCase()}%`;
        rows = db.query(
          `SELECT event_ticker FROM event_index
           WHERE category = ? AND (LOWER(title) LIKE ? OR LOWER(event_ticker) LIKE ? OR LOWER(COALESCE(sub_title,'')) LIKE ? OR LOWER(COALESCE(series_ticker,'')) LIKE ? OR LOWER(COALESCE(tags,'')) LIKE ?)
           LIMIT 30`,
        ).all(categoryLabel, term, term, term, term, term);
      } else {
        const normalizedTerm = searchTerm.trim().toUpperCase();
        const isTicker = /^[A-Z0-9]+$/.test(normalizedTerm);
        if (isTicker) {
          rows = db.query(
            `SELECT event_ticker FROM event_index WHERE series_ticker = ? LIMIT 30`,
          ).all(normalizedTerm);
        }
        if (!rows || rows.length === 0) {
          const term = `%${searchTerm.toLowerCase()}%`;
          rows = db.query(
            `SELECT event_ticker FROM event_index
             WHERE LOWER(title) LIKE ? OR LOWER(event_ticker) LIKE ? OR LOWER(COALESCE(sub_title,'')) LIKE ? OR LOWER(COALESCE(series_ticker,'')) LIKE ? OR LOWER(COALESCE(tags,'')) LIKE ?
             LIMIT 30`,
          ).all(term, term, term, term, term);
        }
      }
      if (rows.length === 0) return [];

      // Read events with nested markets directly from the index — no API calls
      const tickers = rows.map((r: any) => r.event_ticker as string);
      return getEventsFromIndex(db, tickers);
    } catch {
      return [];
    }
  }

  /**
   * Background hydration: fetch cached Octagon outcome probabilities for each event
   * and populate Model%/Edge/Conf columns without costing credits.
   */
  private async hydrateOutcomeProbs(token: number): Promise<void> {
    // Deduplicate: pick one market ticker per event to query Octagon
    const seen = new Set<string>();
    const queries: Array<{ eventTicker: string; sampleTicker: string }> = [];
    for (const ev of this.eventsValue) {
      if (seen.has(ev.eventTicker)) continue;
      seen.add(ev.eventTicker);
      if (ev.markets.length > 0) {
        queries.push({ eventTicker: ev.eventTicker, sampleTicker: ev.markets[0].ticker });
      }
    }

    const edgeComputer = new EdgeComputer(getDb(), auditTrail);
    for (const { eventTicker, sampleTicker } of queries) {
      if (token !== this.loadToken) return; // session changed
      try {
        const probs = await this.extractAllOutcomeProbs(sampleTicker);
        if (token !== this.loadToken) return;
        if (probs.size === 0) continue;

        // Update in-memory rows for this event
        for (const ev of this.eventsValue) {
          if (ev.eventTicker !== eventTicker) continue;
          let updated = false;
          for (const mkt of ev.markets) {
            const prob = probs.get(mkt.ticker.toUpperCase());
            if (prob !== undefined && mkt.modelProb === null) {
              mkt.modelProb = prob;
              if (mkt.marketProb !== null) {
                mkt.edge = prob - mkt.marketProb;
                mkt.confidence = edgeComputer.classifyConfidence(Math.abs(mkt.edge));
              }
              updated = true;
            }
          }
          if (updated) this.emitChange();
        }
      } catch {
        // Skip this event on error
      }
    }
  }

  /**
   * Extract all outcome probabilities from the Octagon cache response.
   * Returns a map of MARKET_TICKER (uppercase) → model probability (0-1).
   */
  private async extractAllOutcomeProbs(ticker: string): Promise<Map<string, number>> {
    const probs = new Map<string, number>();
    try {
      const rawCache = await callOctagon(ticker, 'cache');
      const parsed = JSON.parse(rawCache);
      const version = parsed.versions?.[0];
      if (!version?.outcome_probabilities_json) return probs;

      const outcomes: Array<{ market_ticker: string; model_probability: number }> =
        typeof version.outcome_probabilities_json === 'string'
          ? JSON.parse(version.outcome_probabilities_json)
          : version.outcome_probabilities_json;

      // Octagon API always returns percentages (0-100); normalize each value individually
      for (const o of outcomes) {
        if (typeof o.model_probability === 'number' && o.market_ticker) {
          const prob = o.model_probability / 100;
          if (prob >= 0 && prob <= 1) {
            probs.set(o.market_ticker.toUpperCase(), prob);
          }
        }
      }
    } catch {
      // Cache extraction failed
    }
    return probs;
  }

  private async refreshAllReports(sessionToken?: number): Promise<void> {
    this.refreshAllInFlight = true;
    let total = 0;
    let succeeded = 0;
    let failed = 0;
    try {
      // Mark ALL events as pending upfront so UI shows them all immediately
      const eventsToRefresh: Array<{ ev: BrowseEventRow; ticker: string; evTicker: string }> = [];
      for (const ev of this.eventsValue) {
        if (ev.markets.length === 0) continue;
        const evTicker = ev.eventTicker;
        if (this.pendingReports.has(evTicker)) continue;
        eventsToRefresh.push({ ev, ticker: ev.markets[0].ticker, evTicker });
        this.pendingReports.add(evTicker);
        ev.pending = true;
      }
      total = eventsToRefresh.length;
      this.emitChange();

      // Run octagon reports for all events sequentially
      for (const { ev, ticker, evTicker } of eventsToRefresh) {
        // Bail if session changed
        if (sessionToken !== undefined && sessionToken !== this.loadToken) return;
        this.progressMessageValue = `Refreshing reports: ${succeeded + failed}/${total} done...`;
        this.emitChange();
        const errorBefore = this.lastErrorValue;
        await this.runReport(ticker, evTicker, true, sessionToken);
        if (this.lastErrorValue && this.lastErrorValue !== errorBefore) {
          failed++;
        } else {
          succeeded++;
        }
      }
    } finally {
      if (sessionToken === undefined || sessionToken === this.loadToken) {
        this.refreshAllInFlight = false;
        this.progressMessageValue = null;
        if (total > 0) {
          if (failed > 0) {
            this.progressMessageValue = `Refreshed ${succeeded}/${total} reports (${failed} failed)`;
          } else {
            this.progressMessageValue = `Refreshed all ${total} reports successfully`;
          }
          this.emitChange();
        }
      }
    }
  }

  private formatRawReport(raw: string, ticker: string): string {
    return formatRawReport(raw, ticker);
  }

  private resetToIdle(): void {
    this.appStateValue = 'idle';
    this.themeValue = '';
    this.directReportMode = false;
    this.eventsValue = [];
    this.selectedMarketValue = null;
    this.selectedEventTickerValue = null;
    this.lastErrorValue = null;
    this.progressMessageValue = null;
    this.reportTextValue = null;
    this.emitChange();
  }

  private emitChange(): void {
    this.onChange();
  }
}
