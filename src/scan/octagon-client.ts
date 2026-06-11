import type { Database } from 'bun:sqlite';
import type { AuditTrail } from '../audit/trail.js';
import {
  insertReport,
  getLatestReport,
  getTtlForCloseTime,
  isStale,
  type OctagonReport as DbOctagonReport,
} from '../db/octagon-cache.js';
import { getLatestEdge } from '../db/edge.js';
import type {
  OctagonInvoker,
  OctagonVariant,
  OctagonReport,
  MispricingSignal,
  PriceDriver,
  Catalyst,
  Source,
  DriverCategory,
  DriverImpact,
} from './types.js';
import { getBotSetting } from '../utils/bot-config.js';

const CREDITS_PER_FRESH_CALL = 3;
const DEFAULT_DAILY_CREDIT_CEILING = 150;

export class OctagonClient {
  private invoke: OctagonInvoker;
  private db: Database;
  private audit: AuditTrail;
  private dailyCreditCeiling: number;
  private creditsUsed = 0;

  constructor(
    invoke: OctagonInvoker,
    db: Database,
    audit: AuditTrail,
    config?: { dailyCreditCeiling: number }
  ) {
    this.invoke = invoke;
    this.db = db;
    this.audit = audit;
    this.dailyCreditCeiling = config?.dailyCreditCeiling
      ?? (getBotSetting('octagon.daily_credit_ceiling') as number | undefined)
      ?? DEFAULT_DAILY_CREDIT_CEILING;
  }

  /**
   * Try to build an OctagonReport from the prefetched events API data in SQLite.
   * Returns null if no fresh prefetch data is available for this event.
   * This avoids an individual Octagon cache API call when the prefetch is fresh.
   */
  tryFromPrefetch(ticker: string, eventTicker: string, closeTimeIso?: string): OctagonReport | null {
    const row = this.db.query(
      `SELECT model_prob, market_prob, mispricing_signal, drivers_json, fetched_at, expires_at,
              outcome_probabilities_json, report_id, confidence_score
       FROM octagon_reports WHERE event_ticker = $et AND variant_used = 'events-api'
       AND (close_time IS NULL OR close_time > $now)
       ORDER BY fetched_at DESC LIMIT 1`,
    ).get({ $et: eventTicker, $now: new Date().toISOString() }) as {
      model_prob: number; market_prob: number | null; mispricing_signal: string | null;
      drivers_json: string | null; fetched_at: number; expires_at: number;
      outcome_probabilities_json: string | null; report_id: string;
      confidence_score: number | null;
    } | null;

    if (!row) return null;

    // Check if the prefetch is still fresh
    const now = Math.floor(Date.now() / 1000);
    if (row.expires_at < now) return null;

    // Extract per-market probability if available
    let modelProb = row.model_prob;
    let marketProb = row.market_prob ?? 0.5;
    if (row.outcome_probabilities_json) {
      try {
        const outcomes = JSON.parse(row.outcome_probabilities_json) as Array<{
          market_ticker: string; model_probability: number; market_probability: number;
        }>;
        const match = outcomes.find(
          o => o.market_ticker.toUpperCase() === ticker.toUpperCase(),
        );
        if (match) {
          modelProb = match.model_probability / 100;
          marketProb = match.market_probability / 100;
        }
      } catch { /* malformed JSON — use event-level */ }
    }

    // Parse drivers from prefetched data
    let drivers: PriceDriver[] = [];
    if (row.drivers_json) {
      try { drivers = JSON.parse(row.drivers_json); } catch { /* skip */ }
    }

    const edge = modelProb - marketProb;
    let signal: MispricingSignal = 'fair_value';
    if (Math.abs(edge) >= 0.03) signal = edge > 0 ? 'underpriced' : 'overpriced';

    return {
      ticker,
      eventTicker,
      modelProb,
      marketProb,
      mispricingSignal: (row.mispricing_signal as MispricingSignal) ?? signal,
      drivers,
      catalysts: [],
      sources: [],
      resolutionHistory: '',
      contractSnapshot: '',
      variantUsed: 'cache',
      fetchedAt: row.fetched_at,
      rawResponse: '',
      cacheMiss: false,
      reportId: row.report_id,
    };
  }

  async fetchReport(
    ticker: string,
    eventTicker: string,
    variant: OctagonVariant,
    options?: { creditsPreReserved?: boolean; closeTimeIso?: string }
  ): Promise<OctagonReport> {
    let effectiveVariant = variant;

    // Default always uses cache — explicit 'refresh' required for fresh data
    if (variant === 'default') {
      effectiveVariant = 'cache';
    }

    // Auto-downgrade refresh to cache if budget exhausted
    // Skip when credits were pre-reserved via reserveRefresh()
    if (
      variant === 'refresh' &&
      !options?.creditsPreReserved &&
      this.creditsUsed + CREDITS_PER_FRESH_CALL > this.dailyCreditCeiling
    ) {
      effectiveVariant = 'cache';
    }

    const raw = await this.invoke(ticker, effectiveVariant);
    const report = this.parseReport(raw, ticker, eventTicker, effectiveVariant);

    // Persist to DB and record the report_id on the report object
    const closeEpoch = options?.closeTimeIso
      ? Math.floor(new Date(options.closeTimeIso).getTime() / 1000)
      : undefined;
    const dbRow = this.toDbRow(report, closeEpoch);
    insertReport(this.db, dbRow);
    report.reportId = dbRow.report_id;

    // Track credits — only 'refresh' costs credits ('default' is remapped to 'cache')
    // Skip increment when credits were pre-reserved via reserveRefresh()
    const isFresh = effectiveVariant === 'refresh';
    const credits = isFresh ? CREDITS_PER_FRESH_CALL : 0;
    if (isFresh && !options?.creditsPreReserved) {
      this.creditsUsed += CREDITS_PER_FRESH_CALL;
    }

    // Audit
    this.audit.log({
      type: 'OCTAGON_CALL',
      ticker,
      variant: effectiveVariant,
      cache_hit: effectiveVariant === 'cache',
      credits_used: credits,
    });

    return report;
  }

  parseReport(
    raw: string,
    ticker: string,
    eventTicker: string,
    variant: OctagonVariant
  ): OctagonReport {
    const now = Math.floor(Date.now() / 1000);

    const defaults: OctagonReport = {
      ticker,
      eventTicker,
      modelProb: 0.5,
      marketProb: 0.5,
      mispricingSignal: 'fair_value',
      drivers: [],
      catalysts: [],
      sources: [],
      resolutionHistory: '',
      contractSnapshot: '',
      variantUsed: variant,
      fetchedAt: now,
      rawResponse: raw,
      cacheMiss: false,
      reportId: '', // set after DB persist in fetchReport
    };

    // Phase 1: Try JSON parse
    let report: OctagonReport;
    let hasExplicitModelProb = false;
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed === 'object' && parsed !== null) {
        // Check for explicit cache-miss indicators from the API
        if (parsed.cache_miss === true || parsed.cacheMiss === true) {
          report = { ...defaults, cacheMiss: true };
          return report;
        }
        // Check if versions array is empty (cache variant returns { versions: [] } on miss)
        const versions = parsed.versions as unknown[] | undefined;
        if (Array.isArray(versions) && versions.length === 0) {
          report = { ...defaults, cacheMiss: true };
          return report;
        }
        // Check if model probability was actually provided (event-level)
        const source = (versions?.[0] ?? parsed) as Record<string, unknown>;
        hasExplicitModelProb = (source.modelProb ?? source.model_prob ?? source.model_probability) != null;
        report = this.mapJsonToReport(parsed, defaults);
        // Per-market probability from outcome_probabilities_json also counts as explicit
        if (report.modelProb !== defaults.modelProb) {
          hasExplicitModelProb = true;
        }
      } else {
        report = this.extractFromMarkdown(raw, defaults);
      }
    } catch {
      // Not JSON — fall through to regex extraction
      report = this.extractFromMarkdown(raw, defaults);
    }

    // Detect cache miss: no explicit model probability was provided AND no meaningful content
    if (!hasExplicitModelProb && report.modelProb === defaults.modelProb && report.drivers.length === 0 && report.catalysts.length === 0) {
      report.cacheMiss = true;
    }

    return report;
  }

  shouldRefresh(
    ticker: string,
    currentMarketProb: number,
    forceManual?: boolean,
    closeTimeIso?: string
  ): { refresh: boolean; reason: string } {
    // (d) Manual request
    if (forceManual) {
      return { refresh: true, reason: 'manual refresh requested' };
    }

    // (e) Time-based staleness with tiered TTL
    if (closeTimeIso) {
      const closeEpoch = Math.floor(new Date(closeTimeIso).getTime() / 1000);
      const now = Math.floor(Date.now() / 1000);
      const secondsUntilClose = closeEpoch - now;
      const ttl = getTtlForCloseTime(secondsUntilClose);
      if (isStale(this.db, ticker, undefined, closeEpoch)) {
        const tierLabel = ttl <= 3600 ? '1h' : ttl <= 21600 ? '6h' : ttl <= 86400 ? '24h' : '48h';
        return {
          refresh: true,
          reason: `stale per ${tierLabel} TTL tier (close ${closeTimeIso})`,
        };
      }
    }

    const latestEdge = getLatestEdge(this.db, ticker);

    if (latestEdge) {
      // (a) Price moved beyond threshold
      const priceMoveThreshold = getBotSetting('octagon.price_move_threshold') as number;
      const priceDelta = Math.abs(currentMarketProb - latestEdge.market_prob);
      if (priceDelta > priceMoveThreshold) {
        return {
          refresh: true,
          reason: `price moved ${(priceDelta * 100).toFixed(1)}% (>${priceMoveThreshold * 100}% threshold)`,
        };
      }

      // (b) Edge flipped sign
      const oldEdge = latestEdge.edge;
      const impliedEdge = latestEdge.model_prob - currentMarketProb;
      if (oldEdge !== 0 && impliedEdge !== 0 && Math.sign(oldEdge) !== Math.sign(impliedEdge)) {
        return { refresh: true, reason: 'edge flipped sign' };
      }
    }

    // (c) High-impact catalyst occurred
    const latestReport = getLatestReport(this.db, ticker);
    if (latestReport?.catalysts_json) {
      try {
        const parsed = JSON.parse(latestReport.catalysts_json);
        const catalysts: Catalyst[] = Array.isArray(parsed) ? parsed : [];
        const today = new Date().toISOString().slice(0, 10);
        const hasTriggered = catalysts.some(
          (c) => c.impact === 'high' && c.date <= today
        );
        if (hasTriggered) {
          return { refresh: true, reason: 'high-impact catalyst date reached' };
        }
      } catch {
        // Malformed catalysts — ignore
      }
    }

    return { refresh: false, reason: 'no refresh triggers met' };
  }

  toDbRow(report: OctagonReport, closeTimeEpoch?: number): DbOctagonReport {
    const ttl = closeTimeEpoch != null
      ? getTtlForCloseTime(Math.max(0, closeTimeEpoch - report.fetchedAt))
      : 86400;
    return {
      report_id: crypto.randomUUID(),
      ticker: report.ticker,
      event_ticker: report.eventTicker,
      model_prob: report.modelProb,
      market_prob: report.marketProb,
      mispricing_signal: report.mispricingSignal,
      drivers_json: JSON.stringify(report.drivers),
      catalysts_json: JSON.stringify(report.catalysts),
      sources_json: JSON.stringify(report.sources),
      resolution_history_json: report.resolutionHistory || null,
      contract_snapshot_json: report.contractSnapshot || null,
      raw_response: report.rawResponse || null,
      variant_used: report.variantUsed,
      fetched_at: report.fetchedAt,
      expires_at: report.fetchedAt + ttl,
    };
  }

  /**
   * Synchronously reserve credits for a refresh call. Returns the effective
   * variant: 'refresh' if budget allows, 'cache' if budget would be exceeded.
   * Must be called before the async invoke to prevent concurrent calls from
   * overshooting the daily credit ceiling.
   */
  reserveRefresh(requestedVariant: OctagonVariant): OctagonVariant {
    if (requestedVariant !== 'refresh') return requestedVariant === 'default' ? 'cache' : requestedVariant;
    if (this.creditsUsed + CREDITS_PER_FRESH_CALL > this.dailyCreditCeiling) {
      return 'cache';
    }
    this.creditsUsed += CREDITS_PER_FRESH_CALL;
    return 'refresh';
  }

  getCreditsUsed(): number {
    return this.creditsUsed;
  }

  resetCredits(): void {
    this.creditsUsed = 0;
  }

  // --- Private helpers ---

  private mapJsonToReport(parsed: Record<string, unknown>, defaults: OctagonReport): OctagonReport {
    // Handle nested cache response: { versions: [{ model_probability, market_probability, ... }] }
    const versions = parsed.versions as Array<Record<string, unknown>> | undefined;
    const source = versions?.[0] ?? parsed;

    // For multi-outcome events, look up this specific market's probability
    // from outcome_probabilities_json before falling back to event-level values.
    // The event-level model_probability is typically the first outcome's value,
    // not the one for the market we're analyzing.
    let modelProb: number | null = null;
    let marketProb: number | null = null;
    const outcomeJson = (source as Record<string, unknown>).outcome_probabilities_json;
    if (outcomeJson != null) {
      try {
        const outcomes = typeof outcomeJson === 'string'
          ? JSON.parse(outcomeJson) : outcomeJson;
        if (Array.isArray(outcomes)) {
          const match = outcomes.find(
            (o: { market_ticker?: string }) => String(o.market_ticker).toUpperCase() === defaults.ticker.toUpperCase()
          );
          if (match) {
            modelProb = this.toProbFromJson(match.model_probability);
            marketProb = this.toProbFromJson(match.market_probability);
          }
        }
      } catch { /* malformed outcome JSON — fall through */ }
    }

    // Fall back to event-level values (correct for single-outcome markets).
    // Uses toProbFromJson which always divides by 100, unlike toProb which uses a
    // > 1 heuristic that fails for sub-1% values (e.g. 0.9% stays as 0.9 → 90%).
    modelProb = modelProb ?? this.toProbFromJson(source.modelProb ?? source.model_prob ?? source.model_probability) ?? defaults.modelProb;
    marketProb = marketProb ?? this.toProbFromJson(source.marketProb ?? source.market_prob ?? source.market_probability) ?? defaults.marketProb;

    return {
      ...defaults,
      modelProb,
      marketProb,
      mispricingSignal: this.toSignal(source.mispricingSignal ?? source.mispricing_signal) ?? this.inferSignal(
        modelProb,
        marketProb
      ) ?? defaults.mispricingSignal,
      drivers: (() => {
        const latestReport = parsed.latest_report as Record<string, unknown> | undefined;
        const markdownReport = typeof latestReport?.markdown_report === 'string'
          ? latestReport.markdown_report
          : null;
        const shortAnswer = markdownReport ? this.extractShortAnswer(markdownReport) : null;
        return this.parseDrivers(source.drivers)
          ?? (shortAnswer ? [{ claim: shortAnswer, category: 'economic' as const, impact: 'high' as const }] : null)
          ?? this.driversFromTakeaway(source.key_takeaway)
          ?? defaults.drivers;
      })(),
      catalysts: this.parseCatalysts(source.catalysts) ?? defaults.catalysts,
      sources: this.parseSources(source.sources) ?? defaults.sources,
      resolutionHistory: String(source.resolutionHistory ?? source.resolution_history ?? defaults.resolutionHistory),
      contractSnapshot: String(source.contractSnapshot ?? source.contract_snapshot ?? source.outcome_probabilities_json ?? defaults.contractSnapshot),
    };
  }

  private extractFromMarkdown(raw: string, defaults: OctagonReport): OctagonReport {
    // For multi-outcome reports (World Cup Silver Ball, FOMC ladders, IPO
    // event trees), the per-outcome probabilities live in markdown tables
    // like:
    //   | Player       | Market % | Model % |
    //   |--------------|----------|---------|
    //   | Harry Kane   | 35.0%    | 36.0%   |
    //   | Lionel Messi | 29.0%    | 40.1%   |
    // The simple key:value regex below misses these and leaves the
    // 0.5/0.5 placeholder. Try table extraction first, matching against the
    // outcome suffix of the ticker (e.g. KXWCAWARD-26SBALL-HKANE → Kane).
    let modelProb = this.extractProbFromMarkdownTable(raw, defaults.ticker, 'model');
    let marketProb = this.extractProbFromMarkdownTable(raw, defaults.ticker, 'market');
    if (modelProb === null) {
      modelProb = this.extractProb(raw, /model\s*(?:prob(?:ability)?|estimate)\s*[:=]\s*([\d.]+%?)/i);
    }
    if (marketProb === null) {
      marketProb = this.extractProb(raw, /market\s*(?:prob(?:ability)?|price)\s*[:=]\s*([\d.]+%?)/i);
    }
    return {
      ...defaults,
      modelProb: modelProb ?? defaults.modelProb,
      marketProb: marketProb ?? defaults.marketProb,
      mispricingSignal: this.extractSignal(raw) ?? defaults.mispricingSignal,
      drivers: this.extractDrivers(raw),
      catalysts: this.extractCatalysts(raw),
      sources: this.extractSources(raw),
      resolutionHistory: this.extractSection(raw, /##?\s*resolution\s*history/i) ?? defaults.resolutionHistory,
      contractSnapshot: this.extractSection(raw, /##?\s*contract\s*snapshot/i) ?? defaults.contractSnapshot,
    };
  }

  /**
   * Parse markdown tables in `raw` looking for a Model/Market % grid, then
   * find the row matching `ticker`'s outcome suffix and return that row's
   * `kind` (model | market) probability as a 0-1 fraction. Returns null when
   * no table is found, no row matches, or the percentage can't be parsed.
   *
   * Matching strategy: the outcome suffix is whatever follows the last `-`
   * in the ticker (e.g. KXWCAWARD-26SBALL-HKANE → "HKANE"). We compare it
   * against each row's first cell (the name) by stripping non-letters and
   * checking if either string contains the other (case-insensitive). This
   * handles the common patterns: initial+lastname ("HKANE" ⊆ "harrykane"),
   * country abbreviations ("MEX" ⊆ "mexico"), full names, etc.
   *
   * If multiple rows match we pick the strongest (longest overlap) to keep
   * abbreviations like "MEX" from accidentally matching "MEXICO CITY".
   */
  private extractProbFromMarkdownTable(
    raw: string,
    ticker: string,
    kind: 'model' | 'market',
  ): number | null {
    const suffix = ticker.split('-').pop()?.toUpperCase() ?? '';
    if (!suffix || suffix.length < 2) return null;
    const tickerKey = suffix.replace(/[^A-Z0-9]/g, '');

    // Split into possible table blocks. Each block is a contiguous run of
    // lines that start with `|`. A real table has >=3 lines (header, sep,
    // body), at least 3 columns, and at least two columns whose values are
    // percentages.
    const lines = raw.split('\n');
    const blocks: string[][] = [];
    let cur: string[] = [];
    for (const line of lines) {
      if (line.trim().startsWith('|')) {
        cur.push(line);
      } else {
        if (cur.length > 0) blocks.push(cur);
        cur = [];
      }
    }
    if (cur.length > 0) blocks.push(cur);

    const parseCells = (line: string): string[] =>
      line.split('|').slice(1, -1).map((c) => c.trim());
    const isPercent = (s: string): boolean => /^-?\d+(\.\d+)?%?$/.test(s.replace(/[,$]/g, ''));
    const parsePercent = (s: string): number | null => {
      const cleaned = s.replace(/[%,$\s]/g, '');
      const n = parseFloat(cleaned);
      if (!Number.isFinite(n)) return null;
      return n > 1 ? n / 100 : n;
    };

    let bestMatch: { score: number; value: number } | null = null;
    for (const block of blocks) {
      if (block.length < 3) continue;
      const header = parseCells(block[0]).map((c) => c.toLowerCase());
      // Find target column. Accept "model", "model %", "model prob", etc.
      const targetCol = header.findIndex((c) =>
        kind === 'model'
          ? /\bmodel\b/.test(c)
          : /\b(market|mkt|kalshi)\b/.test(c) && !/cap/.test(c),  // avoid "market cap"
      );
      if (targetCol < 0) continue;

      // Body starts after the separator row (line index 2). Skip rows whose
      // cells don't look like data (e.g. another separator).
      for (let i = 2; i < block.length; i++) {
        const cells = parseCells(block[i]);
        if (cells.length <= targetCol) continue;
        const cell = cells[targetCol];
        if (!isPercent(cell)) continue;
        const name = (cells[0] ?? '').replace(/[^A-Za-z0-9]/g, '').toUpperCase();
        if (!name) continue;
        // Match strategies (try in order, take strongest):
        //   1. ticker fully inside row name ("MEX" in "MEXICO")
        //   2. row name fully inside ticker ("MESSI" in "LIONELMESSI")
        //   3. ticker suffix without leading initial(s) inside row name
        //      ("HKANE" → "KANE" in "HARRYKANE", "LMESSI" → "MESSI" in
        //      "LIONELMESSI"). Common Kalshi convention is one or two
        //      initials + lastname, so we strip up to 2 leading chars.
        let score = 0;
        if (name.includes(tickerKey)) score = tickerKey.length;
        else if (tickerKey.includes(name)) score = name.length;
        else {
          for (let drop = 1; drop <= Math.min(2, tickerKey.length - 3); drop++) {
            const trimmed = tickerKey.slice(drop);
            if (trimmed.length >= 3 && name.includes(trimmed)) {
              score = Math.max(score, trimmed.length);
              break;
            }
          }
        }
        if (score === 0) continue;
        const v = parsePercent(cell);
        if (v === null) continue;
        if (!bestMatch || score > bestMatch.score) {
          bestMatch = { score, value: v };
        }
      }
    }
    return bestMatch?.value ?? null;
  }

  private inferSignal(modelProb: number | null, marketProb: number | null): MispricingSignal | null {
    if (modelProb === null || marketProb === null) return null;
    const edge = modelProb - marketProb;
    if (Math.abs(edge) < 0.03) return 'fair_value';
    return edge > 0 ? 'underpriced' : 'overpriced';
  }

  private driversFromTakeaway(takeaway: unknown): PriceDriver[] | null {
    if (typeof takeaway !== 'string' || !takeaway.trim()) return null;
    return [{ claim: takeaway, category: 'economic', impact: 'medium' }];
  }

  private toProb(val: unknown): number | null {
    if (val === undefined || val === null) return null;
    if (typeof val === 'number') return val > 1 ? val / 100 : val;
    if (typeof val === 'string') {
      const cleaned = val.replace('%', '').trim();
      const num = parseFloat(cleaned);
      if (isNaN(num)) return null;
      return num > 1 ? num / 100 : num;
    }
    return null;
  }

  /** Parse probability from Octagon JSON API responses where values are always percentages (0-100). */
  private toProbFromJson(val: unknown): number | null {
    if (val === undefined || val === null) return null;
    if (typeof val === 'number') return val / 100;
    if (typeof val === 'string') {
      const num = parseFloat(val.replace('%', '').trim());
      if (isNaN(num)) return null;
      return num / 100;
    }
    return null;
  }

  private toSignal(val: unknown): MispricingSignal | null {
    if (typeof val !== 'string') return null;
    const normalized = val.toLowerCase().replace(/[\s-]/g, '_');
    if (normalized === 'overpriced') return 'overpriced';
    if (normalized === 'underpriced') return 'underpriced';
    if (normalized === 'fair_value' || normalized === 'fair') return 'fair_value';
    return null;
  }

  private parseDrivers(val: unknown): PriceDriver[] | null {
    if (!Array.isArray(val)) return null;
    return val
      .filter((d): d is Record<string, unknown> => typeof d === 'object' && d !== null)
      .map((d) => ({
        claim: String(d.claim ?? ''),
        category: this.toCategory(d.category) ?? 'economic',
        impact: this.toImpact(d.impact) ?? 'medium',
        sourceUrl: d.sourceUrl != null ? String(d.sourceUrl) : undefined,
      }));
  }

  private parseCatalysts(val: unknown): Catalyst[] | null {
    if (!Array.isArray(val)) return null;
    return val
      .filter((c): c is Record<string, unknown> => typeof c === 'object' && c !== null)
      .map((c) => ({
        date: String(c.date ?? ''),
        event: String(c.event ?? ''),
        impact: this.toImpact(c.impact) ?? 'medium',
        potentialMove: String(c.potentialMove ?? c.potential_move ?? ''),
      }));
  }

  private parseSources(val: unknown): Source[] | null {
    if (!Array.isArray(val)) return null;
    return val
      .filter((s): s is Record<string, unknown> => typeof s === 'object' && s !== null)
      .map((s) => ({
        url: String(s.url ?? ''),
        title: s.title != null ? String(s.title) : undefined,
      }));
  }

  private toCategory(val: unknown): DriverCategory | null {
    if (typeof val !== 'string') return null;
    const v = val.toLowerCase();
    if (v === 'political' || v === 'economic' || v === 'sentiment' || v === 'technical') return v;
    return null;
  }

  private toImpact(val: unknown): DriverImpact | null {
    if (typeof val !== 'string') return null;
    const v = val.toLowerCase();
    if (v === 'high' || v === 'medium' || v === 'low') return v;
    return null;
  }

  private extractProb(raw: string, pattern: RegExp): number | null {
    const match = raw.match(pattern);
    if (!match) return null;
    return this.toProb(match[1]);
  }

  private extractSignal(raw: string): MispricingSignal | null {
    const match = raw.match(/(?:mispricing|signal|assessment)\s*[:=]\s*(\w[\w\s]*)/i);
    if (!match) return null;
    return this.toSignal(match[1].trim());
  }

  private extractDrivers(raw: string): PriceDriver[] {
    const drivers: PriceDriver[] = [];
    // Match bullet points under a "drivers" section
    const section = this.extractSection(raw, /##?\s*(?:price\s*)?drivers/i);
    if (!section) return drivers;

    const bulletPattern = /[-*]\s*\*?\*?(.+?)(?:\n|$)/g;
    let m: RegExpExecArray | null;
    while ((m = bulletPattern.exec(section)) !== null) {
      drivers.push({
        claim: m[1].replace(/\*\*/g, '').trim(),
        category: 'economic',
        impact: 'medium',
      });
    }
    return drivers;
  }

  private extractCatalysts(raw: string): Catalyst[] {
    const catalysts: Catalyst[] = [];
    const section = this.extractSection(raw, /##?\s*catalysts/i);
    if (!section) return catalysts;

    const bulletPattern = /[-*]\s*\*?\*?(.+?)(?:\n|$)/g;
    let m: RegExpExecArray | null;
    while ((m = bulletPattern.exec(section)) !== null) {
      const text = m[1].replace(/\*\*/g, '').trim();
      const dateMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
      catalysts.push({
        date: dateMatch?.[1] ?? '',
        event: text,
        impact: 'medium',
        potentialMove: '',
      });
    }
    return catalysts;
  }

  private extractSources(raw: string): Source[] {
    const sources: Source[] = [];
    // Extract markdown links anywhere in the document
    const linkPattern = /\[([^\]]*)\]\((https?:\/\/[^)]+)\)/g;
    let m: RegExpExecArray | null;
    while ((m = linkPattern.exec(raw)) !== null) {
      sources.push({ url: m[2], title: m[1] || undefined });
    }
    // Also extract bare URLs
    const urlPattern = /(?<!\()(https?:\/\/[^\s)]+)/g;
    const existingUrls = new Set(sources.map((s) => s.url));
    while ((m = urlPattern.exec(raw)) !== null) {
      if (!existingUrls.has(m[1])) {
        sources.push({ url: m[1] });
        existingUrls.add(m[1]);
      }
    }
    return sources;
  }

  private extractShortAnswer(markdown: string): string | null {
    const section = this.extractSection(markdown, /##?\s*Short\s+Answer/i);
    if (!section) return null;
    const firstPara = section.split(/\n{2,}|\n(?=##)/)[0]?.trim();
    if (!firstPara) return null;
    return firstPara
      .replace(/\*\*(.+?)\*\*/g, '$1')
      .replace(/\*(.+?)\*/g, '$1')
      .replace(/\[\^[^\]]*\]/g, '')
      .replace(/^Key\s+takeaway[.:]\s*/i, '')
      .replace(/\s{2,}/g, ' ')
      .trim();
  }

  private extractSection(raw: string, headerPattern: RegExp): string | null {
    const lines = raw.split('\n');
    let capturing = false;
    const sectionLines: string[] = [];

    for (const line of lines) {
      if (headerPattern.test(line)) {
        capturing = true;
        continue;
      }
      if (capturing) {
        // Stop at next header
        if (/^##?\s/.test(line) && sectionLines.length > 0) break;
        sectionLines.push(line);
      }
    }

    return sectionLines.length > 0 ? sectionLines.join('\n').trim() : null;
  }
}
