import type { Database } from 'bun:sqlite';

export interface EdgeMarket {
  market_ticker: string;
  event_ticker: string;
  model_prob: number;       // 0-100
  market_prob: number;      // 0-100
  edge_pp: number;          // model - market
  direction: 'YES' | 'NO';
  series_category: string;
  confidence_score: number;
}

export interface EdgeScanResult {
  markets: EdgeMarket[];
  total_scanned: number;
  events_scanned: number;
}

/**
 * Scan all cached Octagon events for markets with edge above a threshold.
 * Reads directly from SQLite — zero API calls, instant response.
 */
export function scanEdges(
  db: Database,
  opts?: { minEdgePp?: number; limit?: number; category?: string },
): EdgeScanResult {
  const minEdgePp = opts?.minEdgePp ?? 5;
  const limit = opts?.limit ?? 20;
  const category = opts?.category;

  let query = `SELECT event_ticker, series_category, confidence_score, outcome_probabilities_json
    FROM octagon_reports WHERE variant_used = 'events-api' AND outcome_probabilities_json IS NOT NULL`;
  const params: Record<string, string> = {};
  if (category) {
    query += ' AND LOWER(series_category) LIKE $cat';
    params.$cat = `%${category.toLowerCase()}%`;
  }

  const rows = db.query(query).all(params) as Array<{
    event_ticker: string;
    series_category: string | null;
    confidence_score: number | null;
    outcome_probabilities_json: string;
  }>;

  const allMarkets: EdgeMarket[] = [];
  let totalScanned = 0;

  for (const row of rows) {
    let outcomes: Array<{ market_ticker: string; model_probability: number; market_probability: number }>;
    try {
      outcomes = JSON.parse(row.outcome_probabilities_json);
    } catch {
      continue;
    }
    if (!Array.isArray(outcomes)) continue;

    for (const o of outcomes) {
      if (typeof o.model_probability !== 'number' || typeof o.market_probability !== 'number') continue;
      if (!o.market_ticker) continue;
      totalScanned++;
      const edgePp = Math.round((o.model_probability - o.market_probability) * 10) / 10;
      if (Math.abs(edgePp) < minEdgePp) continue;

      allMarkets.push({
        market_ticker: o.market_ticker,
        event_ticker: row.event_ticker,
        model_prob: o.model_probability,
        market_prob: o.market_probability,
        edge_pp: edgePp,
        direction: edgePp > 0 ? 'YES' : 'NO',
        series_category: row.series_category ?? '',
        confidence_score: row.confidence_score ?? 0,
      });
    }
  }

  // Sort by |edge| descending
  allMarkets.sort((a, b) => Math.abs(b.edge_pp) - Math.abs(a.edge_pp));

  return {
    markets: allMarkets.slice(0, limit),
    total_scanned: totalScanned,
    events_scanned: rows.length,
  };
}

export function formatEdgeScanHuman(result: EdgeScanResult, minEdgePp: number): string {
  const lines: string[] = [];
  lines.push(`Octagon Edge Scanner — ${result.events_scanned} events, ${result.total_scanned} markets scanned`);
  lines.push('════════════════════════════════════════════════════════');
  lines.push('');

  if (result.markets.length === 0) {
    lines.push(`  No markets with |edge| ≥ ${minEdgePp}pp found.`);
    return lines.join('\n');
  }

  const header = '  ' + [
    '#'.padStart(3),
    'Ticker'.padEnd(35),
    'Model'.padStart(6),
    'Market'.padStart(7),
    'Edge'.padStart(7),
    'Dir'.padStart(5),
    'Category'.padEnd(15),
  ].join('  ');
  lines.push(header);

  for (let i = 0; i < result.markets.length; i++) {
    const m = result.markets[i];
    const row = '  ' + [
      String(i + 1).padStart(3),
      m.market_ticker.padEnd(35),
      `${m.model_prob.toFixed(0)}%`.padStart(6),
      `${m.market_prob.toFixed(0)}%`.padStart(7),
      `${m.edge_pp >= 0 ? '+' : ''}${m.edge_pp.toFixed(0)}pp`.padStart(7),
      m.direction.padStart(5),
      m.series_category.padEnd(15),
    ].join('  ');
    lines.push(row);
  }

  lines.push('');
  lines.push(`${result.markets.length} markets with |edge| ≥ ${minEdgePp}pp`);

  return lines.join('\n');
}
