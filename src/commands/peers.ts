import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  getClusterPeers,
  getMarketClusterMembership,
  type ClusterPeersResponse,
  type ClusterMembership,
} from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

export type PeersResult =
  | { kind: 'peers'; data: ClusterPeersResponse }
  | { kind: 'membership'; data: ClusterMembership };

export async function handlePeers(args: ParsedArgs): Promise<CLIResponse<PeersResult>> {
  const ticker = args.positionalArgs[0]?.toUpperCase() ?? args.ticker?.toUpperCase();
  if (!ticker) {
    return wrapError('peers', 'MISSING_TICKER', 'Usage: peers <ticker> [--behavioral] [--limit N] [--show-cluster]');
  }
  try {
    if (args.showCluster) {
      const data = await getMarketClusterMembership(ticker);
      return wrapSuccess('peers', { kind: 'membership', data });
    }
    const data = await getClusterPeers(ticker, {
      kind: args.behavioral ? 'behavioral' : 'thematic',
      limit: args.limit,
    });
    return wrapSuccess('peers', { kind: 'peers', data });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('peers', 'OCTAGON_ERROR', message);
  }
}

export function formatPeersHuman(result: PeersResult): string {
  if (result.kind === 'membership') return formatMembership(result.data);
  return formatPeers(result.data);
}

function formatPeers(data: ClusterPeersResponse): string {
  const lines: string[] = [];
  const c = data.cluster;
  lines.push(`Peers for ${data.market_ticker} (${data.kind} cluster ${c.cluster_id}: "${c.label}", size ${c.size})`);
  if (c.description) lines.push(`  ${c.description}`);
  lines.push('');

  if (data.data.length === 0) {
    lines.push('No peer markets in this cluster.');
    return lines.join('\n');
  }

  const rows: string[][] = data.data.map((m) => [
    m.market_ticker,
    truncate(m.title, 45),
    m.distance != null ? m.distance.toFixed(3) : '-',
    m.category ?? '-',
  ]);

  lines.push(formatTable(['Ticker', 'Title', 'Distance', 'Category'], rows));
  return lines.join('\n');
}

function formatMembership(data: ClusterMembership): string {
  const lines: string[] = [];
  lines.push(`Cluster membership for ${data.market_ticker}`);
  lines.push('');
  if (data.thematic) {
    lines.push(`  Thematic   cluster ${data.thematic.cluster_id}: "${data.thematic.label}" (size ${data.thematic.size})`);
    if (data.thematic.description) lines.push(`             ${data.thematic.description}`);
  } else {
    lines.push('  Thematic   (not assigned in current run)');
  }
  if (data.behavioral) {
    const meanRet = data.behavioral.mean_daily_return != null ? ` · mean ${(data.behavioral.mean_daily_return * 100).toFixed(2)}%/day` : '';
    const vol = data.behavioral.daily_volatility != null ? ` · vol ${(data.behavioral.daily_volatility * 100).toFixed(2)}%/day` : '';
    lines.push(`  Behavioral cluster ${data.behavioral.cluster_id}: "${data.behavioral.label}" (size ${data.behavioral.size})${meanRet}${vol}`);
  } else {
    lines.push('  Behavioral (not assigned in current run)');
  }
  return lines.join('\n');
}
