import { wrapSuccess, wrapError } from './json.js';
import type { CLIResponse } from './json.js';
import type { ParsedArgs } from './parse-args.js';
import {
  listClusters,
  listBehavioralClusters,
  getClusterMarkets,
  getBehavioralClusterMarkets,
  getClustersRankedByReturn,
  type ClusterRow,
  type PagedResult,
  type SimilarMarketRow,
  type RankedClustersResponse,
} from '../scan/octagon-kalshi-api.js';
import { formatTable } from './scan-formatters.js';

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function fmtPct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

export type ClustersResult =
  | { kind: 'list'; behavioral: boolean; data: ClusterRow[] }
  | { kind: 'members'; behavioral: boolean; cluster_id: number; markets: PagedResult<SimilarMarketRow> }
  | { kind: 'ranked'; data: RankedClustersResponse };

export async function handleClusters(args: ParsedArgs): Promise<CLIResponse<ClustersResult>> {
  try {
    if (args.ranked) {
      const data = await getClustersRankedByReturn({
        timeframe: args.timeframe,
        min_return: args.minReturn,
        top_n_per_cluster: args.topK,
        kind: args.behavioral ? 'behavioral' : 'thematic',
        max_clusters: args.limit,
      });
      return wrapSuccess('clusters', { kind: 'ranked', data });
    }

    const positional = args.positionalArgs[0];
    if (positional !== undefined && /^\d+$/.test(positional)) {
      const clusterId = Number(positional);
      const markets = args.behavioral
        ? await getBehavioralClusterMarkets(clusterId, { limit: args.limit })
        : await getClusterMarkets(clusterId, { limit: args.limit });
      return wrapSuccess('clusters', { kind: 'members', behavioral: args.behavioral, cluster_id: clusterId, markets });
    }

    const params = {
      limit: args.limit,
      sample_titles: 3,
      label_contains: args.labelContains,
    };
    const data = args.behavioral
      ? await listBehavioralClusters(params)
      : await listClusters(params);
    return wrapSuccess('clusters', { kind: 'list', behavioral: args.behavioral, data: data.data });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return wrapError('clusters', 'OCTAGON_ERROR', message);
  }
}

export function formatClustersHuman(result: ClustersResult): string {
  if (result.kind === 'list') return formatClusterList(result.data, result.behavioral);
  if (result.kind === 'members') return formatClusterMembers(result.cluster_id, result.markets, result.behavioral);
  return formatRankedClusters(result.data);
}

function formatClusterList(clusters: ClusterRow[], behavioral: boolean): string {
  const lines: string[] = [];
  const kindLabel = behavioral ? 'Behavioral' : 'Thematic';
  lines.push(`${kindLabel} clusters — ${clusters.length} result(s)`);
  lines.push('');

  if (clusters.length === 0) {
    lines.push('No clusters found.');
    return lines.join('\n');
  }

  const headers = behavioral
    ? ['ID', 'Label', 'Size', 'Mean Ret', 'Vol', 'Sample']
    : ['ID', 'Label', 'Size', 'Sample'];

  const rows: string[][] = clusters.map((c) => {
    const sample = truncate(c.sample_titles[0] ?? '-', 50);
    if (behavioral) {
      return [
        String(c.cluster_id),
        truncate(c.label, 30),
        String(c.size),
        c.mean_daily_return != null ? fmtPct(c.mean_daily_return) : '-',
        c.daily_volatility != null ? fmtPct(c.daily_volatility) : '-',
        sample,
      ];
    }
    return [String(c.cluster_id), truncate(c.label, 30), String(c.size), sample];
  });

  lines.push(formatTable(headers, rows));
  lines.push('');
  lines.push(`Use "clusters <id>${behavioral ? ' --behavioral' : ''}" to list members.`);
  return lines.join('\n');
}

function formatClusterMembers(clusterId: number, page: PagedResult<SimilarMarketRow>, behavioral: boolean): string {
  const lines: string[] = [];
  const kindLabel = behavioral ? 'behavioral' : 'thematic';
  lines.push(`Markets in ${kindLabel} cluster ${clusterId} — ${page.data.length} shown${page.has_more ? ' (more available)' : ''}`);
  lines.push('');

  if (page.data.length === 0) {
    lines.push('No markets in this cluster.');
    return lines.join('\n');
  }

  const rows: string[][] = page.data.map((m) => [
    m.market_ticker,
    truncate(m.title, 45),
    m.distance != null ? m.distance.toFixed(3) : '-',
    m.category ?? '-',
  ]);

  lines.push(formatTable(['Ticker', 'Title', 'Distance', 'Category'], rows));
  return lines.join('\n');
}

function formatRankedClusters(data: RankedClustersResponse): string {
  const lines: string[] = [];
  lines.push(`Clusters ranked by ${data.timeframe} return (${data.kind}, top ${data.top_n_per_cluster} per cluster, min return ${(data.min_return * 100).toFixed(1)}%)`);
  lines.push('');

  if (data.data.length === 0) {
    lines.push('No clusters meet the threshold.');
    return lines.join('\n');
  }

  const rows: string[][] = data.data.map((c) => [
    String(c.cluster_id),
    truncate(c.label, 30),
    String(c.size),
    fmtPct(c.summary.total_return),
    c.summary.sharpe != null ? c.summary.sharpe.toFixed(2) : '-',
    fmtPct(c.summary.max_drawdown),
    fmtPct(c.summary.win_rate),
  ]);

  lines.push(formatTable(['ID', 'Label', 'Size', 'Total Ret', 'Sharpe', 'Max DD', 'Win%'], rows));
  return lines.join('\n');
}
