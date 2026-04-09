import { StructuredToolInterface } from '@langchain/core/tools';
import { DynamicStructuredTool } from '@langchain/core/tools';
import { z } from 'zod';
import { createKalshiSearch, KALSHI_SEARCH_DESCRIPTION } from './kalshi/kalshi-search.js';
import { createKalshiTrade, KALSHI_TRADE_DESCRIPTION } from './kalshi/kalshi-trade.js';
import { getExchangeStatus } from './kalshi/exchange.js';
import { callKalshiApi } from './kalshi/api.js';
import { tavilySearch, WEB_SEARCH_DESCRIPTION } from './search/index.js';
import { webFetchTool, WEB_FETCH_DESCRIPTION } from './fetch/web-fetch.js';
import { formatToolResult } from './types.js';
import { edgeQueryTool, EDGE_QUERY_DESCRIPTION } from './v2/edge-query.js';
import { portfolioQueryTool, PORTFOLIO_QUERY_DESCRIPTION } from './v2/portfolio-query.js';
import { riskStatusTool, RISK_STATUS_DESCRIPTION } from './v2/risk-status.js';
import { octagonReportTool, OCTAGON_REPORT_DESCRIPTION } from './v2/octagon-report.js';
import { scanTool, SCAN_DESCRIPTION } from './v2/scan.js';
import { portfolioReviewTool, PORTFOLIO_REVIEW_DESCRIPTION } from './v2/portfolio-review.js';

/**
 * A registered tool with its rich description for system prompt injection.
 */
export interface RegisteredTool {
  /** Tool name (must match the tool's name property) */
  name: string;
  /** The actual tool instance */
  tool: StructuredToolInterface;
  /** Rich description for system prompt (includes when to use, when not to use, etc.) */
  description: string;
}

// Direct portfolio overview tool (balance + positions in one call)
const portfolioOverviewTool = new DynamicStructuredTool({
  name: 'portfolio_overview',
  description: 'Get a quick overview of the Kalshi portfolio: balance and open positions.',
  schema: z.object({}),
  func: async () => {
    const [balanceData, positionsData] = await Promise.all([
      callKalshiApi('GET', '/portfolio/balance'),
      callKalshiApi('GET', '/portfolio/positions'),
    ]);
    return formatToolResult({ balance: balanceData, positions: positionsData });
  },
});

const PORTFOLIO_OVERVIEW_DESCRIPTION = `
Quick portfolio overview tool. Returns current account balance and all open positions in a single call.

## When to Use
- User asks "what's my portfolio?" or "show me my balance and positions"
- Quick portfolio check before or after trading

## When NOT to Use
- Detailed fills or order history (use kalshi_search instead)
`.trim();

const EXCHANGE_STATUS_DESCRIPTION = `
Check whether the Kalshi exchange is currently active and trading is enabled.

## When to Use
- "Is Kalshi open?" or "Can I trade right now?"
`.trim();

/**
 * Get all registered tools with their descriptions.
 *
 * @param model - The model name (needed for sub-agent meta-tools)
 * @returns Array of registered tools
 */
export function getToolRegistry(model: string): RegisteredTool[] {
  const tools: RegisteredTool[] = [
    {
      name: 'kalshi_search',
      tool: createKalshiSearch(model),
      description: KALSHI_SEARCH_DESCRIPTION,
    },
    {
      name: 'kalshi_trade',
      tool: createKalshiTrade(model),
      description: KALSHI_TRADE_DESCRIPTION,
    },
    {
      name: 'portfolio_overview',
      tool: portfolioOverviewTool,
      description: PORTFOLIO_OVERVIEW_DESCRIPTION,
    },
    {
      name: 'exchange_status',
      tool: getExchangeStatus,
      description: EXCHANGE_STATUS_DESCRIPTION,
    },
    {
      name: 'web_fetch',
      tool: webFetchTool,
      description: WEB_FETCH_DESCRIPTION,
    },
    {
      name: 'edge_query',
      tool: edgeQueryTool,
      description: EDGE_QUERY_DESCRIPTION,
    },
    {
      name: 'portfolio_query',
      tool: portfolioQueryTool,
      description: PORTFOLIO_QUERY_DESCRIPTION,
    },
    {
      name: 'risk_status',
      tool: riskStatusTool,
      description: RISK_STATUS_DESCRIPTION,
    },
    {
      name: 'octagon_report',
      tool: octagonReportTool,
      description: OCTAGON_REPORT_DESCRIPTION,
    },
    {
      name: 'scan_markets',
      tool: scanTool,
      description: SCAN_DESCRIPTION,
    },
    {
      name: 'portfolio_review',
      tool: portfolioReviewTool,
      description: PORTFOLIO_REVIEW_DESCRIPTION,
    },
  ];

  // Include web_search if Tavily API key is configured
  if (process.env.TAVILY_API_KEY) {
    tools.push({
      name: 'web_search',
      tool: tavilySearch,
      description: WEB_SEARCH_DESCRIPTION,
    });
  }

  return tools;
}

/**
 * Get just the tool instances for binding to the LLM.
 *
 * @param model - The model name
 * @returns Array of tool instances
 */
export function getTools(model: string): StructuredToolInterface[] {
  return getToolRegistry(model).map((t) => t.tool);
}

/**
 * Build the tool descriptions section for the system prompt.
 * Formats each tool's rich description with a header.
 *
 * @param model - The model name
 * @returns Formatted string with all tool descriptions
 */
export function buildToolDescriptions(model: string): string {
  return getToolRegistry(model)
    .map((t) => `### ${t.name}\n\n${t.description}`)
    .join('\n\n');
}
