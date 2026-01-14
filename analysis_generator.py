"""
AI Analysis Generator for Kalshi Events.

Uses Gemini 2.5 Flash (with Google Search grounding), Octagon Deep Research API,
and Exa Crawl API to generate comprehensive analysis for prediction market events.
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
from pydantic import BaseModel, Field

from config import GeminiConfig, ExaConfig, OctagonConfig
from research_client import OctagonClient


def transform_candlesticks_for_lightweight_charts(
    kalshi_candlesticks: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Transform Kalshi candlestick data to TradingView Lightweight Charts format.
    
    Lightweight Charts expects:
        { "time": 1704153600, "open": 0.50, "high": 0.51, "low": 0.49, "close": 0.51 }
    
    Args:
        kalshi_candlesticks: Raw candlestick data from Kalshi API
        
    Returns:
        List of candlesticks formatted for Lightweight Charts
    """
    if not kalshi_candlesticks:
        return []
    
    formatted = []
    for candle in kalshi_candlesticks:
        ts = candle.get("end_period_ts", 0)
        if not ts:
            continue
        
        price = candle.get("price")
        volume = candle.get("volume", 0)
        
        # Handle different price formats from Kalshi API
        if isinstance(price, dict):
            # OHLC format: {"open": 5025, "high": 5125, "low": 4950, "close": 5075}
            open_price = price.get("open", 0)
            high_price = price.get("high", 0)
            low_price = price.get("low", 0)
            close_price = price.get("close", 0)
            
            # Convert from cents to probability (0-1)
            divisor = 100 if max(open_price, high_price, low_price, close_price) <= 100 else 10000
            
            formatted.append({
                "time": ts,
                "open": round(open_price / divisor, 4),
                "high": round(high_price / divisor, 4),
                "low": round(low_price / divisor, 4),
                "close": round(close_price / divisor, 4),
                "volume": volume
            })
        elif isinstance(price, (int, float)):
            # Single price value - use for all OHLC
            prob = price / 100 if price > 1 else price
            formatted.append({
                "time": ts,
                "open": round(prob, 4),
                "high": round(prob, 4),
                "low": round(prob, 4),
                "close": round(prob, 4),
                "volume": volume
            })
        else:
            # Try to extract from yes_bid if price not available
            yes_bid = candle.get("yes_bid", {})
            if isinstance(yes_bid, dict):
                close_price = yes_bid.get("close", 0)
                divisor = 100 if close_price <= 100 else 10000
                prob = close_price / divisor
                formatted.append({
                    "time": ts,
                    "open": round(prob, 4),
                    "high": round(prob, 4),
                    "low": round(prob, 4),
                    "close": round(prob, 4),
                    "volume": volume
                })
    
    # Sort by time ascending (required by Lightweight Charts)
    formatted.sort(key=lambda x: x["time"])
    
    return formatted


# ============================================================================
# Pydantic Models for Structured Outputs
# ============================================================================

class ResearchQuestions(BaseModel):
    """Five research questions for a prediction market."""
    q1: str = Field(description="Direct question about the outcome")
    q2: str = Field(description="Question about recent news or developments")
    q3: str = Field(description="Question about expert predictions or forecasts")
    q4: str = Field(description="Question about key data, statistics, or indicators")
    q5: str = Field(description="Question about timeline or upcoming events")


class TableDataItem(BaseModel):
    """A single data point for a table."""
    label: str = Field(description="Short name of the metric (plain text, no markdown or formatting)")
    value: str = Field(description="Data value with source in parentheses (plain text, no markdown)")


class QuestionResearch(BaseModel):
    """Research findings for a single question."""
    subtitle: str = Field(description="A concise 5-10 word title for this research section")
    table_data: List[TableDataItem] = Field(description="3 key data points with sources")
    paragraphs: List[str] = Field(description="2-3 paragraphs with inline citations like [1], [2]")


class CatalystResearch(BaseModel):
    """Key catalysts that could change market probability."""
    subtitle: str = Field(description="Section title, e.g., 'Key Catalysts'")
    paragraphs: List[str] = Field(description="2-3 paragraphs with inline citations like [1], [2]")


class ChartAnomaly(BaseModel):
    """A significant price movement detected in the chart."""
    date: str = Field(description="Date of the anomaly (ISO format)")
    date_readable: str = Field(description="Human-readable date (e.g., 'January 5, 2026')")
    price_before: float = Field(description="Price before the movement (0-1 probability)")
    price_after: float = Field(description="Price after the movement (0-1 probability)")
    change_pct: float = Field(description="Percentage point change")
    direction: str = Field(description="'spike' or 'drop'")
    description: str = Field(description="Brief description of the movement")


def clean_markdown_response(text: str) -> str:
    """Clean markdown artifacts from LLM responses.
    
    Removes code blocks, excessive whitespace, and converts markdown to HTML.
    
    Args:
        text: Raw text from LLM
        
    Returns:
        Cleaned text suitable for richtext fields
    """
    if not text:
        return ""
    
    # Remove code block markers (```json, ```, etc.)
    text = re.sub(r'```\w*\n?', '', text)
    text = re.sub(r'```', '', text)
    
    # Convert markdown bold **text** to HTML <strong>
    text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)
    
    # Convert markdown italic *text* to HTML <em>
    text = re.sub(r'\*([^*]+)\*', r'<em>\1</em>', text)
    
    # Convert markdown headers to HTML
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)
    
    # Convert markdown links [text](url) to HTML
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    
    # Convert line breaks to paragraphs (double newline = new paragraph)
    paragraphs = text.strip().split('\n\n')
    if len(paragraphs) > 1:
        text = ''.join(f'<p>{p.strip()}</p>' for p in paragraphs if p.strip())
    
    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()
    
    return text


def strip_markdown(text: str) -> str:
    """Strip markdown formatting from text, keeping plain text."""
    if not text:
        return ""
    # Remove bold markers
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'__(.+?)__', r'\1', text)
    # Remove italic markers
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'_(.+?)_', r'\1', text)
    # Remove code markers
    text = re.sub(r'`(.+?)`', r'\1', text)
    return text.strip()


def format_text_with_citations(text: str, citations: List[Dict[str, Any]]) -> str:
    """Convert [1], [2] citation markers to clickable HTML links and append sources section.
    
    Args:
        text: Text containing [1], [2], etc. citation markers
        citations: List of dicts with 'order', 'name', 'url' keys from Octagon
        
    Returns:
        HTML formatted text with clickable citation links and sources section
    """
    if not text:
        return ""
    
    # Build citation lookup by order number
    citation_map = {c["order"]: c for c in citations} if citations else {}
    
    # Replace [N] with clickable links
    def replace_citation(match):
        try:
            num = int(match.group(1))
            if num in citation_map:
                url = citation_map[num]["url"]
                return f'<a href="{url}" target="_blank" class="citation-link" data-citation="{num}">[{num}]</a>'
        except (ValueError, KeyError):
            pass
        return match.group(0)  # Return unchanged if not found
    
    formatted = re.sub(r'\[(\d+)\]', replace_citation, text)
    
    # Clean markdown and convert to HTML
    formatted = clean_markdown_response(formatted)
    
    # Append sources section wrapped in semantic div
    if citations:
        formatted += '\n\n<div class="sources-container">'
        formatted += '<h4 class="sources-title">Sources</h4>'
        formatted += '<ol class="sources-list">'
        for c in sorted(citations, key=lambda x: x["order"]):
            name = c.get("name", f"Source {c['order']}")
            url = c.get("url", "")
            formatted += f'<li class="source-item"><a href="{url}" target="_blank" class="source-link">{name}</a></li>'
        formatted += '</ol></div>'
    
    return formatted


def format_table_html(table_data: List[Dict[str, str]]) -> str:
    """Format table data as an HTML table for Webflow richtext.

    Webflow richtext supports: <table>, <thead>, <tbody>, <tr>, <th>, <td>
    
    Args:
        table_data: List of {label, value} dicts

    Returns:
        HTML table string compatible with Webflow richtext
    """
    if not table_data:
        return ""

    rows = []
    for item in table_data:
        # Strip any markdown formatting from the data
        label = strip_markdown(item.get('label', ''))
        value = strip_markdown(item.get('value', ''))
        if label or value:
            # Use <th> for label column (semantically correct, renders bold)
            rows.append(f'<tr><th>{label}</th><td>{value}</td></tr>')

    if not rows:
        return ""

    # Use proper table structure with tbody
    return f'<table><tbody>{"".join(rows)}</tbody></table>'


def format_orderbook_summary(orderbook: Dict[str, Any]) -> Tuple[str, str]:
    """Format orderbook data into richtext summary and JSON.
    
    Args:
        orderbook: Raw orderbook data from Kalshi API
        
    Returns:
        Tuple of (richtext_summary, json_data)
    """
    if not orderbook:
        return "<p>No orderbook data available.</p>", "[]"
    
    # Extract yes side orderbook
    yes_data = orderbook.get("yes", {}) or orderbook
    bids = yes_data.get("bids", []) or []
    asks = yes_data.get("asks", []) or []
    
    # Calculate spread
    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None
    
    if best_bid and best_ask:
        bid_price = best_bid.get("price", 0) / 100 if best_bid.get("price", 0) > 1 else best_bid.get("price", 0)
        ask_price = best_ask.get("price", 0) / 100 if best_ask.get("price", 0) > 1 else best_ask.get("price", 0)
        spread = abs(ask_price - bid_price) * 100  # In cents
        
        bid_size = best_bid.get("quantity", 0) or best_bid.get("count", 0)
        ask_size = best_ask.get("quantity", 0) or best_ask.get("count", 0)
        
        summary = f"<p><strong>Spread:</strong> {spread:.0f}c</p>"
        summary += f"<p><strong>Best Bid:</strong> {bid_price*100:.0f}c ({bid_size:,} contracts)</p>"
        summary += f"<p><strong>Best Ask:</strong> {ask_price*100:.0f}c ({ask_size:,} contracts)</p>"
        
        # Calculate total depth
        total_bid_depth = sum(b.get("quantity", 0) or b.get("count", 0) for b in bids[:5])
        total_ask_depth = sum(a.get("quantity", 0) or a.get("count", 0) for a in asks[:5])
        summary += f"<p><strong>Depth (5 levels):</strong> {total_bid_depth:,} bid / {total_ask_depth:,} ask</p>"
    else:
        summary = "<p>Limited orderbook data available.</p>"
    
    # Build JSON for frontend
    json_data = {
        "spread": spread if best_bid and best_ask else None,
        "best_bid": {"price": bid_price * 100, "size": bid_size} if best_bid else None,
        "best_ask": {"price": ask_price * 100, "size": ask_size} if best_ask else None,
        "bids": [{"price": (b.get("price", 0) / 100 if b.get("price", 0) > 1 else b.get("price", 0)) * 100, 
                  "size": b.get("quantity", 0) or b.get("count", 0)} for b in bids[:10]],
        "asks": [{"price": (a.get("price", 0) / 100 if a.get("price", 0) > 1 else a.get("price", 0)) * 100,
                  "size": a.get("quantity", 0) or a.get("count", 0)} for a in asks[:10]]
    }
    
    return summary, json.dumps(json_data)


def format_trades_summary(trades: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Format recent trades into richtext summary and JSON.
    
    Args:
        trades: List of trade objects from Kalshi API
        
    Returns:
        Tuple of (richtext_summary, json_data)
    """
    if not trades:
        return "<p>No recent trade data available.</p>", "[]"
    
    # Calculate summary stats
    total_trades = len(trades)
    
    # Get trade sizes (in cents, convert to dollars)
    sizes = []
    for t in trades:
        # Try different field names
        size = t.get("count", 0) or t.get("size", 0) or t.get("quantity", 0)
        price = t.get("yes_price", 0) or t.get("price", 0)
        if size and price:
            # Size in contracts * price per contract
            dollar_value = size * (price / 100 if price > 1 else price)
            sizes.append(dollar_value)
    
    if sizes:
        avg_size = sum(sizes) / len(sizes)
        max_size = max(sizes)
        total_volume = sum(sizes)
        
        summary = f"<p><strong>Recent Trades:</strong> {total_trades} trades</p>"
        summary += f"<p><strong>Total Volume:</strong> ${total_volume:,.0f}</p>"
        summary += f"<p><strong>Average Trade:</strong> ${avg_size:.0f}</p>"
        summary += f"<p><strong>Largest Trade:</strong> ${max_size:.0f}</p>"
    else:
        summary = f"<p><strong>Recent Trades:</strong> {total_trades} trades</p>"
    
    # Build JSON for frontend (limit to recent 20)
    json_data = []
    for t in trades[:20]:
        json_data.append({
            "time": t.get("created_time", "") or t.get("ts", ""),
            "price": t.get("yes_price", 0) or t.get("price", 0),
            "size": t.get("count", 0) or t.get("size", 0) or t.get("quantity", 0)
        })
    
    return summary, json.dumps(json_data)


def format_key_dates(event: Dict[str, Any], markets: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Extract and format key dates from event and market data.
    
    Args:
        event: Event data with open_time, close_time, etc.
        markets: List of market data
        
    Returns:
        Tuple of (richtext_summary, json_data)
    """
    key_dates = []
    
    # Extract from event
    open_time = event.get("open_time", "")
    close_time = event.get("close_time", "")
    strike_date = event.get("strike_date", "")
    
    def format_date(date_str: str, label: str) -> Optional[Dict[str, Any]]:
        if not date_str:
            return None
        try:
            if isinstance(date_str, str):
                # Handle ISO format
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return {
                    "date": dt.strftime("%Y-%m-%d"),
                    "date_readable": dt.strftime("%B %d, %Y"),
                    "event": label,
                    "description": f"Market {label.lower()}"
                }
        except:
            pass
        return None
    
    # Add event dates
    if open_time:
        d = format_date(open_time, "Opened")
        if d:
            key_dates.append(d)
    
    if close_time:
        d = format_date(close_time, "Closes")
        if d:
            key_dates.append(d)
    
    if strike_date:
        d = format_date(strike_date, "Strike Date")
        if d:
            key_dates.append(d)
    
    # Extract from markets (expiration times)
    for m in markets[:1]:  # Just first market
        exp_time = m.get("latest_expiration_time", "") or m.get("expiration_time", "")
        if exp_time:
            d = format_date(exp_time, "Expiration")
            if d and d not in key_dates:
                key_dates.append(d)
    
    # Sort by date
    key_dates.sort(key=lambda x: x["date"])
    
    # Build richtext
    if key_dates:
        richtext = "<ul class='key-dates-list'>"
        for d in key_dates:
            richtext += f"<li><strong>{d['event']}:</strong> {d['date_readable']}</li>"
        richtext += "</ul>"
    else:
        richtext = "<p>No key dates available.</p>"
    
    return richtext, json.dumps(key_dates)


def format_historical_track_record(settled_markets: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Format settled markets into historical track record summary.
    
    Args:
        settled_markets: List of settled market objects
        
    Returns:
        Tuple of (richtext_summary, json_data)
    """
    if not settled_markets:
        return "<p>No historical resolution data available for this series.</p>", "[]"
    
    total = len(settled_markets)
    yes_count = sum(1 for m in settled_markets if m.get("result") == "yes" or m.get("result") == "Yes")
    no_count = sum(1 for m in settled_markets if m.get("result") == "no" or m.get("result") == "No")
    
    summary = f"<p><strong>Historical Resolutions:</strong> {total} markets in this series</p>"
    summary += f"<p><strong>Outcomes:</strong> {yes_count} resolved YES, {no_count} resolved NO</p>"
    
    # List recent resolutions
    recent = settled_markets[:5]
    if recent:
        summary += "<p><strong>Recent resolutions:</strong></p><ul>"
        for m in recent:
            ticker = m.get("ticker", "Unknown")
            result = m.get("result", "Unknown")
            settled_time = m.get("settlement_time", "") or m.get("close_time", "")
            summary += f"<li>{ticker}: {result.upper()}"
            if settled_time:
                # Try to format date
                try:
                    if isinstance(settled_time, str):
                        dt = datetime.fromisoformat(settled_time.replace("Z", "+00:00"))
                        summary += f" ({dt.strftime('%b %d, %Y')})"
                except:
                    pass
            summary += "</li>"
        summary += "</ul>"
    
    # Build JSON
    json_data = []
    for m in settled_markets:
        json_data.append({
            "ticker": m.get("ticker", ""),
            "result": m.get("result", ""),
            "settled_time": m.get("settlement_time", "") or m.get("close_time", ""),
            "final_price": m.get("last_price", 0)
        })
    
    return summary, json.dumps(json_data)


def parse_json_safely(text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON from text, handling common issues with LLM output.
    
    Args:
        text: Text that may contain JSON
        
    Returns:
        Parsed dict or None if parsing fails
    """
    if not text:
        return None
    
    # First, strip markdown code blocks (```json ... ``` or ``` ... ```)
    # This is very common in LLM outputs
    text = re.sub(r'^```(?:json)?\s*\n?', '', text.strip())
    text = re.sub(r'\n?```\s*$', '', text.strip())
    text = text.strip()
    
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Try to find JSON object in the text
    # Look for outermost { } pair
    start = text.find('{')
    if start == -1:
        return None
    
    # Find matching closing brace by counting braces
    depth = 0
    end = -1
    for i, char in enumerate(text[start:], start):
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    
    if end == -1:
        return None
    
    json_str = text[start:end]
    
    # Try parsing the extracted JSON
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    
    # Try fixing common issues
    # Remove trailing commas before } or ]
    fixed = re.sub(r',\s*([\}\]])', r'\1', json_str)
    
    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass
    
    # Try replacing single quotes with double quotes (careful with apostrophes)
    # Only do this if there are no double quotes in values
    if '"' not in json_str:
        fixed = json_str.replace("'", '"')
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
    
    return None


class AnalysisGenerator:
    """Generates AI-powered analysis for Kalshi events."""
    
    def __init__(
        self,
        gemini_config: GeminiConfig,
        octagon_config: OctagonConfig,
        exa_config: ExaConfig
    ):
        self.gemini_config = gemini_config
        self.octagon_config = octagon_config
        self.exa_config = exa_config
        
        # Initialize clients lazily
        self._gemini_client = None
        self._octagon_client = None
        self._exa_client = None
    
    def _init_gemini(self):
        """Initialize Gemini client."""
        if self._gemini_client is None and self.gemini_config.is_configured:
            try:
                from google import genai
                self._gemini_client = genai.Client(api_key=self.gemini_config.api_key)
                logger.info("Gemini client initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini client: {e}")
        return self._gemini_client
    
    def _init_octagon(self):
        """Initialize Octagon client."""
        if self._octagon_client is None:
            self._octagon_client = OctagonClient(self.octagon_config)
            logger.info("Octagon client initialized")
        return self._octagon_client
    
    def _init_exa(self):
        """Initialize Exa client."""
        if self._exa_client is None and self.exa_config.is_configured:
            try:
                from exa_py import Exa
                self._exa_client = Exa(api_key=self.exa_config.api_key)
                logger.info("Exa client initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Exa client: {e}")
        return self._exa_client
    
    async def crawl_kalshi_page(
        self,
        series_ticker: str,
        series_title: str,
        event_ticker: str
    ) -> Dict[str, str]:
        """Crawl Kalshi event page using Exa to get contract rules and discussion.
        
        Args:
            series_ticker: The series ticker (e.g., "KXRAMPBREX")
            series_title: The series title (e.g., "Ramp v Brex")
            event_ticker: The event ticker (e.g., "KXRAMPBREX-40")
            
        Returns:
            Dict with 'content' (page text) and 'url' keys
        """
        exa = self._init_exa()
        if not exa:
            logger.warning("Exa not configured, skipping page crawl")
            return {"content": "", "url": ""}
        
        # Build URL: https://kalshi.com/markets/{series_ticker}/{series_title_slug}/{event_ticker}
        # All parts should be lowercase
        series_slug = series_ticker.lower()
        title_slug = re.sub(r'[^a-z0-9]+', '-', series_title.lower()).strip('-')
        event_slug = event_ticker.lower()
        
        url = f"https://kalshi.com/markets/{series_slug}/{title_slug}/{event_slug}"
        
        try:
            logger.info(f"Crawling Kalshi page: {url}")
            
            response = exa.get_contents(
                [url],
                text={
                    "max_characters": 15000,
                    "include_html_tags": False
                }
            )
            
            if response.results and len(response.results) > 0:
                content = response.results[0].text or ""
                logger.info(f"Crawled {len(content)} characters from {url}")
                return {"content": content, "url": url}
            else:
                logger.warning(f"No content returned from Exa for {url}")
                return {"content": "", "url": url}
                
        except Exception as e:
            logger.error(f"Error crawling Kalshi page {url}: {e}")
            return {"content": "", "url": url}
    
    async def _gemini_generate(
        self,
        prompt: str,
        use_search_grounding: bool = False
    ) -> str:
        """Generate content using Gemini, optionally with Google Search grounding.
        
        Args:
            prompt: The prompt to send to Gemini
            use_search_grounding: Whether to use Google Search for grounding
            
        Returns:
            Generated text response
        """
        client = self._init_gemini()
        if not client:
            logger.warning("Gemini not configured, skipping generation")
            return ""
        
        try:
            from google.genai import types
            
            config_kwargs = {}
            if use_search_grounding:
                config_kwargs["tools"] = [
                    types.Tool(google_search=types.GoogleSearch())
                ]
            
            response = client.models.generate_content(
                model=self.gemini_config.model,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs) if config_kwargs else None
            )
            
            return response.text or ""
            
        except Exception as e:
            logger.error(f"Error generating content with Gemini: {e}")
            return ""
    
    async def _resolve_redirect_url(self, redirect_url: str) -> str:
        """Resolve a Vertex AI Search redirect URL to its actual destination.
        
        Args:
            redirect_url: The vertexaisearch.cloud.google.com redirect URL
            
        Returns:
            The actual destination URL, or the original if resolution fails
        """
        if not redirect_url or "vertexaisearch.cloud.google.com" not in redirect_url:
            return redirect_url
        
        try:
            import httpx
            async with httpx.AsyncClient(follow_redirects=True, timeout=5.0) as client:
                response = await client.head(redirect_url)
                return str(response.url)
        except Exception:
            return redirect_url  # Fallback to original if resolving fails
    
    async def _extract_grounding_sources(self, response) -> List[Dict[str, str]]:
        """Extract grounding sources from Gemini response metadata.
        
        Resolves Vertex AI Search redirect URLs to actual source URLs.
        
        Args:
            response: Gemini API response object
            
        Returns:
            List of dicts with 'title' and 'uri' keys
        """
        sources = []
        try:
            # Access grounding metadata from the response
            # The structure is: response.candidates[0].grounding_metadata.grounding_chunks
            if hasattr(response, 'candidates') and response.candidates:
                candidate = response.candidates[0]
                
                if hasattr(candidate, 'grounding_metadata') and candidate.grounding_metadata:
                    metadata = candidate.grounding_metadata
                    
                    if hasattr(metadata, 'grounding_chunks') and metadata.grounding_chunks:
                        # Collect redirect URLs to resolve in parallel
                        redirect_tasks = []
                        chunk_data = []
                        
                        for chunk in metadata.grounding_chunks:
                            if hasattr(chunk, 'web') and chunk.web:
                                title = chunk.web.title or ""
                                uri = chunk.web.uri or ""
                                chunk_data.append({"title": title})
                                redirect_tasks.append(self._resolve_redirect_url(uri))
                        
                        # Resolve all redirect URLs in parallel
                        if redirect_tasks:
                            resolved_urls = await asyncio.gather(*redirect_tasks, return_exceptions=True)
                            for i, url in enumerate(resolved_urls):
                                if isinstance(url, Exception):
                                    url = ""
                                chunk_data[i]["uri"] = url
                            sources = chunk_data
                
        except Exception as e:
            logger.warning(f"Failed to extract grounding sources: {e}")
        
        return sources
    
    async def _gemini_generate_structured(
        self,
        prompt: str,
        response_schema: type[BaseModel],
        use_search_grounding: bool = False
    ) -> tuple[Optional[BaseModel], List[Dict[str, str]]]:
        """Generate structured content using Gemini with JSON schema.
        
        Note: Gemini 2.5 does NOT support combining tools (grounding) with 
        structured JSON output. When grounding is needed, we use a two-step
        approach: first get grounded info, then format with structured output.
        
        Args:
            prompt: The prompt to send to Gemini
            response_schema: Pydantic model class defining the expected output
            use_search_grounding: Whether to use Google Search for grounding
            
        Returns:
            Tuple of (Parsed Pydantic model instance or None, list of grounding sources)
        """
        client = self._init_gemini()
        if not client:
            logger.warning("Gemini not configured, skipping generation")
            return None, []
        
        try:
            from google.genai import types
            
            if use_search_grounding:
                # Two-step approach for grounded + structured output
                # Step 1: Get grounded information
                grounded_response = client.models.generate_content(
                    model=self.gemini_config.model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[types.Tool(google_search=types.GoogleSearch())]
                    )
                )
                grounded_text = grounded_response.text or ""
                
                # Extract real grounding sources from the response (resolves redirect URLs)
                grounding_sources = await self._extract_grounding_sources(grounded_response)
                logger.info(f"Extracted {len(grounding_sources)} grounding sources from search")
                
                if not grounded_text:
                    return None, grounding_sources
                
                # Build source reference for the formatting step
                source_list = "\n".join([
                    f"[{i+1}] {s['title']} - {s['uri']}"
                    for i, s in enumerate(grounding_sources)
                ]) if grounding_sources else "No sources available"
                
                # Step 2: Format into structured output
                schema_json = response_schema.model_json_schema()
                format_prompt = f"""Based on this research information, extract and format the data.

Research findings:
{grounded_text}

Available sources (use these citation numbers in your paragraphs):
{source_list}

Format the above information according to this JSON schema:
{json.dumps(schema_json, indent=2)}

IMPORTANT: When citing sources in paragraphs, use the exact citation numbers [1], [2], etc. from the source list above. Only cite sources that are actually listed.

Return ONLY valid JSON matching the schema."""

                structured_response = client.models.generate_content(
                    model=self.gemini_config.model,
                    contents=format_prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_json_schema=schema_json,
                    )
                )
                
                return response_schema.model_validate_json(structured_response.text), grounding_sources
            else:
                # Direct structured output without grounding
                response = client.models.generate_content(
                    model=self.gemini_config.model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_json_schema=response_schema.model_json_schema(),
                    )
                )
                
                return response_schema.model_validate_json(response.text), []
            
        except Exception as e:
            logger.error(f"Error generating structured content with Gemini: {e}")
            return None, []
    
    async def summarize_contract_rules(
        self,
        exa_content: str,
        event_url: str,
        event_title: str
    ) -> str:
        """Summarize contract rules from crawled Kalshi page content.
        
        Args:
            exa_content: Raw text content from Exa crawl
            event_url: URL of the Kalshi event page
            event_title: Title of the event
            
        Returns:
            2-3 sentence summary of contract resolution rules
        """
        if not exa_content:
            return "Contract details not available."
        
        prompt = f"""You are summarizing the contract rules for a Kalshi prediction market.

## Page Content from {event_url}:
{exa_content[:8000]}

Extract and summarize in 2-3 sentences:
1. What exactly triggers a YES resolution
2. What triggers a NO resolution
3. Key dates/deadlines
4. Any special settlement conditions

Keep it factual and concise. Do not include speculation."""

        return await self._gemini_generate(prompt, use_search_grounding=False)
    
    async def summarize_market_discussion(
        self,
        exa_content: str,
        event_url: str,
        event_title: str
    ) -> str:
        """Summarize market discussion from crawled Kalshi page or web search.
        
        If Exa content doesn't have meaningful discussion, falls back to
        Google Search to find discussions from the wider internet.
        
        Args:
            exa_content: Raw text content from Exa crawl
            event_url: URL of the Kalshi event page
            event_title: Title of the event
            
        Returns:
            2-3 sentence summary of market discussion
        """
        # First try to extract discussion from Exa content
        if exa_content and len(exa_content) > 500:
            prompt = f"""You are summarizing the market discussion for a Kalshi prediction market: "{event_title}"

## Discussion Content from {event_url}:
{exa_content[:8000]}

Summarize in 2-3 sentences:
1. Main viewpoints being discussed by traders
2. Key arguments for YES vs NO
3. Any notable insights or consensus

If no meaningful discussion exists in the content above, respond with exactly: "NO_DISCUSSION" """

            result = await self._gemini_generate(prompt, use_search_grounding=False)
            
            # If we found discussion, return it
            if result and "NO_DISCUSSION" not in result:
                return result
        
        # Fallback: Use Octagon to find discussions about this topic
        logger.info(f"No Kalshi discussion found, searching for: {event_title}")
        
        octagon = self._init_octagon()
        question = f"""What are people discussing and debating about: "{event_title}"?

Look for:
- Social media discussions (Twitter/X, Reddit, forums)
- News commentary and expert opinions
- Prediction market discussions

Summarize in 2-3 sentences the main viewpoints and arguments."""

        result = await octagon.research_question(question, "")
        
        # Handle new structured response format
        result_text = result.get("text", "") if isinstance(result, dict) else result
        
        if result_text:
            # Truncate to 2-3 sentences if too long
            sentences = result_text.split('. ')
            if len(sentences) > 4:
                result_text = '. '.join(sentences[:3]) + '.'
            return result_text
        
        return "Limited public discussion available for this market."
    
    async def generate_research_questions(
        self,
        event_title: str,
        event_subtitle: str,
        series_category: str,
        market_probability: float,
        candlesticks: Optional[List[Dict[str, Any]]] = None,
        anomalies: Optional[List[ChartAnomaly]] = None
    ) -> Dict[str, str]:
        """Generate 5 timely, mutually exclusive research questions.

        Uses Octagon to understand current context, then Gemini to generate
        structured questions that don't overlap.

        Args:
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            series_category: Category of the series
            market_probability: Current market probability (0-100)
            candlesticks: Optional list of candlestick data for price context
            anomalies: Optional list of detected chart anomalies

        Returns:
            Dict with keys q1-q5 containing the research questions,
            plus 'current_state_summary' with the Octagon research context
        """
        # Step 1: Use Octagon to get current context on this topic
        octagon = self._init_octagon()
        context_question = f"""What are people currently searching for, discussing, and debating about: "{event_title}"?

Identify:
1. The most recent news and developments (last 7 days)
2. Key data points people are looking for
3. Expert opinions being cited
4. Upcoming events or deadlines
5. Common questions and concerns

Be specific about dates, names, and current events."""

        context_result = await octagon.research_question(context_question, "")
        
        # Handle new structured response format
        current_context = context_result.get("text", "") if isinstance(context_result, dict) else context_result
        context_citations = context_result.get("citations", []) if isinstance(context_result, dict) else []

        # Build chart context if candlesticks provided
        chart_context = ""
        if candlesticks and len(candlesticks) > 0:
            prices = []
            for candle in candlesticks:
                price = candle.get("price")
                if price is not None:
                    if isinstance(price, dict):
                        price = price.get("close", 0)
                    prices.append(float(price) if price else 0)

            if prices:
                min_price = min(prices) / 100 if max(prices) > 1 else min(prices)
                max_price = max(prices) / 100 if max(prices) > 1 else max(prices)
                latest_price = prices[-1] / 100 if prices[-1] > 1 else prices[-1]
                first_price = prices[0] / 100 if prices[0] > 1 else prices[0]
                price_change = latest_price - first_price
                trend = "upward" if price_change > 0.05 else "downward" if price_change < -0.05 else "sideways"

                chart_context = f"""
Price Chart Analysis:
- Price range: {min_price*100:.1f}% to {max_price*100:.1f}% YES probability
- Starting price: {first_price*100:.1f}% → Current price: {latest_price*100:.1f}%
- Overall trend: {trend} ({price_change*100:+.1f} percentage points)
- Data points: {len(candlesticks)} periods"""

        # Add anomalies to chart context
        if anomalies:
            anomaly_lines = ["", "Significant Price Movements Detected:"]
            for a in anomalies:
                anomaly_lines.append(f"- {a.date_readable}: {abs(a.change_pct):.1f}pp {a.direction} ({a.price_before*100:.1f}% → {a.price_after*100:.1f}%)")
            chart_context += "\n".join(anomaly_lines)

        # Step 2: Use Gemini to generate 5 mutually exclusive questions based on the context
        prompt = f"""You are generating 5 research questions for a prediction market analysis page.

Market: "{event_title}"
Subtitle: {event_subtitle}
Category: {series_category}
Current market odds: {market_probability:.1f}% YES
{chart_context}

Current context and trending topics:
{current_context[:3000] if current_context else "No current context available."}

Generate exactly 5 MUTUALLY EXCLUSIVE research questions. Each question must:
- Cover a completely different angle (NO overlap between questions)
- Be timely and reference current events, dates, or recent developments
- Use natural, conversational phrasing
- Include specific names, dates, or terms from the market

The 5 questions MUST cover these distinct areas:
- q1: Direct question about the core outcome
- q2: Recent news/developments (specific to last week)
- q3: Expert predictions and forecasts (cite specific sources)
- q4: Key data, statistics, or indicators (specific metrics)
- q5: Timeline and upcoming catalysts (specific dates)

IMPORTANT: Questions must NOT overlap. Each should be independently researchable."""

        result, _ = await self._gemini_generate_structured(
            prompt, ResearchQuestions, use_search_grounding=False
        )
        
        # Format the current state summary as HTML with citations
        if current_context:
            formatted_context = format_text_with_citations(current_context, context_citations)
        else:
            formatted_context = ""
        
        if result:
            return {
                "q1": result.q1,
                "q2": result.q2,
                "q3": result.q3,
                "q4": result.q4,
                "q5": result.q5,
                "current_state_summary": formatted_context,
            }
        
        # Fallback if structured output fails
        logger.warning("Structured output failed for questions, using defaults")
        return {
            "q1": f"Will {event_title}?",
            "q2": f"What is the latest news about {event_title}?",
            "q3": f"What do experts predict about {event_title}?",
            "q4": f"What data supports predictions about {event_title}?",
            "q5": f"When will {event_title} be decided?",
            "current_state_summary": formatted_context,
        }
    
    def _format_paragraphs_as_html(self, paragraphs: List[str]) -> str:
        """Format paragraphs as HTML for richtext display.
        
        Args:
            paragraphs: List of paragraph strings
            
        Returns:
            HTML string with paragraphs
        """
        if not paragraphs:
            return ""
        
        html_parts = []
        for para in paragraphs:
            # Convert [1], [2], etc. to superscript for any inline citations
            formatted_para = re.sub(
                r'\[(\d+)\]',
                r'<sup>[\1]</sup>',
                para
            )
            html_parts.append(f"<p>{formatted_para}</p>")
        
        return "".join(html_parts)
    
    def _format_paragraphs_with_footnotes(
        self,
        paragraphs: List[str],
        grounding_sources: List[Dict[str, str]]
    ) -> str:
        """Format paragraphs and footnotes as HTML for richtext display.
        
        Args:
            paragraphs: List of paragraph strings with inline citations like [1], [2]
            grounding_sources: List of source dicts with 'title' and 'uri' from Gemini grounding
            
        Returns:
            HTML string with paragraphs and footnotes section
        """
        if not paragraphs:
            return ""
        
        # Format paragraphs as HTML <p> tags
        # Convert inline citations [1] to superscript
        html_parts = []
        for para in paragraphs:
            # Convert [1], [2], etc. to superscript
            formatted_para = re.sub(
                r'\[(\d+)\]',
                r'<sup>[\1]</sup>',
                para
            )
            html_parts.append(f"<p>{formatted_para}</p>")
        
        # Add footnotes section using real grounding sources
        if grounding_sources:
            html_parts.append("<hr>")
            html_parts.append("<p><strong>Sources:</strong></p>")
            html_parts.append("<ol>")
            for source in grounding_sources:
                title = source.get("title", "")
                uri = source.get("uri", "")
                if title and uri:
                    html_parts.append(f'<li><a href="{uri}">{title}</a></li>')
                elif title:
                    html_parts.append(f"<li>{title}</li>")
                elif uri:
                    html_parts.append(f'<li><a href="{uri}">{uri}</a></li>')
            html_parts.append("</ol>")
        
        return "".join(html_parts)
    
    async def research_question(
        self,
        question: str,
        event_title: str,
        event_subtitle: str
    ) -> Dict[str, Any]:
        """Research a single question using Octagon Deep Research.
        
        Uses Octagon for research, then Gemini for structured formatting.
        
        Args:
            question: The research question to answer
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            
        Returns:
            Dict with 'subtitle', 'table_data', and 'paragraph' keys
        """
        # Step 1: Use Octagon Deep Research for the actual research
        octagon = self._init_octagon()
        context = f'Prediction market: "{event_title}" which resolves {event_subtitle}'
        research_result = await octagon.research_question(question, context)
        
        # Handle new structured response format
        research_text = research_result.get("text", "") if isinstance(research_result, dict) else research_result
        research_citations = research_result.get("citations", []) if isinstance(research_result, dict) else []
        
        if not research_text:
            return {
                "subtitle": question[:50],
                "table_data": [],
                "paragraph": "Research data not available.",
                "citations": []
            }
        
        # Step 2: Use Gemini to format the research into structured output
        format_prompt = f"""Based on this research, extract and format the key findings.

Research findings:
{research_text}

Format this into:
- subtitle: A concise 5-10 word title for this research section (plain text)
- table_data: Exactly 3 key data points. Each should have:
  - label: Short metric name (plain text only)
  - value: The data with source in parentheses (plain text only)
- paragraphs: 2-3 separate paragraphs summarizing the key findings

IMPORTANT: Do NOT use markdown formatting (no ** or *). Use plain text only.
Preserve any source citations [1], [2], etc. from the research."""

        result, _ = await self._gemini_generate_structured(
            format_prompt, QuestionResearch, use_search_grounding=False
        )
        
        if result:
            # Format paragraphs as HTML with clickable citations
            raw_paragraph = "\n\n".join(result.paragraphs)
            formatted_paragraph = format_text_with_citations(raw_paragraph, research_citations)
            
            return {
                "subtitle": result.subtitle,
                "table_data": [{"label": item.label, "value": item.value} for item in result.table_data],
                "paragraph": formatted_paragraph,
                "citations": research_citations
            }
        
        # Fallback: return raw research text with citations
        logger.warning("Structured formatting failed, using raw research text")
        return {
            "subtitle": question[:50],
            "table_data": [],
            "paragraph": format_text_with_citations(research_text, research_citations),
            "citations": research_citations
        }
    
    async def research_what_could_change(
        self,
        event_title: str,
        market_probability: float,
        close_time: str
    ) -> Dict[str, str]:
        """Research key catalysts that could change the market.
        
        Uses Octagon for research, then Gemini for structured formatting.
        
        Args:
            event_title: Title of the event
            market_probability: Current market probability (0-100)
            close_time: Settlement/close time
            
        Returns:
            Dict with 'subtitle' and 'paragraph' keys
        """
        # Step 1: Use Octagon Deep Research for catalyst research
        question = f"""What are the key catalysts or events that could significantly change the probability of this prediction market?

Prediction market: "{event_title}"
Current probability: {market_probability:.1f}% YES
Settlement date: {close_time}

Please identify:
1. Bullish catalysts (could push YES higher) with specific events and dates
2. Bearish catalysts (could push NO higher) with specific events and dates
3. Timeline of key dates to watch before settlement"""

        octagon = self._init_octagon()
        research_result = await octagon.research_question(question, "")
        
        # Handle new structured response format
        research_text = research_result.get("text", "") if isinstance(research_result, dict) else research_result
        research_citations = research_result.get("citations", []) if isinstance(research_result, dict) else []
        
        if not research_text:
            return {
                "subtitle": "Key Catalysts",
                "paragraph": "Catalyst analysis not available."
            }
        
        # Step 2: Use Gemini to format into structured output
        format_prompt = f"""Based on this research, format the key catalysts.

Research findings:
{research_text}

Format this into:
- subtitle: A concise section title (e.g., "Key Catalysts", "Events to Watch")
- paragraphs: 2-3 separate paragraphs summarizing the catalysts

IMPORTANT: Do NOT use markdown formatting (no ** or *). Use plain text only.
Preserve any source citations [1], [2], etc. from the research."""

        result, _ = await self._gemini_generate_structured(
            format_prompt, CatalystResearch, use_search_grounding=False
        )
        
        if result:
            raw_paragraph = "\n\n".join(result.paragraphs)
            formatted_paragraph = format_text_with_citations(raw_paragraph, research_citations)
            return {
                "subtitle": result.subtitle,
                "paragraph": formatted_paragraph
            }
        
        # Fallback: return raw research text with citations
        logger.warning("Structured formatting failed for catalysts, using raw text")
        return {
            "subtitle": "Key Catalysts",
            "paragraph": format_text_with_citations(research_text, research_citations)
        }
    
    def generate_transparency_section(self) -> Dict[str, str]:
        """Generate the transparency/methodology section (static template).
        
        Returns:
            Dict with 'subtitle' and 'paragraph' keys
        """
        return {
            "subtitle": "Analysis Methodology",
            "paragraph": """This analysis was generated using AI-powered research tools that aggregate and synthesize information from multiple sources.

**Data Sources:** This analysis uses Octagon Deep Research to gather and analyze relevant information about this prediction market.

**Limitations:** AI-generated analysis may contain errors or outdated information. Market conditions can change rapidly, and past patterns may not predict future outcomes. This analysis is provided for informational purposes only and should not be considered financial advice.

**Updates:** Analysis is generated periodically and may not reflect the most recent developments. Always verify critical information from primary sources before making decisions."""
        }
    
    async def _gemini_pro_generate(self, prompt: str) -> str:
        """Generate content using Gemini Pro model for advanced analysis.
        
        Args:
            prompt: The prompt to send to Gemini Pro
            
        Returns:
            Generated text response
        """
        client = self._init_gemini()
        if not client:
            logger.warning("Gemini not configured, skipping Pro generation")
            return ""
        
        try:
            from google.genai import types
            
            response = client.models.generate_content(
                model=self.gemini_config.pro_model,
                contents=prompt,
            )
            
            return response.text or ""
            
        except Exception as e:
            logger.error(f"Error generating content with Gemini Pro: {e}")
            return ""
    
    async def interpret_candlestick_chart(
        self,
        candlesticks: List[Dict[str, Any]],
        event_title: str,
        event_subtitle: str,
        market_ticker: str,
        current_state_summary: str = "",
        anomalies: Optional[List[ChartAnomaly]] = None,
        anomaly_research: Optional[Dict[str, str]] = None
    ) -> str:
        """Interpret candlestick chart data using Gemini Pro.

        Args:
            candlesticks: List of candlestick data points from Kalshi API
            event_title: Title of the event for context
            event_subtitle: Subtitle explaining the resolution criteria
            market_ticker: Market ticker for context
            current_state_summary: Current state of affairs from Octagon research
            anomalies: Optional list of detected price anomalies
            anomaly_research: Optional dict mapping anomaly dates to research findings

        Returns:
            HTML paragraph with chart interpretation
        """
        if not candlesticks:
            return "<p>No historical price data available for this market.</p>"
        
        # Summarize the candlestick data for the LLM
        total_candles = len(candlesticks)
        
        # Get price range and trends
        prices = []
        volumes = []
        for candle in candlesticks:
            price = candle.get("price")
            if price is not None:
                # Convert to cents if needed (Kalshi uses cents)
                if isinstance(price, dict):
                    price = price.get("close", 0)
                prices.append(float(price) if price else 0)
            vol = candle.get("volume", 0)
            volumes.append(vol if vol else 0)
        
        if not prices:
            return "<p>Unable to interpret chart data - no price information available.</p>"
        
        # Calculate summary statistics
        min_price = min(prices) / 100 if max(prices) > 1 else min(prices)  # Convert cents to dollars
        max_price = max(prices) / 100 if max(prices) > 1 else max(prices)
        latest_price = prices[-1] / 100 if prices[-1] > 1 else prices[-1]
        first_price = prices[0] / 100 if prices[0] > 1 else prices[0]
        total_volume = sum(volumes)
        
        # Determine trend
        price_change = latest_price - first_price
        trend = "upward" if price_change > 0.05 else "downward" if price_change < -0.05 else "sideways"
        
        # Sample some candlesticks for context (first, middle, last)
        sample_candles = []
        if total_candles >= 3:
            indices = [0, total_candles // 2, total_candles - 1]
            for i in indices:
                c = candlesticks[i]
                ts = c.get("end_period_ts", 0)
                # Convert Unix timestamp to human-readable date for LLM
                try:
                    date_str = datetime.fromtimestamp(ts).strftime("%B %d, %Y %I:%M %p")
                except (ValueError, OSError):
                    date_str = "unknown"
                sample_candles.append({
                    "date": date_str,
                    "price": f"{(prices[i] / 100 if prices[i] > 1 else prices[i]) * 100:.1f}%",
                    "volume": volumes[i]
                })
        
        # Truncate current state summary for prompt
        context_snippet = current_state_summary[:1500] if current_state_summary else "No additional context available."

        # Build anomaly section with research findings
        anomaly_section = ""
        if anomalies and len(anomalies) > 0:
            anomaly_lines = ["Significant Price Movements Detected:"]
            for a in anomalies:
                research = ""
                if anomaly_research and a.date in anomaly_research:
                    # Truncate research to first 300 chars
                    research_text = anomaly_research[a.date][:300] if anomaly_research[a.date] else ""
                    if research_text:
                        research = f"\n   Context: {research_text}..."
                anomaly_lines.append(f"- {a.date_readable}: {abs(a.change_pct):.1f}pp {a.direction} ({a.price_before*100:.1f}% → {a.price_after*100:.1f}%){research}")
            anomaly_section = "\n".join(anomaly_lines) + "\n"

        prompt = f"""You are a financial analyst interpreting a prediction market price chart.

Market: "{event_title}" ({market_ticker})
Resolution: {event_subtitle}

Current Context (recent news and developments):
{context_snippet}

{anomaly_section}
Chart Summary:
- Total data points: {total_candles}
- Price range: ${min_price:.2f} to ${max_price:.2f} (YES probability as decimal)
- Current price: ${latest_price:.2f} ({latest_price*100:.1f}% YES probability)
- Starting price: ${first_price:.2f} ({first_price*100:.1f}% YES probability)
- Overall trend: {trend}
- Total volume traded: {total_volume:,} contracts

Sample data points (early, middle, recent):
{json.dumps(sample_candles, indent=2)}

Write a 2-3 paragraph technical analysis of this prediction market's price action:
1. Describe the overall price trend and any significant movements
2. IMPORTANT: Explain what caused any significant price spikes or drops using the provided context
3. Note volume patterns and what they suggest about market conviction
4. Identify any support/resistance levels or key price points
5. Provide insight into what the chart suggests about market sentiment

Focus on factual observations. Do not give trading advice.
IMPORTANT: Do NOT use markdown formatting. Use plain text only."""

        analysis = await self._gemini_pro_generate(prompt)
        
        if analysis:
            return clean_markdown_response(analysis)
        
        return f"<p>The market has traded between {min_price*100:.1f}% and {max_price*100:.1f}% YES probability, with a current reading of {latest_price*100:.1f}%. Total volume: {total_volume:,} contracts.</p>"

    def detect_chart_anomalies(
        self,
        candlesticks: List[Dict[str, Any]],
        threshold_pct: float = 10.0
    ) -> List[ChartAnomaly]:
        """Detect significant price movements (anomalies) from candlestick data.
        
        Args:
            candlesticks: List of candlestick data points from Kalshi API
            threshold_pct: Minimum percentage point change to flag as anomaly
            
        Returns:
            List of ChartAnomaly objects sorted by magnitude (largest first)
        """
        if not candlesticks or len(candlesticks) < 2:
            return []
        
        anomalies = []
        
        # Extract prices and timestamps
        data_points = []
        for candle in candlesticks:
            price = candle.get("price")
            if price is not None:
                if isinstance(price, dict):
                    price = price.get("close", 0)
                price = float(price) if price else 0
                # Normalize to 0-1 range
                if price > 1:
                    price = price / 100
                
                ts = candle.get("end_period_ts", 0)
                data_points.append({"price": price, "ts": ts})
        
        if len(data_points) < 2:
            return []
        
        # Detect period-over-period anomalies
        for i in range(1, len(data_points)):
            prev = data_points[i - 1]
            curr = data_points[i]
            
            change_pct = (curr["price"] - prev["price"]) * 100  # Percentage points
            
            if abs(change_pct) >= threshold_pct:
                # Convert timestamp to readable date
                try:
                    dt = datetime.fromtimestamp(curr["ts"])
                    date_iso = dt.strftime("%Y-%m-%d")
                    date_readable = dt.strftime("%B %d, %Y")
                except (ValueError, OSError):
                    date_iso = "unknown"
                    date_readable = "unknown date"
                
                direction = "spike" if change_pct > 0 else "drop"
                
                anomaly = ChartAnomaly(
                    date=date_iso,
                    date_readable=date_readable,
                    price_before=prev["price"],
                    price_after=curr["price"],
                    change_pct=round(change_pct, 1),
                    direction=direction,
                    description=f"{abs(change_pct):.1f}pp {direction} on {date_readable} ({prev['price']*100:.1f}% → {curr['price']*100:.1f}%)"
                )
                anomalies.append(anomaly)
        
        # Sort by magnitude (largest first) and limit to top 5
        anomalies.sort(key=lambda x: abs(x.change_pct), reverse=True)
        return anomalies[:5]

    async def research_anomalies(
        self,
        anomalies: List[ChartAnomaly],
        event_title: str
    ) -> Dict[str, str]:
        """Research what caused each anomaly using Octagon.
        
        Args:
            anomalies: List of detected anomalies
            event_title: Title of the event for context
            
        Returns:
            Dict mapping anomaly date to research findings
        """
        if not anomalies:
            return {}
        
        octagon = self._init_octagon()
        research_results = {}
        
        async def research_single_anomaly(anomaly: ChartAnomaly) -> tuple:
            question = f"""What news, events, or developments related to "{event_title}" occurred on or around {anomaly.date_readable}?

This prediction market saw a significant {anomaly.direction} of {abs(anomaly.change_pct):.1f} percentage points on this date.

Identify:
1. Any news announcements or press releases
2. Official statements or decisions
3. Data releases or reports
4. Expert commentary or forecasts
5. Any other events that could explain this price movement

Be specific about what happened and cite sources."""

            try:
                result = await octagon.research_question(question, "")
                # Handle new structured response format
                text = result.get("text", "") if isinstance(result, dict) else result
                citations = result.get("citations", []) if isinstance(result, dict) else []
                # Format with citations
                formatted = format_text_with_citations(text, citations) if text else ""
                return (anomaly.date, formatted)
            except Exception as e:
                logger.error(f"Error researching anomaly on {anomaly.date}: {e}")
                return (anomaly.date, f"Unable to research: {str(e)}")
        
        # Research all anomalies in parallel
        tasks = [research_single_anomaly(a) for a in anomalies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                date, findings = result
                research_results[date] = findings
        
        return research_results
    
    async def run_octagon_research(
        self,
        event: Dict[str, Any],
        markets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run Octagon Deep Research on an event.
        
        Args:
            event: Event data dictionary
            markets: List of market data dictionaries
            
        Returns:
            Dict with 'full_text', 'probabilities', and 'confidence' keys
        """
        octagon = self._init_octagon()
        
        try:
            logger.info(f"Running Octagon research for event {event.get('event_ticker', 'UNKNOWN')}")
            research_text = await octagon.research_event(event, markets)
            
            # Parse probabilities from the research text
            probabilities = self._parse_probabilities(research_text, markets)
            confidence = self._parse_confidence(research_text)
            
            return {
                "full_text": research_text,
                "probabilities": probabilities,
                "confidence": confidence
            }
            
        except Exception as e:
            logger.error(f"Error running Octagon research: {e}")
            return {
                "full_text": f"Research error: {str(e)}",
                "probabilities": {},
                "confidence": 5
            }
    
    def _parse_probabilities(
        self,
        research_text: str,
        markets: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Parse probability predictions from Octagon research text.
        
        Args:
            research_text: Full research text from Octagon
            markets: List of market data to get tickers
            
        Returns:
            Dict mapping market ticker to probability (0-100)
        """
        probabilities = {}
        
        # Try to extract probability mentions for each market
        for market in markets:
            ticker = market.get("ticker", "")
            if not ticker:
                continue
            
            # Look for patterns like "TICKER: XX%" or "TICKER: XX% probability"
            patterns = [
                rf'{re.escape(ticker)}[:\s]+(\d+(?:\.\d+)?)\s*%',
                rf'{re.escape(ticker)}.*?(\d+(?:\.\d+)?)\s*%\s*probability',
                rf'{re.escape(ticker)}.*?probability[:\s]+(\d+(?:\.\d+)?)\s*%',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, research_text, re.IGNORECASE)
                if match:
                    try:
                        prob = float(match.group(1))
                        if 0 <= prob <= 100:
                            probabilities[ticker] = prob
                            break
                    except ValueError:
                        continue
        
        return probabilities
    
    def _parse_confidence(self, research_text: str) -> int:
        """Parse confidence score from Octagon research text.
        
        Args:
            research_text: Full research text from Octagon
            
        Returns:
            Confidence score (1-10), defaults to 5
        """
        # Look for confidence mentions
        patterns = [
            r'confidence[:\s]+(\d+)\s*/\s*10',
            r'confidence[:\s]+(\d+)\s*(?:out of 10)?',
            r'(\d+)\s*/\s*10\s*confidence',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, research_text, re.IGNORECASE)
            if match:
                try:
                    conf = int(match.group(1))
                    if 1 <= conf <= 10:
                        return conf
                except ValueError:
                    continue
        
        return 5  # Default confidence
    
    async def generate_key_takeaway(
        self,
        event_title: str,
        event_subtitle: str,
        octagon_text: str,
        q_summaries: List[str],
        model_probability: float,
        market_probability: float,
        anomalies: Optional[List[ChartAnomaly]] = None,
        close_time: str = ""
    ) -> str:
        """Generate a one-line key takeaway from all research.

        Args:
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            octagon_text: Full Octagon research text
            q_summaries: List of paragraph summaries from Q1-Q5
            model_probability: Model's probability prediction
            market_probability: Current market probability
            anomalies: Optional list of chart anomalies
            close_time: Market close/settlement time

        Returns:
            One-line key takeaway (max 15 words)
        """
        q_text = "\n".join([f"Q{i+1}: {s[:200]}" for i, s in enumerate(q_summaries) if s])
        
        # Format anomalies for context
        anomaly_text = ""
        if anomalies:
            anomaly_lines = [f"- {a.description}" for a in anomalies[:3]]
            anomaly_text = f"\n## Recent Price Movements:\n" + "\n".join(anomaly_lines)
        
        # Format close time
        close_info = ""
        if close_time:
            close_info = f"\n- Market closes: {close_time}"

        prompt = f"""You are a research analyst summarizing findings for a prediction market.

## Market: "{event_title}"
{event_subtitle}

## Octagon Deep Research Analysis:
{octagon_text[:3000]}

## Grounded Research Findings:
{q_text}
{anomaly_text}

## Market Data:
- Model probability (Octagon): {model_probability:.1f}%
- Current market probability: {market_probability:.1f}%{close_info}

Based on ALL the research above, write a single sentence (max 15 words) that captures the most important finding or insight.

This should be a neutral, factual observation - NOT a trading recommendation or financial advice.

Good examples:
- "Recent polling shows a significant shift in voter sentiment since October."
- "Fed officials have signaled a more hawkish stance in recent statements."
- "Historical data suggests similar events resolved YES 70% of the time."

Respond with ONLY the one-line takeaway, nothing else."""

        response = await self._gemini_generate(prompt, use_search_grounding=False)
        
        # Clean up the response - take just the first line/sentence
        if response:
            lines = response.strip().split('\n')
            return lines[0][:200]  # Limit length
        
        return "Analysis in progress."
    
    async def generate_executive_summary(
        self,
        event_title: str,
        event_subtitle: str,
        series_category: str,
        octagon_text: str,
        q_findings: List[Dict[str, str]],
        what_could_change: str,
        model_probability: float,
        market_probability: float,
        anomalies: Optional[List[ChartAnomaly]] = None,
        close_time: str = ""
    ) -> str:
        """Generate 2-3 paragraph executive summary from all research.
        
        Args:
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            series_category: Category of the series
            octagon_text: Full Octagon research text
            q_findings: List of dicts with 'subtitle' and 'paragraph' for Q1-Q5
            what_could_change: Summary of key catalysts
            model_probability: Model's probability prediction
            market_probability: Current market probability
            anomalies: Optional list of chart anomalies
            close_time: Market close/settlement time
            
        Returns:
            2-3 paragraph executive summary
        """
        q_text = "\n".join([
            f"{i+1}. {f.get('subtitle', '')}: {f.get('paragraph', '')[:150]}..."
            for i, f in enumerate(q_findings) if f
        ])
        
        # Format anomalies for context
        anomaly_text = ""
        if anomalies:
            anomaly_lines = [f"- {a.description}" for a in anomalies[:3]]
            anomaly_text = "\n\n## Recent Price Movements:\n" + "\n".join(anomaly_lines)
        
        # Format close time
        close_info = ""
        if close_time:
            close_info = f"\n- Market closes: {close_time}"
        
        prompt = f"""You are writing a research summary for a prediction market analysis page.

## Market: "{event_title}"
{event_subtitle}
Category: {series_category}

## Octagon Deep Research Analysis:
{octagon_text[:4000]}

## Grounded Research Summary:
{q_text}

## Key Catalysts:
{what_could_change[:1000]}
{anomaly_text}

## Market Data:
- Model probability (Octagon): {model_probability:.1f}%
- Current market probability: {market_probability:.1f}%{close_info}

Write a 2-3 paragraph research summary that:

**Paragraph 1: Background**
- Explain what this market is about and why it matters
- Provide context on the current situation
- State what the market is predicting

**Paragraph 2: Key Findings**
- Synthesize the most important findings from the research
- Highlight relevant data points and evidence
- If there were significant price movements, explain what caused them

**Paragraph 3: Factors to Watch**
- Identify the key factors that could influence the outcome
- Note upcoming events or dates that may be significant
- Mention areas where more information is needed

IMPORTANT: This is a neutral research summary. Do NOT provide trading recommendations, financial advice, or suggest whether to buy or sell. Present facts and analysis only.

Use clear, professional language. Be specific with data points. Format for readability with bold key terms."""

        return await self._gemini_generate(prompt, use_search_grounding=False)
    
    def _calculate_edge(self, model_prob: float, market_prob: float) -> float:
        """Calculate edge in percentage points."""
        return model_prob - market_prob
    
    def _calculate_expected_return(
        self,
        model_prob: float,
        market_prob: float
    ) -> float:
        """Calculate expected return using simple Kelly-style calculation."""
        if market_prob <= 0 or market_prob >= 100:
            return 0.0
        
        # Convert to decimals
        p = model_prob / 100
        q = market_prob / 100
        
        # Simple expected return: (model_prob * payout) - (1 - model_prob) * stake
        # For binary market: payout = 1/market_prob
        if q > 0:
            expected_return = (p * (1/q - 1)) - (1 - p)
            return expected_return * 100  # Return as percentage
        return 0.0
    
    def _calculate_r_score(
        self,
        edge: float,
        confidence: int
    ) -> float:
        """Calculate R-score (z-score based on edge and confidence).

        Higher confidence = lower uncertainty = higher R-score for same edge.
        """
        # Assume standard deviation inversely proportional to confidence
        # At confidence 10, std = 5pp; at confidence 1, std = 15pp
        std = 15 - confidence  # Simple linear relationship
        if std <= 0:
            std = 1

        return edge / std

    def _calculate_price_statistics(
        self,
        candlesticks: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Calculate price statistics from candlestick data.
        
        Args:
            candlesticks: List of candlestick data points
            
        Returns:
            Dict with 'richtext' and 'json' keys
        """
        if not candlesticks:
            return {"richtext": "<p>No price data available.</p>", "json": "{}"}
        
        # Extract prices
        prices = []
        timestamps = []
        for candle in candlesticks:
            price = candle.get("price")
            ts = candle.get("end_period_ts", 0)
            if price is not None:
                if isinstance(price, dict):
                    price = price.get("close", 0)
                price = float(price) if price else 0
                if price > 1:
                    price = price / 100  # Convert cents to probability
                prices.append(price)
                timestamps.append(ts)
        
        if not prices:
            return {"richtext": "<p>No price data available.</p>", "json": "{}"}
        
        # Calculate statistics
        current_price = prices[-1]
        ath = max(prices)
        atl = min(prices)
        ath_idx = prices.index(ath)
        atl_idx = prices.index(atl)
        
        # Format dates for ATH/ATL
        try:
            ath_date = datetime.fromtimestamp(timestamps[ath_idx]).strftime("%b %d, %Y")
        except:
            ath_date = "Unknown"
        try:
            atl_date = datetime.fromtimestamp(timestamps[atl_idx]).strftime("%b %d, %Y")
        except:
            atl_date = "Unknown"
        
        # Calculate changes
        if len(prices) >= 7:
            change_7d = (current_price - prices[-7]) * 100  # percentage points
        else:
            change_7d = None
        
        if len(prices) >= 30:
            change_30d = (current_price - prices[-30]) * 100
        else:
            change_30d = None
        
        # Calculate volatility (standard deviation of daily returns)
        if len(prices) >= 2:
            returns = [(prices[i] - prices[i-1]) / prices[i-1] if prices[i-1] != 0 else 0 
                      for i in range(1, len(prices))]
            if returns:
                mean_return = sum(returns) / len(returns)
                variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
                volatility = (variance ** 0.5) * 100  # As percentage
            else:
                volatility = 0
        else:
            volatility = 0
        
        # Determine volatility level
        if volatility > 10:
            vol_level = "High"
        elif volatility > 5:
            vol_level = "Medium"
        else:
            vol_level = "Low"
        
        # Build richtext summary
        richtext = f"<p><strong>All-Time High:</strong> {ath*100:.1f}% ({ath_date})</p>"
        richtext += f"<p><strong>All-Time Low:</strong> {atl*100:.1f}% ({atl_date})</p>"
        richtext += f"<p><strong>Current Price:</strong> {current_price*100:.1f}%</p>"
        if change_7d is not None:
            richtext += f"<p><strong>7-Day Change:</strong> {change_7d:+.1f}pp</p>"
        if change_30d is not None:
            richtext += f"<p><strong>30-Day Change:</strong> {change_30d:+.1f}pp</p>"
        richtext += f"<p><strong>Volatility:</strong> {vol_level}</p>"
        
        # Build JSON
        json_data = {
            "ath": round(ath * 100, 1),
            "atl": round(atl * 100, 1),
            "ath_date": ath_date,
            "atl_date": atl_date,
            "current": round(current_price * 100, 1),
            "change_7d": round(change_7d, 1) if change_7d is not None else None,
            "change_30d": round(change_30d, 1) if change_30d is not None else None,
            "volatility": round(volatility, 2),
            "volatility_level": vol_level
        }
        
        return {"richtext": richtext, "json": json.dumps(json_data)}
    
    async def generate_analysis(
        self,
        event: Dict[str, Any],
        markets: List[Dict[str, Any]],
        candlesticks: Optional[List[Dict[str, Any]]] = None,
        orderbook: Optional[Dict[str, Any]] = None,
        recent_trades: Optional[List[Dict[str, Any]]] = None,
        settled_markets: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Generate complete analysis for an event.

        This is the main orchestration method that:
        1. Runs parallel API calls (Exa crawl, Octagon research, question generation)
        2. Researches each question with Octagon Deep Research
        3. Synthesizes executive summary and key takeaway
        4. Interprets candlestick chart data with Gemini Pro
        5. Formats orderbook, trades, and historical data

        Args:
            event: Event data dictionary with title, subtitle, ticker, etc.
            markets: List of market data dictionaries for this event
            candlesticks: Optional list of candlestick data for chart analysis
            orderbook: Optional orderbook data for liquidity analysis
            recent_trades: Optional list of recent trades
            settled_markets: Optional list of settled markets for track record

        Returns:
            Dict containing all analysis fields ready for Webflow
        """
        event_ticker = event.get("event_ticker", "")
        event_title = event.get("name", event.get("title", ""))
        event_subtitle = event.get("subtitle", event.get("sub_title", ""))
        series_ticker = event.get("series_ticker", "")
        series_title = event.get("series_title", "")
        series_category = event.get("series_category", "")
        close_time = event.get("close_time", "")
        
        # Calculate market probability from markets data
        market_probability = 50.0  # Default
        if markets:
            # Use the first market's yes_bid as representative
            first_market = markets[0]
            yes_bid = first_market.get("yes_bid_dollars") or first_market.get("yes_bid", 0)
            if yes_bid:
                try:
                    market_probability = float(yes_bid) * 100
                except (ValueError, TypeError):
                    pass
        
        logger.info(f"Starting analysis for {event_ticker}: {event_title}")
        
        # Build Kalshi URL: https://kalshi.com/markets/{series_ticker}/{series_title_slug}/{event_ticker}
        series_slug = series_ticker.lower() if series_ticker else ""
        title_slug = re.sub(r'[^a-z0-9]+', '-', series_title.lower()).strip('-') if series_title else ""
        event_slug = event_ticker.lower() if event_ticker else ""
        kalshi_url = f"https://kalshi.com/markets/{series_slug}/{title_slug}/{event_slug}"
        
        # Calculate aggregate market stats
        total_volume = 0
        total_volume_24h = 0
        total_open_interest = 0
        if markets:
            for m in markets:
                total_volume += int(m.get("volume", 0) or 0)
                total_volume_24h += int(m.get("volume_24h", 0) or 0)
                total_open_interest += int(m.get("open_interest", 0) or 0)
        
        # Initialize analysis result with metadata
        analysis = {
            "analysis_last_updated": datetime.now().isoformat(),
            "analysis_version": "1.0",
            "analysis_owner": "AI-Generated",
            "kalshi_event_url": kalshi_url,
            "market_probability": f"{market_probability:.1f}",
            "total_volume": str(total_volume),
            "total_volume_24h": str(total_volume_24h),
            "total_open_interest": str(total_open_interest),
            "close_time": close_time,
        }
        
        # Detect chart anomalies before Phase 1
        anomalies = []
        if candlesticks:
            logger.info(f"Detecting chart anomalies for {event_ticker}")
            anomalies = self.detect_chart_anomalies(candlesticks, threshold_pct=8.0)
            if anomalies:
                logger.info(f"Found {len(anomalies)} significant price movements")
                for a in anomalies:
                    logger.info(f"  - {a.description}")
        
        # Phase 1: Parallel API calls
        logger.info(f"Phase 1: Running parallel API calls for {event_ticker}")
        
        # Run Exa crawl, Octagon research, question generation, and anomaly research in parallel
        exa_task = self.crawl_kalshi_page(series_ticker, series_title, event_ticker)
        octagon_task = self.run_octagon_research(event, markets)
        questions_task = self.generate_research_questions(
            event_title, event_subtitle, series_category, market_probability, candlesticks, anomalies
        )
        async def empty_dict():
            return {}
        anomaly_task = self.research_anomalies(anomalies, event_title) if anomalies else empty_dict()

        exa_result, octagon_result, questions, anomaly_research = await asyncio.gather(
            exa_task, octagon_task, questions_task, anomaly_task,
            return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(exa_result, Exception):
            logger.error(f"Exa crawl failed: {exa_result}")
            exa_result = {"content": "", "url": ""}
        if isinstance(octagon_result, Exception):
            logger.error(f"Octagon research failed: {octagon_result}")
            octagon_result = {"full_text": "", "probabilities": {}, "confidence": 5}
        if isinstance(questions, Exception):
            logger.error(f"Question generation failed: {questions}")
            questions = {"q1": "", "q2": "", "q3": "", "q4": "", "q5": "", "current_state_summary": ""}
        if isinstance(anomaly_research, Exception):
            logger.error(f"Anomaly research failed: {anomaly_research}")
            anomaly_research = {}
        
        # Add current state summary to analysis
        analysis["current_state_summary_richtext"] = questions.get("current_state_summary", "")
        
        # Add anomaly data to analysis
        if anomalies:
            anomaly_list = []
            for a in anomalies:
                anomaly_entry = {
                    "date": a.date,
                    "date_readable": a.date_readable,
                    "change_pct": a.change_pct,
                    "direction": a.direction,
                    "price_before": round(a.price_before * 100, 1),
                    "price_after": round(a.price_after * 100, 1),
                    "description": a.description,
                    "research": anomaly_research.get(a.date, "")
                }
                anomaly_list.append(anomaly_entry)
            analysis["chart_anomalies_json"] = json.dumps(anomaly_list)
        else:
            analysis["chart_anomalies_json"] = "[]"
        
        # Extract Octagon results
        octagon_text = octagon_result.get("full_text", "")
        octagon_probabilities = octagon_result.get("probabilities", {})
        confidence = octagon_result.get("confidence", 5)
        
        # Get model probability (use first market's prediction from Octagon)
        model_probability = market_probability  # Default to market if no model prediction
        if octagon_probabilities and markets:
            first_ticker = markets[0].get("ticker", "")
            if first_ticker in octagon_probabilities:
                model_probability = octagon_probabilities[first_ticker]
        
        # Phase 2: Generate summaries from Exa content
        logger.info(f"Phase 2: Generating Exa summaries for {event_ticker}")
        
        exa_content = exa_result.get("content", "")
        exa_url = exa_result.get("url", "")
        
        contract_summary_task = self.summarize_contract_rules(
            exa_content, exa_url, event_title
        )
        discussion_summary_task = self.summarize_market_discussion(
            exa_content, exa_url, event_title
        )
        
        contract_summary, discussion_summary = await asyncio.gather(
            contract_summary_task, discussion_summary_task
        )
        
        analysis["contract_snapshot_summary"] = clean_markdown_response(contract_summary)
        analysis["market_discussion_summary"] = clean_markdown_response(discussion_summary)
        
        # Phase 3: Research each question sequentially (to avoid rate limits)
        logger.info(f"Phase 3: Researching questions for {event_ticker}")
        
        q_findings = []
        for i in range(1, 6):
            q_key = f"q{i}"
            question = questions.get(q_key, "")
            
            if question:
                logger.info(f"Researching Q{i}: {question[:50]}...")
                finding = await self.research_question(question, event_title, event_subtitle)
                q_findings.append(finding)
                
                # Store in analysis
                analysis[f"q{i}_subtitle"] = finding.get("subtitle", question[:50])
                
                # Format table data as HTML table
                table_data = finding.get("table_data", [])
                analysis[f"q{i}_table_richtext"] = format_table_html(table_data)
                
                # Clean and format paragraph as HTML
                paragraph = finding.get("paragraph", "")
                analysis[f"q{i}_paragraph_richtext"] = clean_markdown_response(paragraph)
                
                # Small delay between questions to avoid rate limits
                await asyncio.sleep(2)
            else:
                q_findings.append({})
                analysis[f"q{i}_subtitle"] = ""
                analysis[f"q{i}_table_richtext"] = ""
                analysis[f"q{i}_paragraph_richtext"] = ""
        
        # Phase 4: Research what could change
        logger.info(f"Phase 4: Researching catalysts for {event_ticker}")
        
        what_could_change = await self.research_what_could_change(
            event_title, market_probability, close_time
        )
        analysis["what_could_change_subtitle"] = what_could_change.get("subtitle", "Key Catalysts")
        analysis["what_could_change_paragraph_richtext"] = clean_markdown_response(
            what_could_change.get("paragraph", "")
        )
        
        # Add transparency section
        transparency = self.generate_transparency_section()
        analysis["transparency_subtitle"] = transparency["subtitle"]
        analysis["transparency_paragraph_richtext"] = clean_markdown_response(transparency["paragraph"])
        
        # Phase 5: Generate executive summary and key takeaway
        logger.info(f"Phase 5: Generating executive summary for {event_ticker}")
        
        q_paragraphs = [f.get("paragraph", "") for f in q_findings]
        
        key_takeaway = await self.generate_key_takeaway(
            event_title, event_subtitle, octagon_text,
            q_paragraphs, model_probability, market_probability,
            anomalies, close_time
        )
        
        executive_summary = await self.generate_executive_summary(
            event_title, event_subtitle, series_category,
            octagon_text, q_findings,
            what_could_change.get("paragraph", ""),
            model_probability, market_probability,
            anomalies, close_time
        )
        
        # Phase 6: Chart interpretation (if candlestick data provided)
        if candlesticks:
            logger.info(f"Phase 6: Interpreting chart data for {event_ticker}")
            market_ticker = markets[0].get("ticker", "") if markets else ""
            current_state = analysis.get("current_state_summary_richtext", "")
            chart_analysis = await self.interpret_candlestick_chart(
                candlesticks, event_title, event_subtitle, market_ticker, current_state,
                anomalies, anomaly_research
            )
            analysis["chart_analysis_richtext"] = chart_analysis
            # Transform to Lightweight Charts format for frontend
            chart_data = transform_candlesticks_for_lightweight_charts(candlesticks)
            analysis["candlestick_data_json"] = json.dumps(chart_data)
        else:
            analysis["chart_analysis_richtext"] = "<p>No historical price data available.</p>"
            analysis["candlestick_data_json"] = "[]"

        # Phase 7: Process orderbook data
        if orderbook:
            orderbook_richtext, orderbook_json = format_orderbook_summary(orderbook)
            analysis["orderbook_summary_richtext"] = orderbook_richtext
            analysis["orderbook_json"] = orderbook_json
        else:
            analysis["orderbook_summary_richtext"] = "<p>No orderbook data available.</p>"
            analysis["orderbook_json"] = "{}"

        # Phase 8: Process recent trades
        if recent_trades:
            trades_richtext, trades_json = format_trades_summary(recent_trades)
            analysis["trade_activity_richtext"] = trades_richtext
            analysis["recent_trades_json"] = trades_json
        else:
            analysis["trade_activity_richtext"] = "<p>No recent trade data available.</p>"
            analysis["recent_trades_json"] = "[]"

        # Phase 9: Process historical track record
        if settled_markets:
            history_richtext, history_json = format_historical_track_record(settled_markets)
            analysis["historical_resolutions_richtext"] = history_richtext
            analysis["historical_resolutions_json"] = history_json
        else:
            analysis["historical_resolutions_richtext"] = "<p>No historical resolution data available for this series.</p>"
            analysis["historical_resolutions_json"] = "[]"

        # Phase 10: Calculate price statistics from candlesticks
        if candlesticks:
            price_stats = self._calculate_price_statistics(candlesticks)
            analysis["price_statistics_richtext"] = price_stats["richtext"]
            analysis["price_statistics_json"] = price_stats["json"]
        else:
            analysis["price_statistics_richtext"] = "<p>No price statistics available.</p>"
            analysis["price_statistics_json"] = "{}"

        # Phase 11: Format key dates
        key_dates_richtext, key_dates_json = format_key_dates(event, markets)
        analysis["key_dates_richtext"] = key_dates_richtext
        analysis["key_dates_json"] = key_dates_json

        # Calculate metrics
        edge = self._calculate_edge(model_probability, market_probability)
        expected_return = self._calculate_expected_return(model_probability, market_probability)
        r_score = self._calculate_r_score(edge, confidence)
        
        # Add final fields
        analysis["model_probability"] = f"{model_probability:.1f}"
        analysis["confidence_score"] = str(confidence)
        analysis["edge_pp"] = f"{edge:.1f}"
        analysis["expected_return"] = f"{expected_return:.1f}"
        analysis["r_score"] = f"{r_score:.2f}"
        analysis["key_takeaway"] = clean_markdown_response(key_takeaway)  # One-line summary/verdict
        analysis["executive_summary_richtext"] = clean_markdown_response(executive_summary)
        
        logger.info(f"Completed analysis for {event_ticker}")
        
        return analysis
    
    async def close(self):
        """Close any open clients."""
        if self._octagon_client:
            await self._octagon_client.close()
