"""
Kalshi Data Export Script

Fetches active series, open events, and open markets from Kalshi API and exports them
to 3 CSV files: markets.csv, events.csv, and contracts.csv.

Only active/open data is exported (no expired or settled items):
- Events: fetched with status=open filter
- Markets: extracted from open events (via with_nested_markets=true)
- Series: filtered to only those with at least one open event
"""

import asyncio
import csv
import json
import time
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
import httpx
from loguru import logger
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from config import KalshiConfig, load_config


class KalshiDataExporter:
    """Exports Kalshi data to CSV files."""
    
    def __init__(self, config: KalshiConfig):
        self.config = config
        # Force real API (not demo) since this is read-only
        self.base_url = "https://api.elections.kalshi.com"
        self.api_key = config.api_key
        self.private_key = config.private_key
        self.client = None
        
    async def _get_headers(self, method: str, path: str) -> Dict[str, str]:
        """Generate headers with RSA signature."""
        timestamp = str(int(time.time() * 1000))
        
        # Create message to sign
        message = f"{timestamp}{method}{path}"
        
        # Sign the message
        signature = self._sign_message(message)
        
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json"
        }
    
    def _sign_message(self, message: str) -> str:
        """Sign a message using RSA private key."""
        try:
            # Load private key
            private_key = serialization.load_pem_private_key(
                self.private_key.encode(),
                password=None,
                backend=default_backend()
            )
            
            # Sign the message
            signature = private_key.sign(
                message.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            # Return base64 encoded signature
            return base64.b64encode(signature).decode()
            
        except Exception as e:
            logger.error(f"Error signing message: {e}")
            raise
    
    async def fetch_all_series(self) -> List[Dict[str, Any]]:
        """Fetch all series from Kalshi API with pagination.
        
        Note: Series are filtered client-side to only include those with open events,
        since the API's status filter is unreliable.
        """
        all_series = []
        cursor = None
        page = 1
        
        while True:
            try:
                headers = await self._get_headers("GET", "/trade-api/v2/series")
                params = {
                    "limit": 100,
                    # Note: status filter removed - we filter client-side based on open events
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logger.info(f"Fetching series page {page}...")
                response = await self.client.get(
                    "/trade-api/v2/series",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                if data is None:
                    logger.error("Received None response from API")
                    break
                    
                series = data.get("series", []) if isinstance(data, dict) else []
                
                if not series:
                    break
                
                all_series.extend(series)
                logger.info(f"Page {page}: {len(series)} series (total: {len(all_series)})")
                
                # Check if there's a next page
                cursor = data.get("cursor")
                if not cursor:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching series page {page}: {e}")
                break
        
        logger.info(f"Fetched {len(all_series)} total series from {page} pages (will filter to active)")
        return all_series
    
    async def fetch_all_events(self, with_nested_markets: bool = False) -> List[Dict[str, Any]]:
        """Fetch all open (active) events from Kalshi API with pagination."""
        all_events = []
        cursor = None
        page = 1
        
        while True:
            try:
                headers = await self._get_headers("GET", "/trade-api/v2/events")
                params = {
                    "limit": 100,
                    "status": "open",  # Only fetch open/active events
                    "with_nested_markets": "true" if with_nested_markets else "false"
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logger.info(f"Fetching events page {page}...")
                response = await self.client.get(
                    "/trade-api/v2/events",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                if data is None:
                    logger.error("Received None response from API")
                    break
                    
                events = data.get("events", []) if isinstance(data, dict) else []
                
                if not events:
                    break
                
                all_events.extend(events)
                logger.info(f"Page {page}: {len(events)} events (total: {len(all_events)})")
                
                # Check if there's a next page
                cursor = data.get("cursor")
                if not cursor:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching events page {page}: {e}")
                break
        
        logger.info(f"Fetched {len(all_events)} open events from {page} pages")
        return all_events
    
    async def fetch_all_markets(self) -> List[Dict[str, Any]]:
        """Fetch all open (active) markets from Kalshi API with pagination."""
        all_markets = []
        cursor = None
        page = 1
        
        while True:
            try:
                headers = await self._get_headers("GET", "/trade-api/v2/markets")
                params = {
                    "limit": 100,
                    "status": "open",  # Only fetch open/active markets
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                logger.info(f"Fetching markets page {page}...")
                response = await self.client.get(
                    "/trade-api/v2/markets",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                if data is None:
                    logger.error("Received None response from API")
                    break
                    
                markets = data.get("markets", []) if isinstance(data, dict) else []
                
                if not markets:
                    break
                
                all_markets.extend(markets)
                logger.info(f"Page {page}: {len(markets)} markets (total: {len(all_markets)})")
                
                # Check if there's a next page
                cursor = data.get("cursor")
                if not cursor:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching markets page {page}: {e}")
                break
        
        logger.info(f"Fetched {len(all_markets)} open markets from {page} pages")
        return all_markets
    
    def build_markets(self, series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build markets (browse pages) from series data."""
        markets = []
        
        # Track unique categories and tags
        categories: Set[str] = set()
        category_tags: Dict[str, Set[str]] = {}
        
        # Collect categories and tags from series
        for s in series:
            category = s.get("category", "")
            tags = s.get("tags") or []  # Handle None from API
            
            if category:
                categories.add(category)
                if category not in category_tags:
                    category_tags[category] = set()
                for tag in tags:
                    if tag:
                        category_tags[category].add(tag)
        
        # Create category pages
        for category in sorted(categories):
            markets.append({
                "name": "",  # octagon (empty)
                "slug": "",  # octagon (empty)
                "page_type": "",  # octagon (empty)
                "parent_page": "",  # octagon (empty)
                "series_category_filter": category,
                "series_tag_filter": "",  # category pages don't have tag filter
                "intro_richtext": "",  # octagon (empty)
            })
            
            # Create tag pages for this category
            for tag in sorted(category_tags.get(category, set())):
                markets.append({
                    "name": "",  # octagon (empty)
                    "slug": "",  # octagon (empty)
                    "page_type": "",  # octagon (empty)
                    "parent_page": category,  # parent is the category
                    "series_category_filter": category,
                    "series_tag_filter": tag,
                    "intro_richtext": "",  # octagon (empty)
                })
        
        logger.info(f"Built {len(markets)} markets (browse pages) from {len(series)} series")
        return markets
    
    def format_settlement_sources(self, sources: List[Dict[str, Any]]) -> str:
        """Format settlement sources as 'Name | URL' separated by newlines."""
        if not sources:
            return ""
        formatted = []
        for source in sources:
            name = source.get("name", "")
            url = source.get("url", "")
            if name or url:
                formatted.append(f"{name} | {url}")
        return "\n".join(formatted)
    
    def format_product_metadata(self, metadata: Any) -> str:
        """Format product metadata as JSON string."""
        if not metadata:
            return ""
        return json.dumps(metadata)
    
    def build_events_csv_rows(
        self, 
        events: List[Dict[str, Any]], 
        series_by_ticker: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build events CSV rows from events and series data."""
        rows = []
        
        for event in events:
            event_ticker = event.get("event_ticker", "")
            series_ticker = event.get("series_ticker", "")
            series = series_by_ticker.get(series_ticker, {})
            
            # A) Core identity & routing
            row = {
                "name": event.get("title", ""),
                "slug": "",  # octagon (empty)
            }
            
            # B) Kalshi Event fields
            row.update({
                "event_ticker": event_ticker,
                "series_ticker": series_ticker,
                "subtitle": event.get("sub_title", ""),
                "collateral_return_type": event.get("collateral_return_type", ""),
                "mutually_exclusive": str(event.get("mutually_exclusive", False)).lower(),
                "event_category_deprecated": event.get("category", ""),
                "strike_date": event.get("strike_date", ""),
                "strike_period": event.get("strike_period", ""),
                "available_on_brokers": str(event.get("available_on_brokers", False)).lower(),
                "product_metadata_raw": self.format_product_metadata(event.get("product_metadata")),
            })
            
            # C) Denormalized Kalshi Series fields
            series_tags = series.get("tags") or []
            settlement_sources = series.get("settlement_sources") or []
            additional_prohibitions = series.get("additional_prohibitions") or []
            row.update({
                "series_title": series.get("title", ""),
                "series_category": series.get("category", ""),
                "series_tags_raw": "\n".join(series_tags) if series_tags else "",
                "series_frequency": series.get("frequency", ""),
                "contract_url": series.get("contract_url", ""),
                "contract_terms_url": series.get("contract_terms_url", ""),
                "settlement_sources_raw": self.format_settlement_sources(settlement_sources),
                "fee_type": series.get("fee_type", ""),
                "fee_multiplier": str(series.get("fee_multiplier") or ""),
                "additional_prohibitions_raw": "\n".join(additional_prohibitions),
            })
            
            # D) Kalshi Event metadata (optional endpoint - may not be in response)
            event_metadata = event.get("metadata") or {}
            row.update({
                "event_image_url": event_metadata.get("image_url", "") if isinstance(event_metadata, dict) else "",
                "featured_image_url": event_metadata.get("featured_image_url", "") if isinstance(event_metadata, dict) else "",
                "competition": event_metadata.get("competition", "") if isinstance(event_metadata, dict) else "",
                "competition_scope": event_metadata.get("competition_scope", "") if isinstance(event_metadata, dict) else "",
            })
            
            # E) Octagon Analysis - metadata (all empty)
            row.update({
                "analysis_last_updated": "",
                "analysis_version": "",
                "analysis_owner": "",
            })
            
            # F) Octagon Analysis - Section 1 Executive Verdict (all empty)
            row.update({
                "confidence_score": "",
                "executive_verdict": "",
                "model_probability": "",
                "market_probability": "",
                "edge_pp": "",
                "expected_return": "",
                "r_score": "",
                "executive_summary_richtext": "",
            })
            
            # G) Octagon Analysis - Section 2 Kalshi Contract Snapshot (all empty)
            row.update({
                "kalshi_event_url": "",
                "contract_snapshot_summary": "",
                "market_discussion_summary": "",
            })
            
            # H) Octagon Analysis - Sections 3-8 (5 dynamic questions, all empty)
            for n in range(1, 6):
                row[f"q{n}_subtitle"] = ""
                row[f"q{n}_table_richtext"] = ""
                row[f"q{n}_paragraph_richtext"] = ""
            
            # I) Octagon Analysis - Section 9 (all empty)
            row.update({
                "what_could_change_subtitle": "",
                "what_could_change_paragraph_richtext": "",
            })
            
            # J) Octagon Analysis - Section 10 (all empty)
            row.update({
                "transparency_subtitle": "",
                "transparency_paragraph_richtext": "",
            })
            
            rows.append(row)
        
        logger.info(f"Built {len(rows)} event rows")
        return rows
    
    def build_contracts_csv_rows(self, markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build contracts CSV rows from markets data."""
        rows = []
        
        for market in markets:
            # Core identity
            row = {
                "name": market.get("yes_sub_title", ""),
                "market_ticker": market.get("ticker", ""),
                "event_ticker": market.get("event_ticker", ""),
                "event": "",  # octagon (empty)
            }
            
            # Descriptive
            row.update({
                "market_type": market.get("market_type", ""),
                "yes_subtitle": market.get("yes_sub_title", ""),
                "no_subtitle": market.get("no_sub_title", ""),
            })
            
            # Lifecycle & status
            settlement_timer = market.get("settlement_timer_seconds")
            row.update({
                "created_time": market.get("created_time", ""),
                "open_time": market.get("open_time", ""),
                "close_time": market.get("close_time", ""),
                "latest_expiration_time": market.get("latest_expiration_time", ""),
                "settlement_timer_seconds": str(settlement_timer) if settlement_timer is not None else "",
                "status": market.get("status", ""),
                "can_close_early": str(market.get("can_close_early") or False).lower(),
                "result": market.get("result", ""),
            })
            
            # Pricing & activity (stored as strings when defined as FixedPointDollars in API)
            row.update({
                "yes_bid_dollars": str(market.get("yes_bid_dollars", "")) if market.get("yes_bid_dollars") is not None else "",
                "yes_ask_dollars": str(market.get("yes_ask_dollars", "")) if market.get("yes_ask_dollars") is not None else "",
                "no_bid_dollars": str(market.get("no_bid_dollars", "")) if market.get("no_bid_dollars") is not None else "",
                "no_ask_dollars": str(market.get("no_ask_dollars", "")) if market.get("no_ask_dollars") is not None else "",
                "last_price_dollars": str(market.get("last_price_dollars", "")) if market.get("last_price_dollars") is not None else "",
                "volume": str(market.get("volume", 0)),
                "volume_24h": str(market.get("volume_24h", 0)),
                "open_interest": str(market.get("open_interest", 0)),
                "liquidity_dollars": str(market.get("liquidity_dollars", "")) if market.get("liquidity_dollars") is not None else "",
            })
            
            # Rules & display
            market_metadata = market.get("metadata") or {}
            row.update({
                "rules_primary": market.get("rules_primary", ""),
                "rules_secondary": market.get("rules_secondary", ""),
                "market_image_url": market_metadata.get("image_url", "") if isinstance(market_metadata, dict) else "",
                "market_color_code": market_metadata.get("color_code", "") if isinstance(market_metadata, dict) else "",
            })
            
            rows.append(row)
        
        logger.info(f"Built {len(rows)} contract rows")
        return rows
    
    def write_csv(
        self, 
        filename: str, 
        rows: List[Dict[str, Any]], 
        fieldnames: List[str]
    ):
        """Write rows to CSV file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        
        logger.info(f"Wrote {len(rows)} rows to {filepath}")
    
    async def export(self):
        """Main export function."""
        # Initialize HTTP client
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=60.0
        )
        
        try:
            logger.info("Starting Kalshi data export...")
            logger.info(f"Using API: {self.base_url}")
            
            # Fetch all data
            # Note: We fetch events WITH nested markets to ensure consistency
            # This guarantees all markets belong to open events
            events_with_markets = await self.fetch_all_events(with_nested_markets=True)
            
            # Extract markets from events (ensures consistency - all markets belong to open events)
            markets = []
            for event in events_with_markets:
                event_markets = event.get("markets", [])
                markets.extend(event_markets)
            
            logger.info(f"Extracted {len(markets)} markets from {len(events_with_markets)} open events")
            
            # Use events without the nested markets field for CSV export
            events = [{k: v for k, v in e.items() if k != "markets"} for e in events_with_markets]
            
            # Get unique series tickers from open events
            active_series_tickers = set(e.get("series_ticker", "") for e in events if e.get("series_ticker"))
            logger.info(f"Found {len(active_series_tickers)} unique series with open events")
            
            # Fetch all series (for metadata) then filter to only those with open events
            all_series = await self.fetch_all_series()
            series = [s for s in all_series if s.get("ticker", "") in active_series_tickers]
            logger.info(f"Filtered to {len(series)} active series (from {len(all_series)} total)")
            
            # Build series lookup
            series_by_ticker = {s.get("ticker", ""): s for s in series}
            
            # Build CSV rows
            markets_rows = self.build_markets(series)
            events_rows = self.build_events_csv_rows(events, series_by_ticker)
            contracts_rows = self.build_contracts_csv_rows(markets)
            
            # Generate timestamp for filenames
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Define fieldnames for each CSV
            markets_fields = [
                "name", "slug", "page_type", "parent_page",
                "series_category_filter", "series_tag_filter", "intro_richtext"
            ]
            
            events_fields = [
                # A) Core identity & routing
                "name", "slug",
                # B) Kalshi Event fields
                "event_ticker", "series_ticker", "subtitle", "collateral_return_type",
                "mutually_exclusive", "event_category_deprecated", "strike_date",
                "strike_period", "available_on_brokers", "product_metadata_raw",
                # C) Denormalized Kalshi Series fields
                "series_title", "series_category", "series_tags_raw", "series_frequency",
                "contract_url", "contract_terms_url", "settlement_sources_raw",
                "fee_type", "fee_multiplier", "additional_prohibitions_raw",
                # D) Kalshi Event metadata
                "event_image_url", "featured_image_url", "competition", "competition_scope",
                # E) Octagon Analysis - metadata
                "analysis_last_updated", "analysis_version", "analysis_owner",
                # F) Octagon Analysis - Section 1 Executive Verdict
                "confidence_score", "executive_verdict", "model_probability",
                "market_probability", "edge_pp", "expected_return", "r_score",
                "executive_summary_richtext",
                # G) Octagon Analysis - Section 2 Kalshi Contract Snapshot
                "kalshi_event_url", "contract_snapshot_summary", "market_discussion_summary",
                # H) Octagon Analysis - Sections 3-8 (5 dynamic questions)
                "q1_subtitle", "q1_table_richtext", "q1_paragraph_richtext",
                "q2_subtitle", "q2_table_richtext", "q2_paragraph_richtext",
                "q3_subtitle", "q3_table_richtext", "q3_paragraph_richtext",
                "q4_subtitle", "q4_table_richtext", "q4_paragraph_richtext",
                "q5_subtitle", "q5_table_richtext", "q5_paragraph_richtext",
                # I) Octagon Analysis - Section 9
                "what_could_change_subtitle", "what_could_change_paragraph_richtext",
                # J) Octagon Analysis - Section 10
                "transparency_subtitle", "transparency_paragraph_richtext",
            ]
            
            contracts_fields = [
                # Core identity
                "name", "market_ticker", "event_ticker", "event",
                # Descriptive
                "market_type", "yes_subtitle", "no_subtitle",
                # Lifecycle & status
                "created_time", "open_time", "close_time", "latest_expiration_time",
                "settlement_timer_seconds", "status", "can_close_early", "result",
                # Pricing & activity
                "yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
                "last_price_dollars", "volume", "volume_24h", "open_interest",
                "liquidity_dollars",
                # Rules & display
                "rules_primary", "rules_secondary", "market_image_url", "market_color_code",
            ]
            
            # Write CSV files
            self.write_csv(f"markets_{timestamp}.csv", markets_rows, markets_fields)
            self.write_csv(f"events_{timestamp}.csv", events_rows, events_fields)
            self.write_csv(f"contracts_{timestamp}.csv", contracts_rows, contracts_fields)
            
            logger.info("Export completed successfully!")
            
        finally:
            await self.client.aclose()


async def main():
    """Main entry point."""
    config = load_config()
    exporter = KalshiDataExporter(config.kalshi)
    await exporter.export()


if __name__ == "__main__":
    asyncio.run(main())
