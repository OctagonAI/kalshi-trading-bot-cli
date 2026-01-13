"""
Kalshi Data Export Script

Fetches active series, open events, and open markets from Kalshi API and exports them
to 3 CSV files: markets.csv, events.csv, and contracts.csv.

Only active/open data is exported (no expired or settled items):
- Events: fetched with status=open filter
- Markets: extracted from open events (via with_nested_markets=true)
- Series: filtered to only those with at least one open event

Optional: Generate AI analysis for events and sync to Webflow CMS.
Use --full-analysis flag to run analysis for all events missing analysis fields.
"""

import argparse
import asyncio
import csv
import json
import re
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
from config import KalshiConfig, WebflowConfig, GeminiConfig, ExaConfig, OctagonConfig, load_config


def slugify(text: str) -> str:
    """Convert text to URL-safe slug.
    
    Examples:
        "Companies" -> "companies"
        "Science and Technology" -> "science-and-technology"
        "Companies - AI" -> "companies-ai"
    """
    if not text:
        return ""
    # Lowercase
    slug = text.lower()
    # Replace spaces and special chars with hyphens
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    # Collapse multiple hyphens
    slug = re.sub(r'-+', '-', slug)
    return slug


# Hardcoded categories to filter events by
ALLOWED_CATEGORIES = {
    "Companies",
    "Crypto",
    "Economics",
    "Financials",
    "Mentions",
    "Science and Technology",
}


class KalshiDataExporter:
    """Exports Kalshi data to CSV files and syncs to Webflow CMS."""
    
    # Maximum number of events to analyze in test mode
    MAX_TEST_ANALYSES = 5
    
    def __init__(
        self,
        config: KalshiConfig,
        webflow_config: Optional[WebflowConfig] = None,
        gemini_config: Optional[GeminiConfig] = None,
        exa_config: Optional[ExaConfig] = None,
        octagon_config: Optional[OctagonConfig] = None,
        full_analysis: bool = False
    ):
        self.config = config
        self.webflow_config = webflow_config
        self.gemini_config = gemini_config
        self.exa_config = exa_config
        self.octagon_config = octagon_config
        self.full_analysis = full_analysis
        
        # Force real API (not demo) since this is read-only
        self.base_url = "https://api.elections.kalshi.com"
        self.api_key = config.api_key
        self.private_key = config.private_key
        self.client = None
        self.webflow_client = None
        self.analysis_generator = None
        
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
                # Note: product_metadata is NOT available in list endpoint
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
            
            # Note: Event metadata (image_url, featured_image_url, competition, competition_scope)
            # is NOT available in the list endpoint - would require individual API calls per event
            
            # E) Octagon Analysis - metadata (all empty)
            row.update({
                "analysis_last_updated": "",
                "analysis_version": "",
                "analysis_owner": "",
            })
            
            # F) Octagon Analysis - Section 1 Executive Summary (all empty)
            row.update({
                "confidence_score": "",
                "key_takeaway": "",
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
            # Note: Market metadata (image_url, color_code) is NOT available in list endpoint
            row.update({
                "rules_primary": market.get("rules_primary", ""),
                "rules_secondary": market.get("rules_secondary", ""),
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
    
    # ==================== Webflow API Methods ====================
    
    async def _init_webflow_client(self):
        """Initialize Webflow HTTP client."""
        if not self.webflow_config or not self.webflow_config.is_configured:
            return False
        
        self.webflow_client = httpx.AsyncClient(
            base_url="https://api.webflow.com/v2",
            headers={
                "Authorization": f"Bearer {self.webflow_config.api_token}",
                "Content-Type": "application/json",
                "accept": "application/json",
            },
            timeout=60.0
        )
        return True
    
    async def _close_webflow_client(self):
        """Close Webflow HTTP client."""
        if self.webflow_client:
            await self.webflow_client.aclose()
            self.webflow_client = None
    
    def _init_analysis_generator(self):
        """Initialize the analysis generator if configs are available."""
        if self.analysis_generator is not None:
            return True
        
        if not (self.gemini_config and self.gemini_config.is_configured):
            logger.warning("Gemini not configured, analysis generation disabled")
            return False
        
        if not self.octagon_config:
            logger.warning("Octagon not configured, analysis generation disabled")
            return False
        
        try:
            from analysis_generator import AnalysisGenerator
            self.analysis_generator = AnalysisGenerator(
                gemini_config=self.gemini_config,
                octagon_config=self.octagon_config,
                exa_config=self.exa_config or ExaConfig(api_key="")
            )
            logger.info("Analysis generator initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize analysis generator: {e}")
            return False
    
    def _needs_analysis(self, webflow_item: Optional[Dict[str, Any]]) -> bool:
        """Check if Webflow item is missing analysis fields.
        
        Args:
            webflow_item: Existing Webflow item dict with 'fieldData' key, or None
            
        Returns:
            True if analysis should be generated, False if already complete
        """
        if webflow_item is None:
            return True
        
        # Required fields that indicate analysis was run
        analysis_fields = [
            'key-takeaway',
            'executive-summary-richtext',
            'model-probability',
            'q1-subtitle',
        ]
        
        field_data = webflow_item.get('fieldData', {})
        return any(not field_data.get(field) for field in analysis_fields)
    
    async def fetch_webflow_items(self, collection_id: str) -> Dict[str, dict]:
        """Fetch all items from Webflow collection, return as slug -> item dict."""
        items_by_slug = {}
        offset = 0
        limit = 100
        
        while True:
            try:
                response = await self.webflow_client.get(
                    f"/collections/{collection_id}/items",
                    params={"offset": offset, "limit": limit}
                )
                response.raise_for_status()
                
                data = response.json()
                items = data.get("items", [])
                
                for item in items:
                    field_data = item.get("fieldData", {})
                    slug = field_data.get("slug", "")
                    if slug:
                        items_by_slug[slug] = {
                            "id": item.get("id"),
                            "fieldData": field_data
                        }
                
                # Check if there are more pages
                pagination = data.get("pagination", {})
                total = pagination.get("total", 0)
                if offset + limit >= total:
                    break
                offset += limit
                
                # Rate limiting - 1 second delay between requests
                await asyncio.sleep(1)
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limited - wait and retry
                    logger.warning("Webflow rate limited, waiting 60 seconds...")
                    await asyncio.sleep(60)
                    continue
                logger.error(f"Error fetching Webflow items: {e}")
                break
            except Exception as e:
                logger.error(f"Error fetching Webflow items: {e}")
                break
        
        logger.info(f"Fetched {len(items_by_slug)} existing items from Webflow collection {collection_id}")
        return items_by_slug
    
    async def create_webflow_item(self, collection_id: str, field_data: dict) -> Optional[str]:
        """Create a new Webflow CMS item and publish it. Returns item ID."""
        try:
            response = await self.webflow_client.post(
                f"/collections/{collection_id}/items/live",
                json={"fieldData": field_data}
            )
            response.raise_for_status()
            
            data = response.json()
            item_id = data.get("id")
            logger.info(f"Created Webflow item: {field_data.get('slug', 'unknown')} -> {item_id}")
            
            # Rate limiting
            await asyncio.sleep(1)
            return item_id
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Webflow rate limited, waiting 60 seconds...")
                await asyncio.sleep(60)
                return await self.create_webflow_item(collection_id, field_data)
            logger.error(f"Error creating Webflow item: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Error creating Webflow item: {e}")
            return None
    
    async def update_webflow_item(self, collection_id: str, item_id: str, field_data: dict) -> bool:
        """Update an existing Webflow CMS item and publish it."""
        try:
            response = await self.webflow_client.patch(
                f"/collections/{collection_id}/items/live",
                json={"items": [{"id": item_id, "fieldData": field_data}]}
            )
            response.raise_for_status()
            
            logger.info(f"Updated Webflow item: {field_data.get('slug', item_id)}")
            
            # Rate limiting
            await asyncio.sleep(1)
            return True
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Webflow rate limited, waiting 60 seconds...")
                await asyncio.sleep(60)
                return await self.update_webflow_item(collection_id, item_id, field_data)
            logger.error(f"Error updating Webflow item: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Error updating Webflow item: {e}")
            return False
    
    async def upsert_webflow_item(
        self, 
        collection_id: str, 
        existing_items: Dict[str, dict], 
        field_data: dict
    ) -> Optional[str]:
        """Create or update a Webflow CMS item based on slug. Returns item ID."""
        slug = field_data.get("slug", "")
        
        if slug in existing_items:
            # Update existing item
            item_id = existing_items[slug]["id"]
            success = await self.update_webflow_item(collection_id, item_id, field_data)
            return item_id if success else None
        else:
            # Create new item
            return await self.create_webflow_item(collection_id, field_data)
    
    async def sync_markets_to_webflow(self, markets_rows: List[Dict[str, Any]]) -> Dict[str, str]:
        """Sync market pages to Webflow CMS.
        
        Markets are browse pages based on category/tag combinations.
        Category pages are created first, then tag pages with parent_page references.
        
        Returns: slug -> webflow_id mapping
        """
        if not self.webflow_config or not self.webflow_config.is_configured:
            logger.warning("Webflow not configured, skipping market sync")
            return {}
        
        collection_id = self.webflow_config.markets_collection_id
        slug_to_id: Dict[str, str] = {}
        
        # Fetch existing items
        existing_items = await self.fetch_webflow_items(collection_id)
        
        # Separate category pages (no parent) and tag pages (have parent)
        category_pages = []
        tag_pages = []
        
        for market in markets_rows:
            category = market.get("series_category_filter", "")
            tag = market.get("series_tag_filter", "")
            
            if tag:
                # Tag page (has parent)
                tag_pages.append(market)
            else:
                # Category page (no parent)
                category_pages.append(market)
        
        logger.info(f"Syncing {len(category_pages)} category pages and {len(tag_pages)} tag pages to Webflow")
        
        # First pass: Upsert category pages (no parent_page reference)
        for market in category_pages:
            category = market.get("series_category_filter", "")
            slug = slugify(category)
            name = category  # Use category name as display name
            
            field_data = {
                "name": name,
                "slug": slug,
                "series-category-filter": category,
                "series-tag-filter": "",
            }
            
            item_id = await self.upsert_webflow_item(collection_id, existing_items, field_data)
            if item_id:
                slug_to_id[slug] = item_id
                # Also add to existing_items for reference resolution
                existing_items[slug] = {"id": item_id, "fieldData": field_data}
        
        # Second pass: Upsert tag pages (with parent_page reference)
        for market in tag_pages:
            category = market.get("series_category_filter", "")
            tag = market.get("series_tag_filter", "")
            
            # Generate slug for tag page
            slug = slugify(f"{category}-{tag}")
            name = f"{category} - {tag}"
            
            # Look up parent category's Webflow ID
            parent_slug = slugify(category)
            parent_id = slug_to_id.get(parent_slug)
            
            field_data = {
                "name": name,
                "slug": slug,
                "series-category-filter": category,
                "series-tag-filter": tag,
            }
            
            # Add parent_page reference if parent exists
            if parent_id:
                field_data["parent-page"] = parent_id
            
            item_id = await self.upsert_webflow_item(collection_id, existing_items, field_data)
            if item_id:
                slug_to_id[slug] = item_id
        
        logger.info(f"Synced {len(slug_to_id)} market pages to Webflow")
        return slug_to_id
    
    async def sync_events_to_webflow(
        self, 
        events_rows: List[Dict[str, Any]], 
        market_id_map: Dict[str, str],
        markets_by_event: Optional[Dict[str, List[Dict[str, Any]]]] = None
    ) -> int:
        """Sync events to Webflow CMS with market_page references.
        
        Each event is linked to a market page based on its series_category.
        Optionally generates AI analysis for events missing analysis fields.
        
        Logic:
        - INSERT: If item doesn't exist in Webflow
        - UPDATE: If item exists but missing analysis fields (and analysis is enabled)
        - SKIP: If item exists AND has analysis
        
        Args:
            events_rows: List of event data dictionaries
            market_id_map: Mapping of market slug -> webflow_id
            markets_by_event: Mapping of event_ticker -> list of markets (for analysis)
            
        Returns: Number of events synced
        """
        if not self.webflow_config or not self.webflow_config.is_configured:
            logger.warning("Webflow not configured, skipping event sync")
            return 0
        
        collection_id = self.webflow_config.events_collection_id
        
        # Counters
        analysis_count = 0
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        # Initialize analysis generator if needed
        analysis_enabled = self._init_analysis_generator()
        
        # Fetch existing items
        existing_items = await self.fetch_webflow_items(collection_id)
        
        mode_str = "full" if self.full_analysis else f"test (max {self.MAX_TEST_ANALYSES})"
        logger.info(f"Syncing {len(events_rows)} events to Webflow (analysis mode: {mode_str})")
        
        for event in events_rows:
            event_ticker = event.get("event_ticker", "")
            event_name = event.get("name", "")
            series_category = event.get("series_category", "")
            
            # Generate slug from event_ticker
            slug = slugify(event_ticker) if event_ticker else slugify(event_name)
            
            if not slug:
                logger.warning(f"Skipping event with no valid slug: {event}")
                continue
            
            # Find the market page to reference
            market_slug = slugify(series_category)
            market_id = market_id_map.get(market_slug)
            
            # Check if item exists in Webflow
            existing = existing_items.get(slug)
            
            # Build base field data for Webflow
            field_data = {
                "name": event_name,
                "slug": slug,
                "event-ticker": event_ticker,
                "series-ticker": event.get("series_ticker", ""),
                "subtitle": event.get("subtitle", ""),
                "series-category": series_category,
                "strike-date": event.get("strike_date", ""),
                "strike-period": event.get("strike_period", ""),
            }
            
            # Add market_page reference if market exists
            if market_id:
                field_data["market-page"] = market_id
            
            if existing is None:
                # Case 1: INSERT - item doesn't exist
                should_run_analysis = False
                if analysis_enabled:
                    if self.full_analysis:
                        should_run_analysis = True
                    else:
                        should_run_analysis = analysis_count < self.MAX_TEST_ANALYSES
                
                if should_run_analysis and markets_by_event is not None:
                    logger.info(f"Generating analysis ({analysis_count + 1}): {event_ticker}")
                    event_markets = markets_by_event.get(event_ticker, [])
                    try:
                        analysis = await self.analysis_generator.generate_analysis(event, event_markets)
                        # Add analysis fields to field_data (convert keys to Webflow format)
                        for key, value in analysis.items():
                            webflow_key = key.replace("_", "-")
                            field_data[webflow_key] = value
                        analysis_count += 1
                    except Exception as e:
                        logger.error(f"Analysis generation failed for {event_ticker}: {e}")
                
                item_id = await self.create_webflow_item(collection_id, field_data)
                if item_id:
                    inserted_count += 1
                    existing_items[slug] = {"id": item_id, "fieldData": field_data}
                    
            elif self._needs_analysis(existing):
                # Case 2: UPDATE - item exists but missing analysis
                should_run_analysis = False
                if analysis_enabled:
                    if self.full_analysis:
                        should_run_analysis = True
                    else:
                        should_run_analysis = analysis_count < self.MAX_TEST_ANALYSES
                
                if should_run_analysis and markets_by_event is not None:
                    logger.info(f"Generating analysis ({analysis_count + 1}): {event_ticker}")
                    event_markets = markets_by_event.get(event_ticker, [])
                    try:
                        analysis = await self.analysis_generator.generate_analysis(event, event_markets)
                        # Add analysis fields to field_data
                        for key, value in analysis.items():
                            webflow_key = key.replace("_", "-")
                            field_data[webflow_key] = value
                        analysis_count += 1
                        
                        # Update the item with analysis
                        item_id = existing["id"]
                        success = await self.update_webflow_item(collection_id, item_id, field_data)
                        if success:
                            updated_count += 1
                    except Exception as e:
                        logger.error(f"Analysis generation failed for {event_ticker}: {e}")
                        skipped_count += 1
                else:
                    skipped_count += 1
            else:
                # Case 3: SKIP - item exists AND has analysis
                skipped_count += 1
        
        logger.info(
            f"Webflow sync complete: Inserted={inserted_count}, Updated={updated_count}, "
            f"Skipped={skipped_count}, Analysis generated={analysis_count}"
        )
        return inserted_count + updated_count
    
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
            logger.info(f"Fetched {len(events_with_markets)} open events with nested markets")
            
            # Get unique series tickers from open events
            all_series_tickers = set(e.get("series_ticker", "") for e in events_with_markets if e.get("series_ticker"))
            logger.info(f"Found {len(all_series_tickers)} unique series with open events")
            
            # Fetch all series (for metadata) then filter to only those with open events
            all_series = await self.fetch_all_series()
            series = [s for s in all_series if s.get("ticker", "") in all_series_tickers]
            logger.info(f"Filtered to {len(series)} active series (from {len(all_series)} total)")
            
            # Build series lookup
            series_by_ticker = {s.get("ticker", ""): s for s in series}
            
            # Filter events by allowed categories (based on series category)
            filtered_events_with_markets = []
            for event in events_with_markets:
                series_ticker = event.get("series_ticker", "")
                series_data = series_by_ticker.get(series_ticker, {})
                category = series_data.get("category", "")
                if category in ALLOWED_CATEGORIES:
                    filtered_events_with_markets.append(event)
            
            logger.info(f"Filtered to {len(filtered_events_with_markets)} events in allowed categories: {ALLOWED_CATEGORIES}")
            
            # Extract markets from filtered events (ensures consistency)
            # Also build markets_by_event for analysis generation
            markets = []
            markets_by_event: Dict[str, List[Dict[str, Any]]] = {}
            for event in filtered_events_with_markets:
                event_ticker = event.get("event_ticker", "")
                event_markets = event.get("markets", [])
                markets.extend(event_markets)
                if event_ticker:
                    markets_by_event[event_ticker] = event_markets
            
            logger.info(f"Extracted {len(markets)} markets from {len(filtered_events_with_markets)} filtered events")
            
            # Use events without the nested markets field for CSV export
            events = [{k: v for k, v in e.items() if k != "markets"} for e in filtered_events_with_markets]
            
            # Filter series to only those in allowed categories
            series = [s for s in series if s.get("category", "") in ALLOWED_CATEGORIES]
            logger.info(f"Filtered to {len(series)} series in allowed categories")
            
            # Rebuild series lookup with filtered series
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
                "strike_period", "available_on_brokers",
                # C) Denormalized Kalshi Series fields
                "series_title", "series_category", "series_tags_raw", "series_frequency",
                "contract_url", "contract_terms_url", "settlement_sources_raw",
                "fee_type", "fee_multiplier", "additional_prohibitions_raw",
                # D) Octagon Analysis - metadata
                "analysis_last_updated", "analysis_version", "analysis_owner",
                # E) Octagon Analysis - Section 1 Executive Summary
                "confidence_score", "key_takeaway", "model_probability",
                "market_probability", "edge_pp", "expected_return", "r_score",
                "executive_summary_richtext",
                # F) Octagon Analysis - Section 2 Kalshi Contract Snapshot
                "kalshi_event_url", "contract_snapshot_summary", "market_discussion_summary",
                # G) Octagon Analysis - Sections 3-8 (5 dynamic questions)
                "q1_subtitle", "q1_table_richtext", "q1_paragraph_richtext",
                "q2_subtitle", "q2_table_richtext", "q2_paragraph_richtext",
                "q3_subtitle", "q3_table_richtext", "q3_paragraph_richtext",
                "q4_subtitle", "q4_table_richtext", "q4_paragraph_richtext",
                "q5_subtitle", "q5_table_richtext", "q5_paragraph_richtext",
                # H) Octagon Analysis - Section 9
                "what_could_change_subtitle", "what_could_change_paragraph_richtext",
                # I) Octagon Analysis - Section 10
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
                "rules_primary", "rules_secondary",
            ]
            
            # Write CSV files
            self.write_csv(f"markets_{timestamp}.csv", markets_rows, markets_fields)
            self.write_csv(f"events_{timestamp}.csv", events_rows, events_fields)
            self.write_csv(f"contracts_{timestamp}.csv", contracts_rows, contracts_fields)
            
            logger.info("CSV export completed successfully!")
            
            # Sync to Webflow CMS
            if self.webflow_config and self.webflow_config.is_configured:
                logger.info("Syncing to Webflow CMS...")
                
                # Initialize Webflow client
                await self._init_webflow_client()
                
                try:
                    # Sync markets (browse pages) first to get ID mapping
                    market_id_map = await self.sync_markets_to_webflow(markets_rows)
                    
                    # Sync events with market_page references and optional analysis
                    await self.sync_events_to_webflow(events_rows, market_id_map, markets_by_event)
                    
                    logger.info("Webflow sync completed successfully!")
                finally:
                    await self._close_webflow_client()
                    # Close analysis generator if initialized
                    if self.analysis_generator:
                        await self.analysis_generator.close()
            else:
                logger.info("Webflow not configured, skipping sync. Set WEBFLOW_* env vars to enable.")
            
        finally:
            await self.client.aclose()


async def main():
    """Main entry point."""
    # Parse CLI arguments
    parser = argparse.ArgumentParser(
        description="Export Kalshi data to CSV and optionally sync to Webflow with AI analysis"
    )
    parser.add_argument(
        '--full-analysis',
        action='store_true',
        help='Run analysis for all events missing analysis fields (expensive). '
             'Default: only analyze first 5 events.'
    )
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    # Create exporter with all configs
    exporter = KalshiDataExporter(
        config=config.kalshi,
        webflow_config=config.webflow,
        gemini_config=config.gemini,
        exa_config=config.exa,
        octagon_config=config.octagon,
        full_analysis=args.full_analysis
    )
    
    await exporter.export()


if __name__ == "__main__":
    asyncio.run(main())
