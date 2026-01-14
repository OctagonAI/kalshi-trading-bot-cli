"""
Simple Kalshi API Client with RSA authentication
"""

import hashlib
import json
import time
import base64
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import httpx
from loguru import logger
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from config import KalshiConfig


class KalshiClient:
    """Simple Kalshi API client for basic trading operations."""
    
    def __init__(self, config: KalshiConfig, minimum_time_remaining_hours: float = 1.0, max_markets_per_event: int = 10, max_close_ts: Optional[int] = None):
        self.config = config
        self.base_url = config.base_url
        self.api_key = config.api_key
        self.private_key = config.private_key
        self.minimum_time_remaining_hours = minimum_time_remaining_hours
        self.max_markets_per_event = max_markets_per_event
        self.max_close_ts = max_close_ts
        self.client = None
        self.session_token = None
        
    async def login(self):
        """Login to Kalshi API."""
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0
        )
        
        # For now, we'll assume the client handles authentication
        # In the real implementation, you'd do login here
        logger.info(f"Connected to Kalshi API at {self.base_url}")
        
    async def get_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get top events sorted by 24-hour volume."""
        try:
            # First, fetch ALL events from the platform using pagination
            all_events = await self._fetch_all_events()
            
            # Calculate total volume_24h for each event from its markets 
            # (API already filters for "open" status events)
            enriched_events = []
            now = datetime.now(timezone.utc)
            minimum_time_remaining = self.minimum_time_remaining_hours * 3600  # Convert hours to seconds
            filter_enabled = self.max_close_ts is not None
            markets_seen = 0
            markets_kept = 0
            events_dropped_by_expiration = 0
            
            for event in all_events:
                # Get markets and select top N by volume
                all_markets = event.get("markets", [])
                markets_seen += len(all_markets)

                # Optionally filter markets by close time if max_close_ts is provided
                if self.max_close_ts is not None and all_markets:
                    filtered_markets = []
                    for market in all_markets:
                        close_time_str = market.get("close_time", "")
                        if not close_time_str:
                            continue
                        try:
                            # Parse ISO8601 close_time
                            if close_time_str.endswith('Z'):
                                close_dt = datetime.fromisoformat(close_time_str.replace('Z', '+00:00'))
                            else:
                                close_dt = datetime.fromisoformat(close_time_str)
                            if close_dt.tzinfo is None:
                                close_dt = close_dt.replace(tzinfo=timezone.utc)
                            close_ts = int(close_dt.timestamp())
                            if close_ts <= self.max_close_ts:
                                filtered_markets.append(market)
                        except Exception:
                            # If parsing fails, skip this market from filtered list
                            continue
                    all_markets = filtered_markets
                
                # If no markets remain after filtering, skip this event
                if not all_markets:
                    if filter_enabled:
                        events_dropped_by_expiration += 1
                    continue

                if filter_enabled:
                    markets_kept += len(all_markets)

                # Sort markets by volume (descending) and take top N
                sorted_markets = sorted(all_markets, key=lambda m: m.get("volume", 0), reverse=True)
                top_markets = sorted_markets[:self.max_markets_per_event]
                
                if len(all_markets) > self.max_markets_per_event:
                    logger.info(f"Event {event.get('event_ticker', '')} has {len(all_markets)} markets, selecting top {len(top_markets)} by volume")
                
                # Calculate volume metrics for this event using top markets
                total_liquidity = 0
                total_volume = 0
                total_volume_24h = 0
                total_open_interest = 0
                
                for market in top_markets:
                    total_liquidity += market.get("liquidity", 0)
                    total_volume += market.get("volume", 0)
                    total_volume_24h += market.get("volume_24h", 0)
                    total_open_interest += market.get("open_interest", 0)
                
                # Calculate time remaining if strike_date exists
                time_remaining_hours = None
                strike_date_str = event.get("strike_date", "")
                
                if strike_date_str:
                    try:
                        # Parse strike date
                        if strike_date_str.endswith('Z'):
                            strike_date = datetime.fromisoformat(strike_date_str.replace('Z', '+00:00'))
                        else:
                            strike_date = datetime.fromisoformat(strike_date_str)
                        
                        # Ensure timezone awareness
                        if strike_date.tzinfo is None:
                            strike_date = strike_date.replace(tzinfo=timezone.utc)
                        
                        # Calculate time remaining
                        time_remaining = (strike_date - now).total_seconds()
                        time_remaining_hours = time_remaining / 3600
                        
                        # Optional: Skip events that are very close to striking
                        if time_remaining > 0 and time_remaining < minimum_time_remaining:
                            logger.info(f"Event {event.get('event_ticker', '')} strikes in {time_remaining/60:.1f} minutes, skipping")
                            continue
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse strike_date '{strike_date_str}' for event {event.get('event_ticker', '')}: {e}")
                        # Continue without time filtering for this event
                
                # If no top markets selected, skip event
                if not top_markets:
                    continue

                enriched_events.append({
                    "event_ticker": event.get("event_ticker", ""),
                    "title": event.get("title", ""),
                    "subtitle": event.get("sub_title", ""),
                    "volume": total_volume,
                    "volume_24h": total_volume_24h,
                    "liquidity": total_liquidity,
                    "open_interest": total_open_interest,
                    "category": event.get("category", ""),
                    "mutually_exclusive": event.get("mutually_exclusive", False),
                    "strike_date": strike_date_str,
                    "strike_period": event.get("strike_period", ""),
                    "time_remaining_hours": time_remaining_hours,
                    "markets": top_markets,  # Store the top markets with the event
                    "total_markets": len(all_markets),  # Store original market count
                })
            
            # Sort by volume_24h (descending) for true popularity ranking
            enriched_events.sort(key=lambda x: x.get("volume_24h", 0), reverse=True)
            
            # Return only the top N events as requested
            top_events = enriched_events[:limit]
            
            # Summary log for expiration filter effects
            if filter_enabled and markets_seen > 0:
                dropped = markets_seen - markets_kept
                logger.info(
                    f"Expiration filter summary: kept {markets_kept}/{markets_seen} markets; "
                    f"dropped {dropped}. Events dropped due to no remaining markets: {events_dropped_by_expiration}"
                )
            
            logger.info(f"Retrieved {len(all_events)} total events, filtered to {len(enriched_events)} active events, returning top {len(top_events)} by 24h volume")
            return top_events
            
        except Exception as e:
            logger.error(f"Error getting events: {e}")
            return []
    
    async def _fetch_all_events(self) -> List[Dict[str, Any]]:
        """Fetch all events from the platform using pagination."""
        all_events = []
        cursor = None
        page = 1
        
        while True:
            try:
                headers = await self._get_headers("GET", "/trade-api/v2/events")
                params = {
                    "limit": 100,  # Maximum events per page
                    "status": "open",  # Only get open events (active/tradeable)
                    "with_nested_markets": "true"
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
                    logger.error(f"Received None response from API")
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
        
        logger.info(f"Fetched {len(all_events)} total events from {page} pages")
        return all_events
    
    async def get_markets_for_event(self, event_ticker: str) -> List[Dict[str, Any]]:
        """Get markets for a specific event (returns pre-filtered top markets from get_events)."""
        # This method is kept for compatibility but now returns pre-filtered markets
        # The actual filtering happens in get_events() to avoid duplicate API calls
        logger.warning(f"get_markets_for_event called for {event_ticker} - markets should be pre-loaded from get_events()")
        
        # Fallback: fetch markets directly if needed
        try:
            headers = await self._get_headers("GET", "/trade-api/v2/markets")
            params = {"event_ticker": event_ticker, "status": "open"}
            # Pass through server-side filter if available
            if self.max_close_ts is not None:
                params["max_close_ts"] = self.max_close_ts
            response = await self.client.get(
                "/trade-api/v2/markets",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            all_markets = data.get("markets", [])

            # Client-side filtering as a fallback when server-side filtering is not applied
            if self.max_close_ts is not None and all_markets:
                filtered_markets = []
                for market in all_markets:
                    close_time_str = market.get("close_time", "")
                    if not close_time_str:
                        continue
                    try:
                        if close_time_str.endswith('Z'):
                            close_dt = datetime.fromisoformat(close_time_str.replace('Z', '+00:00'))
                        else:
                            close_dt = datetime.fromisoformat(close_time_str)
                        if close_dt.tzinfo is None:
                            close_dt = close_dt.replace(tzinfo=timezone.utc)
                        close_ts = int(close_dt.timestamp())
                        if close_ts <= self.max_close_ts:
                            filtered_markets.append(market)
                    except Exception:
                        continue
                all_markets = filtered_markets
            
            # Sort by volume and take top markets
            sorted_markets = sorted(all_markets, key=lambda m: m.get("volume", 0), reverse=True)
            top_markets = sorted_markets[:self.max_markets_per_event]
            
            # Return markets without odds for research
            simple_markets = []
            for market in top_markets:
                simple_markets.append({
                    "ticker": market.get("ticker", ""),
                    "title": market.get("title", ""),
                    "subtitle": market.get("subtitle", ""),
                    "volume": market.get("volume", 0),
                    "open_time": market.get("open_time", ""),
                    "close_time": market.get("close_time", ""),
                    # Note: NOT including yes_bid, no_bid, yes_ask, no_ask for research
                })
            
            logger.info(f"Retrieved {len(simple_markets)} markets for event {event_ticker} (top {len(top_markets)} by volume)")
            return simple_markets
            
        except Exception as e:
            logger.error(f"Error getting markets for event {event_ticker}: {e}")
            return []
    
    async def get_market_with_odds(self, ticker: str) -> Dict[str, Any]:
        """Get a specific market with current odds for trading."""
        try:
            headers = await self._get_headers("GET", f"/trade-api/v2/markets/{ticker}")
            response = await self.client.get(
                f"/trade-api/v2/markets/{ticker}",
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            market = data.get("market", {})
            
            # Get specific fields
            yes_bid = market.get("yes_bid", 0)
            no_bid = market.get("no_bid", 0)
            yes_ask = market.get("yes_ask", 0)
            no_ask = market.get("no_ask", 0)
            
            # Note: Event-level filtering is already done in get_events()
            return {
                "ticker": market.get("ticker", ""),
                "title": market.get("title", ""),
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "volume": market.get("volume", 0),
                "status": market.get("status", ""),
                "close_time": market.get("close_time", ""),
            }
            
        except Exception as e:
            logger.error(f"Error getting market {ticker}: {e}")
            return {}
    
    async def get_user_positions(self) -> List[Dict[str, Any]]:
        """Get all user positions."""
        try:
            headers = await self._get_headers("GET", "/trade-api/v2/portfolio/positions")
            response = await self.client.get(
                "/trade-api/v2/portfolio/positions",
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Debug: Log the raw API response structure
            logger.debug(f"Position API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            # The API returns market_positions, not positions
            positions = data.get("market_positions", [])
            
            # Also check for event_positions (though we primarily need market_positions)
            event_positions = data.get("event_positions", [])
            
            logger.info(f"Retrieved {len(positions)} market positions and {len(event_positions)} event positions")
            logger.debug(f"Market positions: {positions[:3] if positions else 'None'}")  # Log first 3 for debugging
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting user positions: {e}")
            return []
    
    async def has_position_in_market(self, ticker: str) -> bool:
        """Check if user already has a position in the specified market."""
        try:
            positions = await self.get_user_positions()
            
            for position in positions:
                if position.get("ticker") == ticker:
                    # Check if position has any contracts
                    # In Kalshi API: positive = YES contracts, negative = NO contracts, 0 = no position
                    position_size = position.get("position", 0)
                    
                    if position_size != 0:
                        position_type = "YES" if position_size > 0 else "NO"
                        logger.info(f"Found existing position in {ticker}: {abs(position_size)} {position_type} contracts")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking position for {ticker}: {e}")
            return False  # If we can't check, assume no position to be safe

    async def place_order(self, ticker: str, side: str, amount: float) -> Dict[str, Any]:
        """Place a simple market order."""
        try:
            # Generate a unique client order ID
            import uuid
            client_order_id = str(uuid.uuid4())
            
            # Convert dollar amount to cents for buy_max_cost
            buy_max_cost_cents = int(amount * 100)
            
            # For market orders, we want to spend up to our dollar amount
            # Set a high count but limit with buy_max_cost to control actual spending
            max_contracts = 1000  # High number to ensure we can buy up to our budget
            
            order_data = {
                "ticker": ticker,
                "side": side,  # "yes" or "no"
                "action": "buy",
                "type": "market",
                "client_order_id": client_order_id,
                "count": max_contracts,  # High count to allow buying up to budget
                "buy_max_cost": buy_max_cost_cents  # Actual spending limit in cents
            }
            
            headers = await self._get_headers("POST", "/trade-api/v2/portfolio/orders")
            response = await self.client.post(
                "/trade-api/v2/portfolio/orders",
                headers=headers,
                json=order_data
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Order placed: {ticker} {side} ${amount} (max cost: {buy_max_cost_cents} cents)")
            return {"success": True, "order_id": result.get("order_id", ""), "client_order_id": client_order_id}
            
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {"success": False, "error": str(e)}
    
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
    
    async def get_market_candlesticks(
        self,
        market_ticker: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        period_interval: int = 60  # 60 minutes = 1 hour
    ) -> List[Dict[str, Any]]:
        """Get candlestick data for a specific market.
        
        Args:
            market_ticker: The market ticker (e.g., 'INXD-24JAN01')
            start_ts: Start timestamp in Unix seconds (default: 30 days ago)
            end_ts: End timestamp in Unix seconds (default: now)
            period_interval: Candlestick period in minutes (1, 60, or 1440)
            
        Returns:
            List of candlestick data points
        """
        try:
            # Default to last 30 days if not specified
            now_ts = int(time.time())
            if end_ts is None:
                end_ts = now_ts
            if start_ts is None:
                start_ts = now_ts - (30 * 24 * 60 * 60)  # 30 days ago
            
            headers = await self._get_headers("GET", "/trade-api/v2/markets/candlesticks")
            params = {
                "market_tickers": market_ticker,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval
            }
            
            response = await self.client.get(
                "/trade-api/v2/markets/candlesticks",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            markets = data.get("markets", [])
            
            if markets and len(markets) > 0:
                candlesticks = markets[0].get("candlesticks", [])
                logger.info(f"Retrieved {len(candlesticks)} candlesticks for {market_ticker}")
                return candlesticks
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting candlesticks for {market_ticker}: {e}")
            return []
    
    async def close(self):
        """Close the HTTP client."""
        if self.client:
            await self.client.aclose() 