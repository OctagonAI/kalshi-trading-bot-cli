"""
Configuration management for the simple trading bot.
"""
import os
from typing import Optional
from pathlib import Path
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KalshiConfig(BaseModel):
    """Kalshi API configuration."""
    api_key: str = Field(..., description="Kalshi API key")
    private_key: str = Field(..., description="Kalshi private key (PEM format)")
    use_demo: bool = Field(default=True, description="Use demo environment")
    
    @property
    def base_url(self) -> str:
        """Get the appropriate base URL based on environment."""
        if self.use_demo:
            return "https://demo-api.kalshi.co"
        return "https://api.elections.kalshi.com"
    
    @validator('private_key')
    def validate_private_key(cls, v):
        """Validate and format private key."""
        if not v or v == "your_kalshi_private_key_here":
            raise ValueError("KALSHI_PRIVATE_KEY is required. Please set it in your .env file.")
        
        # If it looks like a file path, try to read it
        if not v.startswith('-----BEGIN') and (Path(v).exists() or v.endswith('.pem')):
            try:
                with open(v, 'r') as f:
                    v = f.read()
            except Exception as e:
                raise ValueError(f"Could not read private key file '{v}': {e}")
        
        # Basic validation that it looks like a PEM key
        if not v.strip().startswith('-----BEGIN') or not v.strip().endswith('-----'):
            raise ValueError(
                "Private key must be in PEM format starting with '-----BEGIN' and ending with '-----'. "
                "Make sure to include \\n for line breaks in your .env file."
            )
        
        return v

class OctagonConfig(BaseModel):
    """Octagon Deep Research API configuration."""
    api_key: str = Field(..., description="Octagon API key")
    base_url: str = Field(default="https://api.octagon.ai", description="Octagon API base URL")
    
    @validator('api_key')
    def validate_api_key(cls, v):
        if not v or v == "your_octagon_api_key_here":
            raise ValueError("OCTAGON_API_KEY is required. Please set it in your .env file.")
        return v

class OpenAIConfig(BaseModel):
    """OpenAI API configuration."""
    api_key: str = Field(..., description="OpenAI API key")
    model: str = Field(default="gpt-5", description="OpenAI model to use")
    
    @validator('api_key')
    def validate_api_key(cls, v):
        if not v or v == "your_openai_api_key_here":
            raise ValueError("OPENAI_API_KEY is required. Please set it in your .env file.")
        return v


class WebflowConfig(BaseModel):
    """Webflow API configuration."""
    api_token: str = Field(default="", description="Webflow API token")
    site_id: str = Field(default="", description="Webflow site ID")
    markets_collection_id: str = Field(default="", description="Markets collection ID")
    events_collection_id: str = Field(default="", description="Events collection ID")
    
    @property
    def is_configured(self) -> bool:
        """Check if Webflow is configured."""
        return bool(self.api_token and self.site_id and self.markets_collection_id and self.events_collection_id)


class GeminiConfig(BaseModel):
    """Google Gemini API configuration."""
    api_key: str = Field(default="", description="Gemini API key")
    model: str = Field(default="gemini-2.5-flash", description="Gemini model to use")
    pro_model: str = Field(default="gemini-2.5-pro", description="Gemini Pro model for advanced analysis")
    
    @property
    def is_configured(self) -> bool:
        """Check if Gemini is configured."""
        return bool(self.api_key)


class ExaConfig(BaseModel):
    """Exa API configuration for web crawling."""
    api_key: str = Field(default="", description="Exa API key")
    
    @property
    def is_configured(self) -> bool:
        """Check if Exa is configured."""
        return bool(self.api_key)


def _clean_env_value(value: str) -> str:
    """Clean environment variable value by removing inline comments."""
    # Split on '#' and take the first part, then strip whitespace
    return value.split('#')[0].strip()

class BotConfig(BaseSettings):
    """Main bot configuration."""
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra='allow')
    
    # API configurations
    kalshi: KalshiConfig = Field(..., description="Kalshi configuration")
    octagon: OctagonConfig = Field(..., description="Octagon configuration")
    openai: OpenAIConfig = Field(..., description="OpenAI configuration")
    webflow: WebflowConfig = Field(default_factory=WebflowConfig, description="Webflow configuration")
    gemini: GeminiConfig = Field(default_factory=GeminiConfig, description="Gemini configuration")
    exa: ExaConfig = Field(default_factory=ExaConfig, description="Exa configuration")
    
    # Bot settings
    dry_run: bool = Field(default=True, description="Run in dry-run mode (overridden by CLI)")
    max_bet_amount: float = Field(default=100.0, description="Maximum bet amount per market")
    max_events_to_analyze: int = Field(default=50, description="Number of top events to analyze by volume_24h")
    research_batch_size: int = Field(default=10, description="Number of parallel deep research requests to batch")
    research_timeout_seconds: int = Field(default=900, description="Per-event research timeout in seconds")
    skip_existing_positions: bool = Field(default=True, description="Skip betting on markets where we already have positions")
    minimum_time_remaining_hours: float = Field(default=1.0, description="Minimum hours remaining before event strike to consider it tradeable (only applied to events with strike_date)")
    max_markets_per_event: int = Field(default=10, description="Maximum number of markets per event to analyze (selects top N markets by volume)")
    # Legacy alpha threshold (deprecated - use R-score filtering instead)
    minimum_alpha_threshold: float = Field(default=2.0, description="DEPRECATED: Use z_threshold and enable_r_score_filtering instead")
    
    # Risk-adjusted trading parameters (now the default system)
    z_threshold: float = Field(default=1.5, description="Minimum R-score (z-score) threshold for placing bets")
    enable_r_score_filtering: bool = Field(default=True, description="DEPRECATED: R-score filtering is now always enabled")
    
    # Kelly criterion and position sizing
    enable_kelly_sizing: bool = Field(default=True, description="Use Kelly criterion for position sizing")
    kelly_fraction: float = Field(default=0.5, ge=0.1, le=1.0, description="Fraction of Kelly to use (0.5 = half-Kelly)")
    max_kelly_bet_fraction: float = Field(default=0.1, ge=0.01, le=0.5, description="Maximum fraction of bankroll per bet")
    bankroll: float = Field(default=1000.0, description="Total bankroll for Kelly sizing calculations")
    
    # Portfolio management
    max_portfolio_positions: int = Field(default=10, description="Maximum number of positions to hold simultaneously")
    portfolio_selection_method: str = Field(default="top_r_scores", description="Method for portfolio selection: 'top_r_scores', 'diversified', or 'legacy'")
    
    # Hedging settings
    enable_hedging: bool = Field(default=True, description="Enable hedging to minimize risk")
    hedge_ratio: float = Field(default=0.25, ge=0, le=0.5, description="Default hedge ratio (0.25 = hedge 25% of main bet)")
    min_confidence_for_hedging: float = Field(default=0.6, ge=0, le=1, description="Only hedge bets with confidence below this threshold")
    max_hedge_amount: float = Field(default=50.0, description="Maximum hedge amount per bet")
    
    def __init__(self, **data):
        # Build nested configs from environment variables
        
        # Handle private key from file if specified
        private_key = os.getenv("KALSHI_PRIVATE_KEY", "")
        private_key_file = os.getenv("KALSHI_PRIVATE_KEY_FILE", "")
        
        if private_key_file and not private_key:
            private_key = private_key_file  # Will be processed by validator
        
        kalshi_config = KalshiConfig(
            api_key=os.getenv("KALSHI_API_KEY", ""),
            private_key=private_key,
            use_demo=os.getenv("KALSHI_USE_DEMO", "true").lower() == "true"
        )
        
        octagon_config = OctagonConfig(
            api_key=os.getenv("OCTAGON_API_KEY", ""),
            base_url=os.getenv("OCTAGON_BASE_URL", "https://api.octagon.ai")
        )
        
        openai_config = OpenAIConfig(
            api_key=os.getenv("OPENAI_API_KEY", ""),
            model=os.getenv("OPENAI_MODEL", "gpt-5")
        )
        
        webflow_config = WebflowConfig(
            api_token=os.getenv("WEBFLOW_API_TOKEN", ""),
            site_id=os.getenv("WEBFLOW_SITE_ID", ""),
            markets_collection_id=os.getenv("WEBFLOW_MARKETS_COLLECTION_ID", ""),
            events_collection_id=os.getenv("WEBFLOW_EVENTS_COLLECTION_ID", "")
        )
        
        gemini_config = GeminiConfig(
            api_key=os.getenv("GEMINI_API_KEY", ""),
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            pro_model=os.getenv("GEMINI_PRO_MODEL", "gemini-2.5-pro")
        )
        
        exa_config = ExaConfig(
            api_key=os.getenv("EXA_API_KEY", "")
        )
        
        data.update({
            "kalshi": kalshi_config,
            "octagon": octagon_config,
            "openai": openai_config,
            "webflow": webflow_config,
            "gemini": gemini_config,
            "exa": exa_config,
            "dry_run": True,  # Default to dry run, overridden by CLI
            "max_bet_amount": float(_clean_env_value(os.getenv("MAX_BET_AMOUNT", "100.0"))),
            "max_events_to_analyze": int(_clean_env_value(os.getenv("MAX_EVENTS_TO_ANALYZE", "50"))),
            "research_batch_size": int(_clean_env_value(os.getenv("RESEARCH_BATCH_SIZE", "10"))),
            "research_timeout_seconds": int(_clean_env_value(os.getenv("RESEARCH_TIMEOUT_SECONDS", "900"))),
            "skip_existing_positions": _clean_env_value(os.getenv("SKIP_EXISTING_POSITIONS", "true")).lower() == "true",
            "minimum_time_remaining_hours": float(_clean_env_value(os.getenv("MINIMUM_TIME_REMAINING_HOURS", "1.0"))),
            "max_markets_per_event": int(_clean_env_value(os.getenv("MAX_MARKETS_PER_EVENT", "10"))),
            "minimum_alpha_threshold": float(_clean_env_value(os.getenv("MINIMUM_ALPHA_THRESHOLD", "2.0"))),
            # Risk-adjusted trading parameters
            "z_threshold": float(_clean_env_value(os.getenv("Z_THRESHOLD", "1.5"))),
            "enable_r_score_filtering": _clean_env_value(os.getenv("ENABLE_R_SCORE_FILTERING", "true")).lower() == "true",
            # Kelly criterion and position sizing
            "enable_kelly_sizing": _clean_env_value(os.getenv("ENABLE_KELLY_SIZING", "true")).lower() == "true",
            "kelly_fraction": float(_clean_env_value(os.getenv("KELLY_FRACTION", "0.5"))),
            "max_kelly_bet_fraction": float(_clean_env_value(os.getenv("MAX_KELLY_BET_FRACTION", "0.1"))),
            "bankroll": float(_clean_env_value(os.getenv("BANKROLL", "1000.0"))),
            # Portfolio management
            "max_portfolio_positions": int(_clean_env_value(os.getenv("MAX_PORTFOLIO_POSITIONS", "10"))),
            "portfolio_selection_method": os.getenv("PORTFOLIO_SELECTION_METHOD", "top_r_scores"),
            # Hedging settings
            "enable_hedging": _clean_env_value(os.getenv("ENABLE_HEDGING", "true")).lower() == "true",
            "hedge_ratio": float(_clean_env_value(os.getenv("HEDGE_RATIO", "0.25"))),
            "min_confidence_for_hedging": float(_clean_env_value(os.getenv("MIN_CONFIDENCE_FOR_HEDGING", "0.6"))),
            "max_hedge_amount": float(_clean_env_value(os.getenv("MAX_HEDGE_AMOUNT", "50.0")))
        })
        
        super().__init__(**data)

def load_config() -> BotConfig:
    """Load and validate configuration."""
    return BotConfig() 