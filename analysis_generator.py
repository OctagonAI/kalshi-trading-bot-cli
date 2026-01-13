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
    label: str = Field(description="Name of the metric or data point")
    value: str = Field(description="Value with source attribution")


class QuestionResearch(BaseModel):
    """Research findings for a single question."""
    subtitle: str = Field(description="A concise 5-10 word title for this research section")
    table_data: List[TableDataItem] = Field(description="3 key data points with sources")
    paragraph: str = Field(description="2-3 paragraph analysis with citations")


class CatalystResearch(BaseModel):
    """Key catalysts that could change market probability."""
    subtitle: str = Field(description="Section title, e.g., 'Key Catalysts'")
    paragraph: str = Field(description="2-3 paragraphs on bullish/bearish catalysts with dates")


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


def format_table_html(table_data: List[Dict[str, str]]) -> str:
    """Format table data as an HTML table.
    
    Args:
        table_data: List of {label, value} dicts
        
    Returns:
        HTML table string
    """
    if not table_data:
        return ""
    
    rows = []
    for item in table_data:
        label = item.get('label', '')
        value = item.get('value', '')
        if label or value:
            rows.append(f'<tr><td><strong>{label}</strong></td><td>{value}</td></tr>')
    
    if not rows:
        return ""
    
    return f'<table><tbody>{"".join(rows)}</tbody></table>'


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
    
    async def _gemini_generate_structured(
        self,
        prompt: str,
        response_schema: type[BaseModel],
        use_search_grounding: bool = False
    ) -> Optional[BaseModel]:
        """Generate structured content using Gemini with JSON schema.
        
        Note: Gemini 2.5 does NOT support combining tools (grounding) with 
        structured JSON output. When grounding is needed, we use a two-step
        approach: first get grounded info, then format with structured output.
        
        Args:
            prompt: The prompt to send to Gemini
            response_schema: Pydantic model class defining the expected output
            use_search_grounding: Whether to use Google Search for grounding
            
        Returns:
            Parsed Pydantic model instance, or None if generation fails
        """
        client = self._init_gemini()
        if not client:
            logger.warning("Gemini not configured, skipping generation")
            return None
        
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
                
                if not grounded_text:
                    return None
                
                # Step 2: Format into structured output
                schema_json = response_schema.model_json_schema()
                format_prompt = f"""Based on this research information, extract and format the data.

Research findings:
{grounded_text}

Format the above information according to this JSON schema:
{json.dumps(schema_json, indent=2)}

Return ONLY valid JSON matching the schema."""

                structured_response = client.models.generate_content(
                    model=self.gemini_config.model,
                    contents=format_prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_json_schema=schema_json,
                    )
                )
                
                return response_schema.model_validate_json(structured_response.text)
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
                
                return response_schema.model_validate_json(response.text)
            
        except Exception as e:
            logger.error(f"Error generating structured content with Gemini: {e}")
            return None
    
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
        
        # Fallback: Use Google Search to find discussions about this topic
        logger.info(f"No Kalshi discussion found, searching web for: {event_title}")
        
        search_prompt = f"""Search for recent discussions, opinions, and analysis about: "{event_title}"

Look for:
- Social media discussions (Twitter/X, Reddit, forums)
- News commentary and expert opinions
- Prediction market discussions from other platforms

Summarize in 2-3 sentences the main viewpoints and arguments being made about this topic.
If this is a niche topic with limited discussion, note what experts or analysts are saying about it."""

        result = await self._gemini_generate(search_prompt, use_search_grounding=True)
        
        if result:
            return result
        
        return "Limited public discussion available for this market."
    
    async def generate_research_questions(
        self,
        event_title: str,
        event_subtitle: str,
        series_category: str,
        market_probability: float
    ) -> Dict[str, str]:
        """Generate 5 research questions using Gemini with Google Search grounding.
        
        Uses Gemini's structured output feature to guarantee valid JSON.
        
        Args:
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            series_category: Category of the series
            market_probability: Current market probability (0-100)
            
        Returns:
            Dict with keys q1-q5 containing the research questions
        """
        prompt = f"""You are generating research questions for a prediction market analysis page.

Market: "{event_title}"
Subtitle: {event_subtitle}
Category: {series_category}
Current market odds: {market_probability:.1f}% YES

Search Google to understand what questions and topics people are currently searching for related to this market topic. Look at related searches, news headlines, and common angles being covered.

Generate exactly 5 questions that align with real search demand - questions people are actively searching for online.

Requirements:
- Use natural, conversational phrasing (how people actually search)
- Include specific names, dates, or terms from the market title
- Each question should be standalone and searchable

Guidelines for each question:
- q1: Direct question about the outcome (e.g., "Will X happen?")
- q2: Question about recent news or developments
- q3: Question about expert predictions or forecasts
- q4: Question about key data, statistics, or indicators
- q5: Question about timeline or upcoming events"""

        result = await self._gemini_generate_structured(
            prompt, ResearchQuestions, use_search_grounding=True
        )
        
        if result:
            return {
                "q1": result.q1,
                "q2": result.q2,
                "q3": result.q3,
                "q4": result.q4,
                "q5": result.q5,
            }
        
        # Fallback if structured output fails
        logger.warning("Structured output failed for questions, using defaults")
        return {
            "q1": f"Will {event_title}?",
            "q2": f"What is the latest news about {event_title}?",
            "q3": f"What do experts predict about {event_title}?",
            "q4": f"What data supports predictions about {event_title}?",
            "q5": f"When will {event_title} be decided?"
        }
    
    async def research_question(
        self,
        question: str,
        event_title: str,
        event_subtitle: str
    ) -> Dict[str, Any]:
        """Research a single question using Gemini with Google Search grounding.
        
        Uses Gemini's structured output feature to guarantee valid JSON.
        
        Args:
            question: The research question to answer
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            
        Returns:
            Dict with 'subtitle', 'table_data', and 'paragraph' keys
        """
        prompt = f"""Research question: {question}

Context: This research is for the prediction market "{event_title}" which resolves {event_subtitle}.

Search for current, authoritative information to answer this question thoroughly.

Provide:
- A concise 5-10 word subtitle for this research section
- Exactly 3 key data points with their sources
- A 2-3 paragraph analysis with inline citations [Source Name] explaining what the data means for the prediction market outcome"""

        result = await self._gemini_generate_structured(
            prompt, QuestionResearch, use_search_grounding=True
        )
        
        if result:
            return {
                "subtitle": result.subtitle,
                "table_data": [{"label": item.label, "value": item.value} for item in result.table_data],
                "paragraph": result.paragraph
            }
        
        # Fallback: try unstructured generation
        logger.warning("Structured output failed for question research, using fallback")
        response = await self._gemini_generate(prompt, use_search_grounding=True)
        
        return {
            "subtitle": question[:50],
            "table_data": [],
            "paragraph": clean_markdown_response(response) if response else "Research data not available."
        }
    
    async def research_what_could_change(
        self,
        event_title: str,
        market_probability: float,
        close_time: str
    ) -> Dict[str, str]:
        """Research key catalysts that could change the market.
        
        Uses Gemini's structured output feature to guarantee valid JSON.
        
        Args:
            event_title: Title of the event
            market_probability: Current market probability (0-100)
            close_time: Settlement/close time
            
        Returns:
            Dict with 'subtitle' and 'paragraph' keys
        """
        prompt = f"""Prediction market: "{event_title}"
Current market probability: {market_probability:.1f}% YES
Settlement date: {close_time}

Research and identify the key catalysts or events that could significantly change the probability of this market.

Include in your paragraph:
- Bullish catalysts (could push YES higher) with specific events and dates
- Bearish catalysts (could push NO higher) with specific events and dates
- Timeline of key dates to watch before settlement
- Cite sources for any scheduled events or announcements"""

        result = await self._gemini_generate_structured(
            prompt, CatalystResearch, use_search_grounding=True
        )
        
        if result:
            return {
                "subtitle": result.subtitle,
                "paragraph": result.paragraph
            }
        
        # Fallback
        logger.warning("Structured output failed for catalysts, using fallback")
        response = await self._gemini_generate(prompt, use_search_grounding=True)
        
        return {
            "subtitle": "Key Catalysts",
            "paragraph": clean_markdown_response(response) if response else "Catalyst analysis not available."
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
        market_probability: float
    ) -> str:
        """Generate a one-line key takeaway from all research.
        
        Args:
            event_title: Title of the event
            event_subtitle: Subtitle of the event
            octagon_text: Full Octagon research text
            q_summaries: List of paragraph summaries from Q1-Q5
            model_probability: Model's probability prediction
            market_probability: Current market probability
            
        Returns:
            One-line key takeaway (max 15 words)
        """
        q_text = "\n".join([f"Q{i+1}: {s[:200]}" for i, s in enumerate(q_summaries) if s])
        
        prompt = f"""You are a research analyst summarizing findings for a prediction market.

## Market: "{event_title}"
{event_subtitle}

## Octagon Deep Research Analysis:
{octagon_text[:3000]}

## Grounded Research Findings:
{q_text}

## Market Data:
- Model probability (Octagon): {model_probability:.1f}%
- Current market probability: {market_probability:.1f}%

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
        market_probability: float
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
            
        Returns:
            2-3 paragraph executive summary
        """
        q_text = "\n".join([
            f"{i+1}. {f.get('subtitle', '')}: {f.get('paragraph', '')[:150]}..."
            for i, f in enumerate(q_findings) if f
        ])
        
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

## Market Data:
- Model probability (Octagon): {model_probability:.1f}%
- Current market probability: {market_probability:.1f}%

Write a 2-3 paragraph research summary that:

**Paragraph 1: Background**
- Explain what this market is about and why it matters
- Provide context on the current situation
- State what the market is predicting

**Paragraph 2: Key Findings**
- Synthesize the most important findings from the research
- Highlight relevant data points and evidence
- Note any conflicting information or uncertainty

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
    
    async def generate_analysis(
        self,
        event: Dict[str, Any],
        markets: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate complete analysis for an event.
        
        This is the main orchestration method that:
        1. Runs parallel API calls (Exa crawl, Octagon research, question generation)
        2. Researches each question with Gemini + Google Search
        3. Synthesizes executive summary and key takeaway
        
        Args:
            event: Event data dictionary with title, subtitle, ticker, etc.
            markets: List of market data dictionaries for this event
            
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
        
        # Initialize analysis result with metadata
        analysis = {
            "analysis_last_updated": datetime.now().isoformat(),
            "analysis_version": "1.0",
            "analysis_owner": "AI-Generated",
            "kalshi_event_url": kalshi_url,
            "market_probability": f"{market_probability:.1f}",
        }
        
        # Phase 1: Parallel API calls
        logger.info(f"Phase 1: Running parallel API calls for {event_ticker}")
        
        # Run Exa crawl, Octagon research, and question generation in parallel
        exa_task = self.crawl_kalshi_page(series_ticker, series_title, event_ticker)
        octagon_task = self.run_octagon_research(event, markets)
        questions_task = self.generate_research_questions(
            event_title, event_subtitle, series_category, market_probability
        )
        
        exa_result, octagon_result, questions = await asyncio.gather(
            exa_task, octagon_task, questions_task,
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
            questions = {"q1": "", "q2": "", "q3": "", "q4": "", "q5": ""}
        
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
            q_paragraphs, model_probability, market_probability
        )
        
        executive_summary = await self.generate_executive_summary(
            event_title, event_subtitle, series_category,
            octagon_text, q_findings,
            what_could_change.get("paragraph", ""),
            model_probability, market_probability
        )
        
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
        analysis["executive_verdict"] = clean_markdown_response(key_takeaway)  # One-line summary/verdict
        analysis["executive_summary_richtext"] = clean_markdown_response(executive_summary)
        
        logger.info(f"Completed analysis for {event_ticker}")
        
        return analysis
    
    async def close(self):
        """Close any open clients."""
        if self._octagon_client:
            await self._octagon_client.close()
