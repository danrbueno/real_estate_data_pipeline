"""AI Agent for scraping real estate data using OpenAI"""

import json
from typing import Optional, List, Dict, Any
import datetime
from openai import OpenAI
from config import OPENAI_API_KEY, OPENAI_MODEL


class AIScrapingAgent:
    """AI Agent that uses OpenAI to extract real estate data from HTML"""

    def __init__(self, model: str = OPENAI_MODEL):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = model

    def _call_openai(self, prompt: str, response_format: Optional[dict] = None) -> Dict[str, Any]:
        """
        Call OpenAI API with the given prompt
        
        Args:
            prompt: The prompt to send
            response_format: Optional JSON schema for response format
            
        Returns:
            Parsed response from OpenAI
        """
        try:
            kwargs = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,  # Low temperature for consistent extraction
            }
            
            if response_format:
                kwargs["response_format"] = response_format

            response = self.client.chat.completions.create(**kwargs)
            content = response.choices[0].message.content
            
            # Try to parse as JSON
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                if "```json" in content:
                    json_start = content.find("```json") + 7
                    json_end = content.find("```", json_start)
                    if json_end > json_start:
                        json_str = content[json_start:json_end].strip()
                        return json.loads(json_str)
                
                # Try to extract JSON between curly braces
                if "{" in content and "}" in content:
                    json_start = content.find("{")
                    json_end = content.rfind("}") + 1
                    if json_end > json_start:
                        json_str = content[json_start:json_end]
                        return json.loads(json_str)
                
                # If all else fails, return raw response
                return {"raw_response": content}
                
        except Exception as e:
            print(f"Error calling OpenAI: {e}")
            return {"error": str(e)}

    def extract_property_links(self, html: str, base_url: str) -> List[str]:
        """
        Extract property links from listing page HTML
        
        Args:
            html: HTML content of the listing page
            base_url: Base URL to construct full URLs
            
        Returns:
            List of property URLs
        """
        prompt = f"""
Extract all property/apartment links from this HTML. Return ONLY a JSON object with this format:
{{
    "links": ["https://example.com/property1", "https://example.com/property2"]
}}

Important:
- Extract only apartment links (ignore other property types)
- Make full URLs (prepend {base_url} if relative)
- Remove duplicates
- Return only the JSON object, no other text

HTML:
{html[:5000]}...
"""
        
        result = self._call_openai(prompt)
        return result.get("links", [])

    def extract_pagination_info(self, html: str) -> Dict[str, Any]:
        """
        Extract pagination information from listing page
        
        Args:
            html: HTML content of the listing page
            
        Returns:
            Dict with pagination info (has_next_page, total_in_page, page_is_empty)
        """
        prompt = f"""
Analyze this HTML page and count how many property listings are shown. Return ONLY a JSON object:

{{
    "has_next_page": true,
    "total_in_page": 25,
    "page_is_empty": false
}}

Instructions:
1. COUNT PROPERTY LISTINGS carefully:
   - Look for repeated item containers, divs with property data
   - Count div elements that contain price, address, features
   - Each property has a "data-id" attribute - count how many unique data-id values exist
   - Common pattern: 25 items per page on most pages, but last page may have fewer (e.g., 15-22 items)
   - If you find less than 5 items, count very carefully and report the exact number

2. DETERMINE has_next_page:
   - Look for "next" or "próxima" buttons/links that are NOT disabled
   - Look for pagination controls/UI
   - If page appears empty or last, set to false
   - Otherwise set to true

3. Detect if page is empty:
   - Set to true only if there are genuinely NO property items (less than 3 items)
   - Set to false if there are any items, even if less than 25

Return ONLY valid JSON, no explanation.

HTML:
{html[:5000]}...
"""
        
        result = self._call_openai(prompt)
        return result

    def extract_property_details(self, html: str, property_url: str) -> Dict[str, Any]:
        """
        Extract detailed information from a property page
        
        Args:
            html: HTML content of the property page
            property_url: URL of the property
            
        Returns:
            Dict with property details
        """
        prompt = f"""
Extract detailed property information from this HTML. Return ONLY a JSON object with this format:
{{
    "title": "Apartamento 2 quartos",
    "price": "R$ 250.000",
    "bedrooms": "2",
    "bathrooms": "1",
    "area": "80 m²",
    "location": "Brasília - DF",
    "neighborhood": "Asa Sul",
    "description": "...",
    "amenities": ["piscina", "portaria"],
    "link": "{property_url}",
    "scraped_at": "{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "other_features": {{
        "key_name": "value"
    }}
}}

Instructions:
- Extract ALL visible information from the property page
- Convert all text to lowercase for keys (remove special chars, replace spaces with underscore)
- Keep values as they appear in the page
- Include any additional features found (from <h6> tags, tables, etc.)
- If a field is not found, omit it from the response
- Return only valid JSON, no extra text

HTML:
{html[:8000]}...
"""
        
        result = self._call_openai(prompt)
        return result

    def validate_extraction(self, data: Dict[str, Any]) -> bool:
        """
        Validate that extraction was successful
        
        Args:
            data: Extracted data
            
        Returns:
            True if data looks valid
        """
        # Check for error in response
        if "error" in data:
            return False
        
        # Check for required fields
        required_fields = ["title", "link"]
        return all(field in data for field in required_fields)
