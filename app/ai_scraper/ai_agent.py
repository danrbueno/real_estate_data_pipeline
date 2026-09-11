"""AI Agent for scraping real estate data using OpenAI"""

import json
import re
import datetime
from typing import Optional, Dict, Any

from openai import OpenAI
from config import OPENAI_API_KEY, OPENAI_MODEL


class AIScrapingAgent:
    """AI Agent that uses OpenAI to extract real estate data from HTML"""

    def __init__(self, model: str = OPENAI_MODEL):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = model

    def close(self):
        """Keep the agent lifecycle compatible with the downloader."""

    @staticmethod
    def _parse_json_content(content: str) -> Dict[str, Any]:
        """Parse JSON, unwrapping markdown code fences or surrounding text if needed."""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            if "```json" in content:
                json_start = content.find("```json") + 7
                json_end = content.find("```", json_start)
                if json_end > json_start:
                    json_str = content[json_start:json_end].strip()
                    return json.loads(json_str)

            if "{" in content and "}" in content:
                json_start = content.find("{")
                json_end = content.rfind("}") + 1
                if json_end > json_start:
                    json_str = content[json_start:json_end]
                    return json.loads(json_str)

            return {"raw_response": content}

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
            return self._parse_json_content(content)

        except Exception as e:
            print(f"Error calling OpenAI: {e}")
            return {"error": str(e)}

    @staticmethod
    def _clean_html_for_ai(html: str, max_chars: int = 30000) -> str:
        """Clean HTML to preserve property links and page structure while staying within token limits."""
        if not html:  # pragma: no cover
            return ""
        # 1. Remove non-content blocks
        cleaned = re.sub(r'<(head|script|style|svg|footer|header|noscript)\b[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)
        cleaned = re.sub(r'<img\b[^>]*>', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'data:image/[^;]+;base64,[^"\'\s>]+', '', cleaned)

        # 2. Strip all attributes EXCEPT href from all tags
        def clean_tag_attrs(match):
            tag_content = match.group(0)
            tag_name = match.group(1)
            href_match = re.search(r'\bhref=(?:\"[^\"]*\"|\'[^\']*\'|[^\s>]+)', tag_content, re.IGNORECASE)
            href_str = f' {href_match.group(0)}' if href_match else ''
            if tag_name.startswith('/'):
                return f'</{tag_name[1:]}>'
            return f'<{tag_name}{href_str}>'

        cleaned = re.sub(r'<(/?[a-zA-Z0-9]+)(?:\s+[^>]*>|>)', clean_tag_attrs, cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned[:max_chars].strip()

    def extract_property_details(self, html: str, property_url: str) -> Dict[str, Any]:
        """
        Extract detailed information from a property page
        
        Args:
            html: HTML content of the property page
            property_url: URL of the property
            
        Returns:
            Dict with property details
        """
        cleaned_html = self._clean_html_for_ai(html, max_chars=30000)
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
{cleaned_html}
"""
        
        result = self._call_openai(prompt)
        return result

    def extract_property_page_details(self, html: str, property_url: str) -> Dict[str, Any]:
        """
        Extract listing data from a property's own detail page

        Args:
            html: HTML content of the property detail page (the page behind the ad's link)
            property_url: URL of the property page

        Returns:
            Dict with the property fields extracted from the page
        """
        cleaned_html = self._clean_html_for_ai(html, max_chars=30000)
        prompt = f"""
Extract real estate listing information from this property page. Return ONLY a JSON object with this exact format:
{{
    "link": "{property_url}",
    "title": "...",
    "price": "R$ 4.400",
    "useful_area": "35 m²",
    "price_per_sqm": "R$ 125",
    "bedrooms": "1",
    "suites": "1",
    "parking_spaces": "1",
    "condo_fee": "R$ 500",
    "total_area": "40 m²",
    "iptu": "R$ 100",
    "floor": "3º andar",
    "neighborhood": "Noroeste"
}}

Instructions:
- "price" is the sale or rental value shown on the page, whichever applies
- Look for values in dedicated fields/tables/icons, and also inside free-text descriptions
  (e.g. "3º andar", "Condomínio R$ ...", "IPTU R$ ...", "Vaga de garagem")
- Extract values exactly as shown in the HTML (keep currency symbols/units)
- If a field cannot be found, set its value to null
- Return only valid JSON, no other text

HTML:
{cleaned_html}
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

