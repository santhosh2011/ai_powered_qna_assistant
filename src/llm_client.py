"""
LLM client: Simple interface for OpenAI API calls.
"""

import os
from typing import List, Dict, Optional
from openai import OpenAI, OpenAIError
from dotenv import load_dotenv

load_dotenv()


class LLMClient:
    """OpenAI chat completion client with sensible defaults."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini"
    ):
        """Initialize LLM client. API key from parameter or OPENAI_API_KEY env var."""
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "OpenAI API key not provided. Either pass it to LLMClient(api_key=...) "
                "or set the OPENAI_API_KEY environment variable."
            )
        
        self.model = model
        self.client = OpenAI(api_key=self.api_key)
    
    def chat(self, messages: List[Dict], **kwargs) -> str:
        """Send messages to OpenAI chat API and return response (default temperature=0.1)."""
        if not messages or not isinstance(messages, list):
            raise ValueError("Messages must be a non-empty list")
        
        if "temperature" not in kwargs:
            kwargs["temperature"] = 0.1
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            
            content = response.choices[0].message.content
            
            if not content:
                raise ValueError("OpenAI API returned empty content")
            
            return content
            
        except OpenAIError as e:
            raise OpenAIError(f"OpenAI API call failed: {str(e)}") from e

