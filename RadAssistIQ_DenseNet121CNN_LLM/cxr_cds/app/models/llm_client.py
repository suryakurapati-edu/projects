import os
from openai import OpenAI
from app.db.mongo_client import MongoDBClient
from app.config import OPENAI_API_KEY
from app.logger import logger
from typing import Dict


class LLMConfigManager:
    """Fetches and sets LLM configuration (API keys, model) from MongoDB."""

    def __init__(self, llm_name: str, mongo_conn: str = "mongodb://localhost:27017/", db_name: str = "db_genai"):
        self.llm_name = llm_name.lower()
        self.mongo = MongoDBClient(connection_string=mongo_conn, database=db_name)
        self.collection = "col_llm_conf"
        self.llm_model = None
        self.api_key = None
        self._load_config()

    def _load_config(self):
        """Fetch configuration from MongoDB and set environment variables."""
        try:
            config_doc = self.mongo.fetch_one(self.collection, {"_id": self.llm_name})
            if not config_doc:
                raise ValueError(f"No config found in MongoDB for LLM '{self.llm_name}'")

            self.api_key = config_doc["api_key"]
            self.llm_model = config_doc.get("model", "gpt-4o-mini")

            if self.llm_name == "openai":
                os.environ["OPENAI_API_KEY"] = self.api_key
            elif self.llm_name == "gemini":
                os.environ["GOOGLE_API_KEY"] = self.api_key

            logger.info(f"Loaded LLM config for '{self.llm_name}' successfully.")
        except Exception as e:
            logger.exception("Failed to load LLM configuration from MongoDB.")
            raise e


class OpenAIClient:
    """Handles interaction with OpenAI API."""

    def __init__(self, model="gpt-4o-mini", api_key=None):
        self.client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
        self.model = model

    def generate_reasoning(self, findings: dict, query: str) -> str:
        """Generate reasoning using OpenAI LLM."""
        top_findings = findings if findings else "None detected"
        prompt = f"""
        You are an expert clinical radiology assistant. You are only allowed to answer
        questions directly related to interpreting the provided chest X-ray findings.
        If the question is unrelated (e.g., asking about patient identity), respond:
        "I'm sorry, I can only answer questions related to interpreting this X-ray."

        Findings:
        {top_findings}

        User question: "{query}"

        Please respond concisely in three sections:
        1. Findings summary
        2. Likely differential diagnoses (ranked)
        3. Reasoning
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful medical assistant."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.exception("Error calling OpenAI API.")
            return f"Error generating response: {str(e)}"
