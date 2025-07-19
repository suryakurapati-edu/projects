import os
from pymongo import MongoClient
import google.generativeai as genai
import asyncio
import time


class LLMClient:
    def __init__(self, model_name="gemini-2.0-flash"):
        self.model_name = model_name
        self.api_key = self._load_api_key_from_mongo()
        if not self.api_key:
            raise ValueError("Gemini API key not found in MongoDB.")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model_name=self.model_name)

    def _load_api_key_from_mongo(self):
        connection_string = "mongodb://localhost:27017/"
        database = "db_genai"
        collection = "col_llm_conf"
        query = {"_id": "gemini"}

        try:
            client = MongoClient(connection_string)
            db = client[database]
            col = db[collection]
            doc = col.find_one(query)
            return doc.get("api_key") if doc else None
        except Exception as e:
            print(f"[MongoDB Error] {e}")
            return None

    async def generate_response(self, context: str, query: str) -> str:
        prompt = f"""
You are a professional medical assistant. Based only on the following context, provide a helpful and accurate answer.

Context:
{context}

User Query:
{query}

Answer:"""

        try:
            start = time.time()
            response = await asyncio.to_thread(self.model.generate_content, prompt)
            print("LLM took", time.time() - start, "seconds")
            return response.text.strip()
        except Exception as e:
            return f"[LLM Error] Failed to generate response: {str(e)}"
