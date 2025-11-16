import torch
from typing import Dict, Tuple
from app.models.vision import RealVisionModel
from app.models.llm_client import OpenAIClient, LLMConfigManager
from app.config import FINDING_THRESHOLD
from app.logger import logger


class AnalyzerService:
    """Main orchestrator combining vision + LLM reasoning."""

    def __init__(self):
        logger.info(f"Initializing analyzer")

        # Vision Model Initialization
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using RealVisionModel on device: {device}")
        self.vision_model = RealVisionModel(device=device)

        # LLM Configuration via MongoDB
        try:
            llm_config = LLMConfigManager(llm_name="openai")  # or "gemini"
            logger.info(f"Loaded LLM configuration from MongoDB for model: {llm_config.llm_model}")
            self.llm_client = OpenAIClient(model=llm_config.llm_model, api_key=llm_config.api_key)
        except Exception as e:
            logger.exception("Failed to initialize OpenAI client from MongoDB config.")
            raise e

    # Internal Helper
    def _filter_findings(self, findings: Dict[str, float]) -> Dict[str, float]:
        """Filter findings by threshold."""
        filtered = {k: v for k, v in findings.items() if v >= FINDING_THRESHOLD}
        return filtered

    # Main Analyzer Flow
    def analyze(self, image, query: str) -> Tuple[Dict[str, float], str]:
        """Run vision model → reasoning pipeline."""
        logger.info("Running vision model prediction.")
        raw = self.vision_model.predict(image)
        findings = self._filter_findings(raw)

        logger.info("Generating reasoning via LLM client.")
        reasoning = self.llm_client.generate_reasoning(findings, query)

        return findings, reasoning
