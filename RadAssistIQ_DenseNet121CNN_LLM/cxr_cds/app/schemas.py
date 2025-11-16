from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class AnalyzeRequest(BaseModel):
    """Request body for analyzing a chest X-ray."""
    xray_id: str = Field(..., example="CXR_000123")
    query: str = Field(..., example="Diagnose the X-ray")


class DiagnosisResponse(BaseModel):
    """Structured API response."""
    xray_id: str
    findings: Dict[str, float]
    diagnosis_summary: str
    likely_diagnoses: List[str]
    reasoning: str
    meta: Optional[Dict] = {}
