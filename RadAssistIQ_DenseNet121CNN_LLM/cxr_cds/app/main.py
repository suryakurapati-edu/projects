from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from app.schemas import AnalyzeRequest, DiagnosisResponse
from app.utils import load_image_by_id, NotFoundError
from app.services.analyzer import AnalyzerService
from app.logger import logger

app = FastAPI(title="Chest X-ray CDS API", version="1.0.0")

# Create analyzer instance (vision + LLM orchestrator)
analyzer = AnalyzerService()


@app.post("/analyze-xray", response_model=DiagnosisResponse)
async def analyze_xray(req: AnalyzeRequest):
    """Endpoint to analyze X-ray and generate LLM reasoning."""
    xray_id = req.xray_id
    query = req.query
    try:
        image = load_image_by_id(xray_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    try:
        findings, reasoning = analyzer.analyze(image, query)
    except Exception as e:
        logger.exception("Analysis failed")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

    lines = [l.strip() for l in reasoning.splitlines() if l.strip()]
    summary = lines[0] if lines else "No summary available"
    response = {
        "xray_id": xray_id,
        "findings": findings,
        "diagnosis_summary": summary,
        "likely_diagnoses": [],
        "reasoning": reasoning,
        "meta": {"query": query}
    }
    return JSONResponse(status_code=200, content=response)
