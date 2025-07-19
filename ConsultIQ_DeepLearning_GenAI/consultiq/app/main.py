from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

from app.models.audio_processor import AudioProcessor
from app.models.text_processor import TextProcessor
from app.models.embedder import Embedder
from app.models.retriever import Retriever
from app.models.llm_client import LLMClient
from app.utils.helpers import get_audio_path, get_transcript_path
from app.config import INDEX_PATH
import asyncio

app = FastAPI()

class UserQuery(BaseModel):
    patient_id: str
    query: str

audio_processor = AudioProcessor()
text_processor = TextProcessor()
embedder = Embedder()
retriever = Retriever(index_path=INDEX_PATH)
llm_client = LLMClient()

@app.post("/query")
async def query_patient(user_query: UserQuery):
    audio_path = get_audio_path(user_query.patient_id)
    transcript_path = get_transcript_path(user_query.patient_id)

    if not os.path.exists(audio_path):
        raise HTTPException(status_code=404, detail="Patient audio file not found.")
    
    # Load transcript if already exists
    if os.path.exists(transcript_path):
        with open(transcript_path, "r") as f:
            transcription = f.read()
    else:
        transcription = audio_processor.transcribe(audio_path)
        with open(transcript_path, "w") as f:
            f.write(transcription)

    chunks = text_processor.split_text(transcription)
    retriever.load_index()
    if retriever.index is None:
        chunk_embeddings = embedder.embed_text(chunks)
        retriever.build_index(chunk_embeddings, chunks)
        print("Built FAISS index from scratch.")
    else:
        print("Loaded existing FAISS index.")
    
    query_embedding = embedder.embed_text([user_query.query])[0]
    relevant_chunks = retriever.search(query_embedding, top_k=1)

    response = await llm_client.generate_response(relevant_chunks[0], user_query.query)
    return {"response": response}
