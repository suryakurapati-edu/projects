import os
from app.config import DATA_DIR, TRANSCRIPT_DIR

def get_audio_path(patient_id):
    return os.path.join(DATA_DIR, f"{patient_id}.wav")

def get_transcript_path(patient_id):
    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    return os.path.join(TRANSCRIPT_DIR, f"{patient_id}.txt")