import os

DATA_DIR = os.path.join(os.getcwd(), 'data')
INDEX_PATH = os.path.join(os.getcwd(), 'vectorstore', 'faiss_index.bin')
TRANSCRIPT_DIR = os.path.join(DATA_DIR, 'transcripts')