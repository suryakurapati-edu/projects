import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
# MODEL_TYPE = os.getenv("MODEL_TYPE", "real")  
XRAWS_ROOT = os.getenv("XRAWS_ROOT", "./images")
FINDING_THRESHOLD = float(os.getenv("FINDING_THRESHOLD", 0.7))
