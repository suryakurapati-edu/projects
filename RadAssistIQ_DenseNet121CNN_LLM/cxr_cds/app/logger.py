from loguru import logger
import os

os.makedirs("logs", exist_ok=True)
logger.add("logs/app.log", rotation="10 MB", level="INFO", backtrace=True, diagnose=True)
