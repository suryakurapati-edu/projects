import logging
import os
from datetime import datetime
import sys

def get_logger():
    job_name = sys.argv[1]
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(LOG_DIR, f"etl_{job_name}_{timestamp}.log")

    logger = logging.getLogger(f"ETLLogger_{job_name}")
    if not logger.hasHandlers():
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger