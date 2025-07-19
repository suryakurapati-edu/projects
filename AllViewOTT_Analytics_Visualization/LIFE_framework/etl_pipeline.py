# Run Command: python etl_pipeline.py "customers_stg"
import sys
import os
import json
from code.loaders import StageLoader, ProcessedLoader
from code.logger_config import get_logger

# logger = None
logger = get_logger()

def main(config_key):
    # global logger
    try:
        config_path = os.path.join("config", f"{config_key}.conf")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path, 'r') as f:
            conf = json.load(f)
            identifier = conf.get("run_layer")
        
        # logger = get_logger(config_key)

        if identifier == "stage":
            loader = StageLoader(config_path)
        elif identifier == "processed":
            loader = ProcessedLoader(config_path)
        else:
            raise ValueError("Invalid identifier in config file.  Expected either 'stage' or 'processed'.")

        loader.run_pipeline()

    except Exception as e:
        error_msg = f"ETL Pipeline execution for {config_key} failed: {e}"
        logger.error(error_msg)
        print(error_msg)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing config Argument")
        sys.exit(1)

    config_key = sys.argv[1]
    main(config_key)