from pymongo import MongoClient
import pandas as pd
from code.logger_config import get_logger

logger = get_logger()

def connect_mongo(mongo_conf):
    """
    Connect to MongoDB using a config dict containing `uri`, `database`, and `collection`.
    """
    uri = mongo_conf["uri"]
    client = MongoClient(uri)
    return client

def read_from_mongo(client, mongo_conf):
    """
    Read data from MongoDB into a Pandas DataFrame.
    """
    db_name = mongo_conf["database"]
    collection_name = mongo_conf["collection"]
    collection = client[db_name][collection_name]
    data = list(collection.find({}, {"_id": 0}))  # Exclude Mongo's internal _id
    return pd.DataFrame(data)

def truncate_collection(client, mongo_conf):
    """
    Delete all records from the specified MongoDB collection.
    """
    db_name = mongo_conf["database"]
    collection_name = mongo_conf["collection"]
    client[db_name][collection_name].delete_many({})

def insert_to_mongo(client, mongo_conf, df):
    """
    Insert a Pandas DataFrame into MongoDB collection.
    """
    db_name = mongo_conf["database"]
    collection_name = mongo_conf["collection"]
    records = df.to_dict(orient='records')
    client[db_name][collection_name].insert_many(records)

def load_json_to_mongo_with_schema(config):
    """
    Load data from JSON file into MongoDB with schema validation, deduplication, and null handling.
    """
    file_path = config["input_file"]
    schema_path = config["schema_path"]
    mongo_conf = config["mongodb"]
    unique_keys = config.get("unique_keys", [])
    surrogate_key = config.get("surrogate_key")
    nulls_output_path = config.get("null_output_file", "data/nulls.csv")

    # Load schema
    schema_df = pd.read_csv(schema_path)
    col_types = {row["column_name"]: row["data_type"] for _, row in schema_df.iterrows()}

    # Load JSON data
    df = pd.read_json(file_path, dtype={k: v for k, v in col_types.items() if v != 'datetime'})
    
    # Convert datetime columns
    for col, dtype in col_types.items():
        if dtype == "datetime":
            df[col] = pd.to_datetime(df[col], format="%d/%m/%y", errors='coerce')

    logger.info(f"Read {len(df)} rows from JSON file: {file_path}")

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Null check on unique keys
    if unique_keys:
        null_df = df[df[unique_keys].isnull().any(axis=1)]
        if not null_df.empty:
            null_df.to_csv(nulls_output_path, index=False)
            logger.warning(f"Found nulls in unique keys, {len(null_df)} rows written to {nulls_output_path}")
        df = df.dropna(subset=unique_keys)

    # Truncate and insert into MongoDB
    client = connect_mongo(mongo_conf)
    truncate_collection(client, mongo_conf)
    insert_to_mongo(client, mongo_conf, df)

    logger.info(f"Inserted {len(df)} clean records into MongoDB collection: {mongo_conf['collection']}")