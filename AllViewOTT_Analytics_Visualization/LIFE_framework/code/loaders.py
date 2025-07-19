import pandas as pd
import os
import json
import uuid
from datetime import datetime
import hashlib
from code.utils.db_utils import connect_postgres, run_query, truncate_table, insert_dataframe
from code.utils.mongo_utils import connect_mongo, read_from_mongo, load_json_to_mongo_with_schema
from code.logger_config import get_logger

logger = get_logger()

class BaseLoader:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.conn = connect_postgres(self.config["database"])
        self.schema_path = self.config["schema_path"]
        self.col_types = self.apply_custom_schema()
        self.df = pd.DataFrame()

    def load_config(self, path):
        with open(path, 'r') as f:
            return json.load(f)

    def apply_custom_schema(self):
        schema_df = pd.read_csv(self.schema_path)
        col_types = {row["column_name"]: row["data_type"] for _, row in schema_df.iterrows()}
        logger.info(f"Custom schema applied: {col_types}")
        return col_types

    def read_csv(self, file_path):
        logger.info(f"CSV file read with schema: {file_path}")
        non_date_columns = {k:v for k,v in self.col_types.items() if v!='datetime'}
        date_columns = {k:v for k,v in self.col_types.items() if v=='datetime'}
        df = pd.read_csv(file_path, dtype=non_date_columns, parse_dates=list(date_columns.keys()))
        df[[col for col in date_columns]] = df[[col for col in date_columns]].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d', errors='coerce"))
        return df
    
    def generate_surrogate_key(self, row, primary_keys):
        key_string = "_".join(str(row[pk]) for pk in primary_keys if pd.notna(row[pk]))
        return hashlib.md5(key_string.encode()).hexdigest() if key_string else str(uuid.uuid4())

class StageLoader(BaseLoader):
    def run_pipeline(self):
        source_type = self.config.get("source")
        file_path = self.config["input_file"]
        source_type = self.config["source_type"]
        unique_keys = self.config.get("unique_keys", [])
        surrogate_key = self.config.get("surrogate_key")
        nulls_output_path = self.config.get("null_output_file", "data/nulls.csv")
        db_schema = self.config.get("target_db_schema", "stage")
        table = self.config["target_table"]

        if source_type == "csv":
            self.df = self.read_csv(file_path)
        elif source_type == "mongo":
            logger.info(f"Reading JSON file: {file_path}")
            load_json_to_mongo_with_schema(self.config)
            logger.info("Reading data from MongoDB source.")
            client = connect_mongo(self.config["mongodb"])
            self.df = read_from_mongo(client, self.config["mongodb"])
            # Cast columns
            non_date_columns = {k:v for k,v in self.col_types.items() if v!='datetime'}
            date_columns = {k:v for k,v in self.col_types.items() if v=='datetime'}
            self.df = self.df.astype(non_date_columns)
            self.df[[col for col in date_columns]] = self.df[[col for col in date_columns]].apply(lambda x: pd.to_datetime(x, format="%d/%m/%y", errors='coerce'))

        else:
            raise ValueError(f"Unsupported file type: {source_type}")
        
        # Remove duplicates
        self.df.drop_duplicates(inplace=True)

        # Null check and write to a file if any
        if unique_keys:
            null_df = self.df[self.df[unique_keys].isnull().any(axis=1)]
            if not null_df.empty:
                null_df.to_csv(nulls_output_path, index=False)
                logger.info(f"Loaded {len(null_df)} null records of file {file_path} into file {nulls_output_path}")
            self.df = self.df.dropna(subset=unique_keys)
        
        # Add surrogate Key
        self.df[surrogate_key] = self.df.apply(lambda row: self.generate_surrogate_key(row, unique_keys), axis=1)

        # Add load timestamp for tracking
        self.df['load_timestamp'] = datetime.now()

        # Since stage layer is SCD type1, truncate the table before next step of insert
        truncate_table(self.conn, table, db_schema)

        # Insert data to postgres
        insert_dataframe(self.conn, self.df, table, db_schema)
        logger.info("Stage pipeline completed successfully.")

class ProcessedLoader(BaseLoader):
    def run_pipeline(self):
        db_schema = self.config.get("target_db_schema")
        table = self.config.get("target_table")
        query_path = self.config.get("sql_file")
        unique_keys = self.config.get("unique_keys", [])
        surrogate_key = self.config.get("surrogate_key")
        scd_type = self.config.get("scd_type")

        with open(query_path, 'r') as f:
            query = f.read()

        new_df = run_query(self.conn, query)

        timestamp = datetime.now()
        high_end_date = pd.Timestamp("9999-12-31")

        if scd_type == 1:
            # SCD Type 1 – Always overwrite with latest
            new_df['update_timestamp'] = timestamp
            new_df['effective_from'] = timestamp
            new_df['effective_to'] = high_end_date

            new_df[surrogate_key] = new_df.apply(lambda row: self.generate_surrogate_key(row, unique_keys), axis=1)

            truncate_table(self.conn, table, db_schema)
            insert_dataframe(self.conn, new_df, table, db_schema)

            logger.info(f"SCD Type 1 load completed for table {db_schema}.{table}")

        elif scd_type == 2:
            # SCD Type 2 – Track history of changes
            # 1. Read current target table
            target_query = f"SELECT * FROM project_analytics.{db_schema}.{table}"
            existing_df = run_query(self.conn, target_query)

            if existing_df.empty:
                # First run, treat all as new inserts
                new_df['update_timestamp'] = timestamp
                new_df['effective_from'] = timestamp
                new_df['effective_to'] = high_end_date
                new_df[surrogate_key] = new_df.apply(lambda row: self.generate_surrogate_key(row, unique_keys), axis=1)

                insert_dataframe(self.conn, new_df, table, db_schema)
                logger.info(f"First-time SCD Type 2 load completed for {db_schema}.{table}")
            else:
                # Identify changed records
                merge_keys = unique_keys.copy()
                compare_columns = [col for col in new_df.columns if col not in merge_keys]

                existing_df_latest = existing_df[existing_df['effective_to'] == high_end_date]

                merged = pd.merge(new_df, existing_df_latest, on=merge_keys, how='left', suffixes=('', '_existing'))

                def is_changed(row):
                    for col in compare_columns:
                        if row[col] != row.get(f"{col}_existing"):
                            return True
                    return False

                merged['is_changed'] = merged.apply(is_changed, axis=1)

                # Separate records
                updates_df = merged[merged['is_changed']]
                inserts_df = updates_df[new_df.columns]  # New version of updated records

                if not updates_df.empty:
                    # 1. Update effective_to of old records
                    old_keys = updates_df[merge_keys]
                    conditions = " AND ".join([f"{k} = %s" for k in merge_keys])
                    update_sql = f"""
                        UPDATE project_analytics.{db_schema}.{table}
                        SET effective_to = %s
                        WHERE effective_to = %s AND {conditions}
                    """
                    cursor = self.conn.cursor()
                    for _, row in old_keys.iterrows():
                        cursor.execute(update_sql, (timestamp, high_end_date, *[row[k] for k in merge_keys]))
                    self.conn.commit()
                    cursor.close()

                    logger.info(f"Updated {len(updates_df)} existing records in SCD Type 2")
                # 2. Insert new/changed rows
                if not inserts_df.empty:
                    inserts_df['update_timestamp'] = timestamp
                    inserts_df['effective_from'] = timestamp
                    inserts_df['effective_to'] = high_end_date
                    # inserts_df[surrogate_key] = inserts_df.apply(lambda row: self.generate_surrogate_key(row, unique_keys), axis=1)
                    inserts_df[surrogate_key] = [str(uuid.uuid4()) for _ in range(len(inserts_df))]

                    insert_dataframe(self.conn, inserts_df, table, db_schema)
                    logger.info(f"Inserted {len(inserts_df)} new/changed records into {db_schema}.{table} as part of SCD Type 2")

        else:
            raise ValueError("Unsupported SCD type. Expected 1 or 2.")