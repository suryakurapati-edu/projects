import sys
import os
import configparser
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

class ETLJob:
    def __init__(self, job_name):
        self.job_name = job_name
        self.config = self.load_config()
        self.spark = self.init_spark()
        self.logger = self.setup_logger()

    def setup_logger(self):
        """Initializes job-level logger to file."""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(log_dir, f"log_{self.job_name}_{timestamp}.log")

        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(self.job_name)

    def load_config(self):
        """Loads the .conf configuration file based on job name."""
        config_path = f"job_config/{self.job_name}.conf"
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file {config_path} not found.")
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def init_spark(self):
        """Initializes Spark session."""
        return SparkSession.builder.appName(self.job_name).getOrCreate()

    def read_sources(self):
        """Reads all source files from config and creates temp views."""
        self.logger.info("Reading source data files...")
        self.source_views = {}
        for table, path in self.config.items("SOURCE"):
            df = self.spark.read.option("header", True).csv(path)
            df.createOrReplaceTempView(table)
            self.source_views[table] = df
            self.logger.info(f"Loaded {table} from {path}")

    def apply_sql_transformation(self):
        """Executes SQL logic from file using temp views."""
        sql_path = self.config.get("TRANSFORM", "sql_path")
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file {sql_path} not found.")
        with open(sql_path, "r") as f:
            sql_query = f.read()
        transformed_df = self.spark.sql(sql_query)
        self.logger.info("Applied SQL transformation.")
        return transformed_df

    def validate_and_dedup(self, df):
        """Removes rows with nulls in unique keys and drops duplicates."""
        if not self.config.has_section("VALIDATION"):
            self.logger.warning("No VALIDATION section found in config.")
            return df

        unique_keys = self.config.get("VALIDATION", "unique_keys").split(",")
        unique_keys = [key.strip() for key in unique_keys]
        for key in unique_keys:
            df = df.filter(col(key).isNotNull())
        df = df.dropDuplicates(unique_keys)
        self.logger.info(f"Performed null checks and deduplication on keys: {unique_keys}")
        return df

    def write_to_s3(self, df):
        """Writes final data to S3 as parquet."""
        dest_path = self.config.get("DESTINATION", "s3_path")
        df.write.mode("overwrite").parquet(dest_path)
        self.logger.info(f"Data written to S3 path: {dest_path}")

    def create_table_if_not_exists(self, df):
        """Creates MySQL table in RDS if it doesn't exist already."""
        import mysql.connector

        host = self.config.get("RDS", "host")
        user = self.config.get("RDS", "username")
        password = self.config.get("RDS", "password")
        database = self.config.get("RDS", "database")
        schema = self.config.get("RDS", "schema")
        table = self.config.get("RDS", "table")

        # Convert Spark schema to MySQL DDL
        dtype_map = {
            "StringType": "VARCHAR(255)",
            "IntegerType": "INT",
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE",
            "TimestampType": "DATETIME",
        }

        ddl = []
        for field in df.schema.fields:
            dtype = dtype_map.get(str(field.dataType), "VARCHAR(255)")
            ddl.append(f"`{field.name}` {dtype}")
        ddl_sql = f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` ({', '.join(ddl)});"

        # Connect to MySQL and execute
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{schema}`;")
        cursor.execute(ddl_sql)
        conn.commit()
        cursor.close()
        conn.close()
        self.logger.info(f"Verified or created table `{schema}`.`{table}` in RDS.")

    def write_to_rds(self, df):
        """Writes data to RDS MySQL using JDBC."""
        host = self.config.get("RDS", "host")
        user = self.config.get("RDS", "username")
        password = self.config.get("RDS", "password")
        db = self.config.get("RDS", "database")
        schema = self.config.get("RDS", "schema")
        table = self.config.get("RDS", "table")
        port = self.config.get("RDS", "port", fallback="3306")

        # Ensure table exists
        self.create_table_if_not_exists(df)

        url = f"jdbc:mysql://{host}:{port}/{db}"

        df.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("overwrite") \
            .save()

        self.logger.info(f"Data written to RDS MySQL table: {schema}.{table}")

    def run(self):
        """Runs the full ETL process."""
        try:
            self.logger.info(f"Started ETL job: {self.job_name}")
            self.read_sources()
            df = self.apply_sql_transformation()
            df = df.withColumn("load_timestamp", current_timestamp())
            df = self.validate_and_dedup(df)
            self.write_to_s3(df)
            self.write_to_rds(df)
            self.logger.info("ETL job completed successfully.")
        except Exception as e:
            self.logger.exception(f"ETL job failed: {str(e)}")
            raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit python.py <job_name>")
        sys.exit(1)

    job_name = sys.argv[1]
    job = ETLJob(job_name)
    job.run()
