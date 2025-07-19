import psycopg2
import pandas as pd
from psycopg2 import sql
from code.logger_config import get_logger

logger = get_logger()

def connect_postgres(config):
    return psycopg2.connect(
        dbname=config["dbname"].strip(),
        user=config["user"].strip(),
        password=config["password"].strip(),
        host=config["host"].strip(),
        port=config["port"]
    )

def run_query(conn, query):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise

def truncate_table(conn, table, schema):
    """Truncate the specified table before loading new data."""
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        logger.info(f"Successfully truncated table {schema}.{table}")
    except Exception as e:
        logger.error(f"Failed to truncate table {schema}.{table}: {e}")
        raise

def insert_dataframe(conn, df, table, schema):
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            columns = list(row.index)
            values = tuple(row)
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
            cursor.execute(insert_query, values)
        conn.commit()
        cursor.close()
        logger.info(f"Successfully inserted {len(df)} rows into {schema}.{table}")
    except Exception as e:
        logger.error(f"Failed to insert data into {schema}.{table}: {e}")
        conn.rollback()
        if cursor:
            cursor.close()
        raise