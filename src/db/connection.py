import os

import psycopg2
from prefect.context import FlowRunContext


DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DB", "rithvik_zluri_pipeline_db"),
    "user": os.getenv("PG_USER", "rithvik_zluri_pipeline_user"),
    "password": os.getenv("PG_PASSWORD", "rithvik_zluri_pipeline_pass"),
}


def get_connection():
    """
    Get a psycopg2 connection using environment-based configuration.

    Expected environment variables (with safe defaults for local dev):
    - PG_HOST
    - PG_PORT
    - PG_DB
    - PG_USER
    - PG_PASSWORD
    """
    return psycopg2.connect(**DB_CONFIG)


def run_sql_file(file_path: str, day: str | None = None):
    conn = get_connection()

    try:
        with conn.cursor() as cursor:

            # -----------------------------
            # Prefect Flow context (SAFE)
            # -----------------------------
            flow_ctx = FlowRunContext.get()
            flow_run_id = str(flow_ctx.flow_run.id) if flow_ctx else None

            # -----------------------------
            # Inject session variables
            # -----------------------------
            if flow_run_id:
                cursor.execute("SET LOCAL app.flow_run_id = %s;", (flow_run_id,))
            if day:
                cursor.execute("SET LOCAL app.day = %s;", (day,))

            # -----------------------------
            # Execute SQL file
            # -----------------------------
            with open(file_path, "r") as f:
                sql = f.read()
                cursor.execute(sql)

        conn.commit()
        print(f"Executed {file_path} successfully")

    except Exception as e:
        conn.rollback()
        print(f"Error executing {file_path}: {e}")
        raise

    finally:
        conn.close()

def get_postgres_properties():
    """
    Return JDBC properties for Spark <-> Postgres integration, sourced from env.

    Environment variables (all optional, sensible local defaults are provided):
    - PG_HOST
    - PG_PORT
    - PG_DB
    - PG_USER
    - PG_PASSWORD
    - PG_DRIVER
    - PG_JDBC_URL  (if not set, constructed from PG_HOST/PG_PORT/PG_DB)
    """
    jdbc_url = os.getenv("PG_JDBC_URL")

    if not jdbc_url:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        db_name = os.getenv("PG_DB", "rithvik_zluri_pipeline_db")
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"

    return {
        "url": jdbc_url,
        "user": os.getenv("PG_USER", "rithvik_zluri_pipeline_user"),
        "password": os.getenv("PG_PASSWORD", "rithvik_zluri_pipeline_pass"),
        "driver": os.getenv("PG_DRIVER", "org.postgresql.Driver"),
    }

