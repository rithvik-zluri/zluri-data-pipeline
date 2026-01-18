import psycopg2
import os
from prefect.context import FlowRunContext

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "rithvik_zluri_pipeline_db",
    "user": "rithvik_zluri_pipeline_user",
    "password": "rithvik_zluri_pipeline_pass"
}

def get_connection():
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
    return {
        "url": "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db",
        "user": os.getenv("PG_USER", "rithvik_zluri_pipeline_user"),
        "password": os.getenv("PG_PASSWORD", "rithvik_zluri_pipeline_pass"),
        "driver": "org.postgresql.Driver",
    }
