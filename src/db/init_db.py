from .connection import run_sql_file
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_DIR = os.path.join(BASE_DIR, "migrations")

def create_tables():
    run_sql_file(os.path.join(MIGRATIONS_DIR, "001_create_tables.sql"))

if __name__ == "__main__":
    create_tables()
