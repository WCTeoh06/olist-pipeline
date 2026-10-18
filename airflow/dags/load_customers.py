import csv
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/raw/olist_customers_dataset.csv"
COLUMNS = ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
           "customer_city", "customer_state"]


@dag(
    dag_id="load_customers",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["olist"],
)
def load_customers():

    @task
    def create_table():
        PostgresHook(postgres_conn_id="warehouse").run("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                customer_unique_id TEXT,
                customer_zip_code_prefix TEXT,
                customer_city TEXT,
                customer_state TEXT
            )
        """)

    @task
    def load():
        rows, skipped = [], 0
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if not r["customer_id"]:
                    skipped += 1
                    continue
                rows.append((
                    r["customer_id"].strip(),
                    r["customer_unique_id"].strip(),
                    r["customer_zip_code_prefix"].strip(),
                    r["customer_city"].strip().title(),
                    r["customer_state"].strip().upper(),
                ))
        PostgresHook(postgres_conn_id="warehouse").insert_rows(
            table="customers", rows=rows, target_fields=COLUMNS,
            commit_every=5000, replace=True, replace_index="customer_id",
        )
        print(f"Loaded {len(rows)} rows, skipped {skipped}")
        return len(rows)

    @task
    def check(expected):
        actual = PostgresHook(postgres_conn_id="warehouse").get_first(
            "SELECT COUNT(*) FROM customers")[0]
        if actual != expected:
            raise ValueError(f"Expected {expected} rows but found {actual}")
        print(f"Check passed: {actual} rows")

    created = create_table()
    loaded = load()
    created >> loaded
    check(loaded)


load_customers()
