from datetime import datetime, timedelta
from airflow.sdk import dag, task


@dag(
    dag_id="hello_pipeline",
    schedule=None,                      # run only when we trigger it
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["learning"],
)
def hello_pipeline():

    @task
    def extract():
        # pretend this data came from somewhere, with a bad value
        return [1, 2, 3, 4, 5, None, 7]

    @task
    def transform(numbers):
        cleaned = [n * 2 for n in numbers if n is not None]
        print(f"Cleaned: {cleaned}")
        return cleaned

    @task
    def load(numbers):
        print(f"Loaded {len(numbers)} rows, total = {sum(numbers)}")

    load(transform(extract()))


hello_pipeline()
