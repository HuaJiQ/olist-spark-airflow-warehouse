
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="olist_m6_hello",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "m6"],
):

    @task
    def prepare():
        print("[M6] Preparing test batch")

    @task
    def validate():
        print("[M6] Validating test batch")

    @task
    def finish():
        print("[M6] Test workflow completed")

    prepare() >> validate() >> finish()
