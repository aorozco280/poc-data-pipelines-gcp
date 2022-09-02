from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "weblog-seed",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    catchup=False
) as dag:
    cmd = (
        f"pip install -r /scripts/requirements.txt && "
        f"python /scripts/generate_traces.py"
    )

    traces = BashOperator(
        task_id="traces",
        bash_command=cmd
    )

    end = EmptyOperator(task_id="end")

    traces >> end
