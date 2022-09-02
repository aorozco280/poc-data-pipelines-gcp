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
        f"cd /app-scripts/weblog && "
        f"pip install -r requirements.txt && "
        f"python generate_traces.py"
    )

    generate = BashOperator(
        task_id="generate",
        bash_command=cmd,
    )

    load = SparkSubmitOperator(
        task_id="load",
        application="/etls/weblog-load/weblog_load.py",
        application_args=["--path", "/apache-logs"],
        packages="org.postgresql:postgresql:42.5.0"
    )

    end = EmptyOperator(task_id="end")

    generate >> load >> end
