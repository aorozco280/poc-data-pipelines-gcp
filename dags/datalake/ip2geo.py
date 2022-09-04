from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "ip2geo",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    tags=["ingestion"],
    catchup=False,
) as dag:
    cmd = (
        "cd /app-scripts/seed-db && "
        f"pip install -r requirements.txt && "
        f"python seed_db.py --model ip2geo"
    )

    create = BashOperator(task_id="create", bash_command=cmd)

    seed = SparkSubmitOperator(
        task_id="load",
        application="/etls/csv-load/csv_load.py",
        application_args=["--path", "/data", "--header", "true", "--model", "ip2geo"],
        packages="org.postgresql:postgresql:42.5.0",
        py_files="/etls/utils.py",
    )

    create >> seed
