from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    "popular-devices",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    tags=["report"],
    catchup=False,
) as dag:
    sensor = ExternalTaskSensor(
        task_id="wait-for-weblog",
        external_dag_id="weblog",
        poke_interval=5,
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate",
        application="/etls/reports/popular-devices/popular_devices.py",
        application_args=["--num-devices", "10"],
        packages="org.postgresql:postgresql:42.5.0",
        py_files="/etls/utils.py",
        retries=2,
    )

    sensor >> aggregate
