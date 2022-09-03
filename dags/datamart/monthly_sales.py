from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    "monthly-sales",
    schedule_interval="@monthly",
    start_date=datetime(2022, 7, 1),
    catchup=False,
) as dag:
    sensor = ExternalTaskSensor(
        task_id="wait-for-b2b",
        external_dag_id="b2b",
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate",
        application="/etls/monthly-sales/monthly_sales.py",
        application_args=["--date", "{{ ds }}"],
        packages="org.postgresql:postgresql:42.5.0",
    )

    sensor >> aggregate
