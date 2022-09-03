from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    "popular-products",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    catchup=False,
) as dag:
    sensor_weblogs = ExternalTaskSensor(
        task_id="wait-for-weblog",
        external_dag_id="weblog",
        poke_interval=5,
    )
    sensor_ip2geo = ExternalTaskSensor(
        task_id="wait-for-ip2geo",
        external_dag_id="ip2geo",
        poke_interval=5,
    )
    sensor_b2b = ExternalTaskSensor(
        task_id="wait-for-b2b",
        external_dag_id="b2b",
        poke_interval=5,
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate",
        application="/etls/reports/popular-products/popular_products.py",
        application_args=["--num-products", "5"],
        packages="org.postgresql:postgresql:42.5.0",
        py_files="/etls/utils.py",
    )

    [sensor_weblogs, sensor_ip2geo, sensor_b2b] >> aggregate
