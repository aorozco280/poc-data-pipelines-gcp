from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor


def daily_dag_last_exec(execution_date, **kwargs):
    return datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)


with DAG(
    "monthly-sales",
    schedule_interval="@monthly",
    start_date=datetime(2022, 7, 1),
    tags=["report"],
    catchup=False,
) as dag:
    sensor = ExternalTaskSensor(
        task_id="wait-for-b2b",
        external_dag_id="b2b",
        poke_interval=5,
        execution_date_fn=daily_dag_last_exec,
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate",
        application="/etls/reports/monthly-sales/monthly_sales.py",
        application_args=["--date", "{{ ds }}"],
        packages="org.postgresql:postgresql:42.5.0",
        py_files="/etls/utils.py",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    sensor >> aggregate
