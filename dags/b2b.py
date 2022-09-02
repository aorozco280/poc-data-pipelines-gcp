from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "b2b-seed",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    catchup=False
) as dag:

    models = [
        "customer",
        "company",
        "order",
        "inventory",
        "sales",
        "product",
    ]
    models_str = ",".join(models)

    cmd = (
        f"pip install -r /scripts/requirements.txt && "
        f"python /scripts/seed-db.py --model {models_str}"
    )

    create = BashOperator(
        task_id="create",
        bash_command=cmd
    )

    seed = SparkSubmitOperator(
        task_id="seed",
        application="path/to/job.py"
    )

    end = EmptyOperator(task_id="end")

    create >> seed >> end
