from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "b2b",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 30),
    tags=["ingestion"],
    catchup=False,
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
        "cd /app-scripts/seed-db && "
        f"pip install -r requirements.txt && "
        f"python seed_db.py --model {models_str}"
    )

    cmd2 = "cd /app-scripts/orders && " f"python create_orders.py --orders 4800"

    create = BashOperator(task_id="create", bash_command=cmd)

    orders = BashOperator(task_id="populate-orders", bash_command=cmd2)

    seed = SparkSubmitOperator(
        task_id="load",
        application="/etls/csv-load/csv_load.py",
        application_args=["--path", "/data", "--header", "true", "--model", models_str],
        packages="org.postgresql:postgresql:42.5.0",
        py_files="/etls/utils.py",
    )

    create >> orders >> seed
