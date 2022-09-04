from pyspark.sql import SparkSession, DataFrame
from typing import Optional

# These options can be set using an airflow connection
_JDBC_BASE_OPTIONS = {
    "url": "jdbc:postgresql://app-db:5432/challenge",
    "properties": {
        "user": "postgres",
        "password": "postgres",
    },
}


def read_postgres(spark_session: SparkSession, table_name: str) -> DataFrame:
    options = _JDBC_BASE_OPTIONS | {"table": f'"{table_name}"'}

    return spark_session.read.option("driver", "org.postgresql.Driver").jdbc(**options)


def write_postgres(df: DataFrame, table_name: str):
    options = _JDBC_BASE_OPTIONS | {"table": f'"{table_name}"', "mode": "overwrite"}

    df.write.option("driver", "org.postgresql.Driver").jdbc(**options)


def read_csv(
    spark_session: SparkSession, filepath: str, header: Optional[bool] = True
) -> DataFrame:
    reader = spark_session.read.option("inferSchema", "true")

    if header:
        reader = reader.option("header", "true")

    return reader.csv(filepath)


def write_csv(df: DataFrame, filepath: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(filepath)

    from pathlib import Path
    import os

    for path in Path(filepath).rglob("*.csv"):
        os.rename(path, f"{filepath}/report.csv")
        os.remove(f"{filepath}/_SUCCESS")


def spark_session(app_name: str):
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
