from pyspark.sql import SparkSession

_JDBC_BASE_OPTIONS = {
    "url": "jdbc:postgresql://app-db:5432/challenge",
    "properties": {
        "user": "postgres",
        "password": "postgres",
    },
}


def read_postgres(spark_session, table_name: str):
    options = _JDBC_BASE_OPTIONS | {"table": f'"{table_name}"'}

    return spark_session.read.option("driver", "org.postgresql.Driver").jdbc(**options)


def write_postgres(df, table_name: str):
    options = _JDBC_BASE_OPTIONS | {"table": f'"{table_name}"', "mode": "overwrite"}

    df.write.option("driver", "org.postgresql.Driver").jdbc(**options)


def spark_session(app_name: str):
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
