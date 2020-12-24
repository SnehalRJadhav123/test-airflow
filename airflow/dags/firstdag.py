from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 12, 22),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id="assignment1", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    parquet_file_import = SparkSubmitOperator(
        task_id="parquet_file_import",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/importparquetfile.py",
        verbose=False
    )

    read_from_hive = HiveOperator(
        task_id="read_from_hive",
        hive_cli_conn_id="hive_conn",
        hql="""
        SELECT * FROM test_airflowdb
        """,
        dag=dag
   )

