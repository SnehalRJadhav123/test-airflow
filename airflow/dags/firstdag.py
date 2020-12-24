from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 12, 22),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id="assignment", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

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

    dump_file_hdfs = BashOperator(
        task_id='dump_file_hdfs',
        bash_command="""
        hdfs dfs -mkdir -p /data && \
        hdfs dfs -put -f /usr/local/airflow/dags/files/userdata2.parquet /data
        """
   )

    dump_allFiles_hdfs = BashOperator(
        task_id='dump_allFiles_hdfs',
        bash_command="""
        hdfs dfs -put -f /usr/local/airflow/dags/files /data
        """
   )