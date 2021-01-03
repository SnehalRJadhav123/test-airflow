from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(dag_id="assignment", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:

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
        SELECT * FROM airflowdb
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

 #   email_notification = EmailOperator(
 #      task_id="email_notification",
 #      to="snehal.r.jadhav23@gmail.com",
 #      subject="Assignment completed",
 #      html_content="""
 #               <h3>Assignment completed</h3>
 #           """
 #  )

    sending_slack_notification = SlackAPIPostOperator(
        task_id="sending_slack",
        token="xoxp-1583002105367-1597981360883-1591005142598-f0fcf268fdb7a5028f3f37f0d1ea3f2e",
        username="airflow",
        text="DAG Airflow assignment: DONE",
        channel="#airflow-exploit"
   )

    create_hive_tables = HiveOperator(
        task_id="create_hive_tables",
        hive_cli_conn_id="hive_conn",
        hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS userdata5 (
        registration_dttm TIMESTAMP,
        id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        gender STRING,
        ip_address STRING,
        cc STRING,
        country STRING,
        birthdate STRING,
        salary DOUBLE,
        title STRING,
        comments STRING
        )
        """,
        dag=dag
    )
    