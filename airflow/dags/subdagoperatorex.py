
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 1, 1)
}

dag = DAG(
    dag_id="subdag_operator",
    default_args=args,
    schedule_interval="@hourly",
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

def subdag_function(schedule_interval,default_args):
    subdag1=DAG(dag_id='subdag_operator.userdata',schedule_interval=schedule_interval, start_date=args['start_date'])
    
    userdata1 = BashOperator(
        task_id='userdata1',
        bash_command="hdfs dfs -put /usr/local/airflow/dags/files/userdata1.parquet /user/hive/warehouse/userdata1/",
        dag=subdag1
   )

    userdata2 = BashOperator(
        task_id='userdata2',
        bash_command="hdfs dfs -put /usr/local/airflow/dags/files/userdata2.parquet /user/hive/warehouse/userdata2/",
        dag=subdag1
   )

    userdata3 = BashOperator(
        task_id='userdata3',
        bash_command="hdfs dfs -put -f /usr/local/airflow/dags/files/userdata3.parquet /user/hive/warehouse/userdata3/",
        dag=subdag1
   )

    userdata4 = BashOperator(
        task_id='userdata4',
        bash_command="hdfs dfs -put -f /usr/local/airflow/dags/files/userdata4.parquet /user/hive/warehouse/userdata4/",
        dag=subdag1
   )
    
    userdata5 = BashOperator(
        task_id='userdata5',
        bash_command="hdfs dfs -put -f /usr/local/airflow/dags/files/userdata5.parquet /user/hive/warehouse/userdata5/",
        dag=subdag1
   )
    return subdag1

userdata = SubDagOperator(
    task_id='userdata', 
    subdag=subdag_function(dag.schedule_interval, args),
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start >> userdata >> end