from datetime import datetime
from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
   'owner': 'snehal',
   'depends_on_past': False,
}

lop_dag = DAG('lop_dag',start_date=datetime.now(),default_args=default_args,schedule_interval='@hourly')

lop_task = LatestOnlyOperator(
   task_id = 'lop_task',
   dag = lop_dag
)

branch1 = DummyOperator(
   task_id='branch1',
   dag=lop_dag
)

branch2 = DummyOperator(
   task_id='branch2',
   dag=lop_dag
)

branch3 = DummyOperator(
   task_id='branch3',
   dag=lop_dag
)
branch4 = DummyOperator(
   task_id='branch4',
   dag=lop_dag,
   trigger_rule=TriggerRule.ALL_DONE
)

branch1 << lop_task
branch3 << [branch1,branch2]
branch4 << [branch1,branch2]

