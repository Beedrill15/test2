from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
	'owner': 'Me',
	'depends_on_past': False,
	'start_date': datetime(2019, 11, 8),
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

dag = DAG('nineam', default_args=default_args, schedule_interval='00 9 * * *')

t1 = BashOperator(
	task_id = 'run_assignments',
	bash_command='bash test2.sh',
	dag=dag)


t2 = BashOperator(
	task_id = 'use_flume',
	bash_command='flume-ng agent --conf /home/hadoop/flume/ --conf -file /home/hadoop/flume/conf/multi.conf -name NetcatAgent -Dflume.root.logger=INFO,console',
	dag=dag)

t1.set_upstream(t2)
