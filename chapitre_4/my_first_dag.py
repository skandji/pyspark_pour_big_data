# Importing the Required Libraries
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Defining the default Arguments
args = {
	'owner': 'Pramod',
	'start_date': airflow.utils.dates.days_ago(3),
	#'end_date': datetime(2018, 12, 30),
	'depends_on_past': False,
	'email': ['airflow@example.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

# Creating a DAG
dag = DAG (
	'pramod_airflow_dag',
	defaulty_args = args
	description = 'A simple DAG',
	# continue to run DAG once per day
	schedule_interval=timedelta(days = 1)
	)

# Declaring Tasks
t1 = BashOperator(
	task_id='print_date',
	bash_command='date',
	dag=dag
	)

t2 = BashOperator(
	task_id='sleep',
	depends_on_past=False,
	bash_command='sleep 5',
	dag=dag
	)

# Mentioning Dependencies
t1 >> t2
