from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 21),
    'retries': 1,
}

dag = DAG(
    'sample_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['sample_dag']
)

def print_hello(**kwargs):
    print("Hello, World!")

def print_goodbye(**kwargs):
    print("Goodbye, World!")

task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)


task2 = PythonOperator(
    task_id='print_goodbye',
    python_callable=print_goodbye,
    dag=dag
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "DAG finished!"',
    dag=dag
)

# Define dependencies between tasks
task1 >> task2 >> end_task