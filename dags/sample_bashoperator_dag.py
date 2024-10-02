"""
File Name: sample_bashoperator_dag.py

Description: This is created for sample testing of bash operator.

Author: Kumar Omkar
Date: 2024-09-28
"""

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz

with DAG(
    dag_id='sample_bash_operator',
    start_date=datetime(2024,8,31,tzinfo=pytz.UTC),
    end_date=datetime(2024,9,15,tzinfo=pytz.UTC),
    schedule="30 12 * * *",
    max_active_runs=1,
    catchup=True,
    tags=['sample_bash_operator'],
) as dag:
    
    
    start: EmptyOperator=EmptyOperator(
        dag=dag,
        task_id="start"
    )
    
    
    bash_task: BashOperator = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello, World!"'
    )
    
    
    end: EmptyOperator=EmptyOperator(
        dag=dag,
        task_id="end"
    )
    
    #Set task dependencies
    start >> bash_task >> end
    
    # Make the DAGs globally avaiable
    globals()["dag_for_sample_bash_operator__2024_09_28"]=dag
