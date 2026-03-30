from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_archer_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline de données Archer-Medaillon (Bronze -> Silver -> Gold)',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'banking'],
) as dag:

  
    t1 = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='python3 /opt/airflow/jobs/ingest_to_bronze.py',
    )

  
    t2 = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python3 /opt/airflow/jobs/job_bronze_to_silver.py',
    )

   
    t3 = BashOperator(
        task_id='silver_to_gold_dimensions',
        bash_command='python3 /opt/airflow/jobs/job_silver_to_gold_dimensions.py',
    )

    
    t4 = BashOperator(
        task_id='silver_to_gold_facts',
        bash_command='python3 /opt/airflow/jobs/job_silver_to_gold_facts.py',
    )

   
    t1 >> t2 >> t3 >> t4
