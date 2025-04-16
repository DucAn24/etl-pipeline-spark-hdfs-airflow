from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'extract_to_staging',
    default_args=default_args,
    description='Extract data from CRM and ERP sources to HDFS staging',
    schedule_interval='@daily',
    catchup=False
)

# Task to extract CRM data
extract_crm = BashOperator(
    task_id='extract_crm_data',
    bash_command='docker exec python3 python /usr/local/spark/app/extract/e_source_crm.py',
    dag=dag
)

# Task to extract ERP data
extract_erp = BashOperator(
    task_id='extract_erp_data',
    bash_command='docker exec python3 python /usr/local/spark/app/extract/e_source_erp.py',
    dag=dag
)

# Define task dependencies
extract_crm >> extract_erp