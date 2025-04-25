from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append('/usr/local/spark/app')


from utils.hdfs_utils import check_hdfs_data
from extract.e_source_crm import extract_crm_data
from extract.e_source_erp import extract_erp_data

###############################################
# DAG Definition
###############################################
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'to_staging',
    default_args=default_args,
    description='Extract and stage data from CRM and ERP sources to HDFS',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define tasks
    start = DummyOperator(task_id="start", dag=dag)
    
    extract_crm_task = PythonOperator(
        task_id='extract_crm_data',
        python_callable=extract_crm_data,
        provide_context=True,
        dag=dag
    )
    
    extract_erp_task = PythonOperator(
        task_id='extract_erp_data',
        python_callable=extract_erp_data,
        provide_context=True,
        dag=dag
    )
    
    verify_hdfs_data = PythonOperator(
        task_id='verify_hdfs_data',
        python_callable=check_hdfs_data,
        provide_context=True,
        dag=dag
    )
    
    end = DummyOperator(task_id="end", dag=dag)
    
    # Set task dependencies
    start >> [extract_crm_task, extract_erp_task] >> verify_hdfs_data >> end