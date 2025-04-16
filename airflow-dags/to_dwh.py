from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
dag = DAG(
    'transform_load_to_dwh',
    default_args=default_args,
    description='Transform data and load to PostgreSQL DWH',
    schedule_interval=None,
    catchup=False
)

# Check if HDFS data is ready
check_hdfs_data = BashOperator(
    task_id='check_hdfs_data',
    bash_command='''
    docker exec -u hadoop namenode hdfs dfs -test -e /raw/source_crm/sales_details.csv && \
    docker exec -u hadoop namenode hdfs dfs -test -e /raw/source_erp/cust_az12.csv && \
    ''',
    dag=dag
)

# Transform CRM customer info
transform_crm_cust = BashOperator(
    task_id='transform_crm_cust',
    bash_command='docker exec spark-worker python /spark/app/transform/crm_cust_info.py',
    dag=dag
)

# Transform CRM product info
transform_crm_prod = BashOperator(
    task_id='transform_crm_prod',
    bash_command='docker exec spark-worker python /spark/app/transform/crm_prd_info.py',
    dag=dag
)

# Transform CRM sales details
transform_crm_sales = BashOperator(
    task_id='transform_crm_sales',
    bash_command='docker exec spark-worker python /spark/app/transform/crm_sales_details.py',
    dag=dag
)

# Transform ERP customer info
transform_erp_cust = BashOperator(
    task_id='transform_erp_cust',
    bash_command='docker exec spark-worker python /spark/app/transform/erp_cust_info.py',
    dag=dag
)

# Transform ERP customer location
transform_erp_loc = BashOperator(
    task_id='transform_erp_loc',
    bash_command='docker exec spark-worker python /spark/app/transform/erp_cust_loc.py',
    dag=dag
)

# Transform ERP product category
transform_erp_prod = BashOperator(
    task_id='transform_erp_prod',
    bash_command='docker exec spark-worker python /spark/app/transform/erp_prd_category.py',
    dag=dag
)

# Build dimensional model
build_dimensional_model = BashOperator(
    task_id='build_dimensional_model',
    bash_command='docker exec spark-worker python /spark/app/transform/dimensional_model.py',
    dag=dag
)

# Load to PostgreSQL DWH
load_to_dwh = BashOperator(
    task_id='load_to_dwh',
    bash_command='docker exec spark-worker python /spark/app/load/load_dwh.py',
    dag=dag
)

# Set task dependencies
check_hdfs_data >> [
    transform_crm_cust, 
    transform_crm_prod, 
    transform_crm_sales,
    transform_erp_cust,
    transform_erp_loc,
    transform_erp_prod
] >> build_dimensional_model >> load_to_dwh