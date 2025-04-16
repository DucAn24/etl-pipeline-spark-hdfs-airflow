from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import subprocess
import os

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
mysql_config = {
    "host": "mysql_source",
    "user": "etl",
    "password": "etl",
    "database": "source_erp"
}

# Define paths
extract_dir = "/usr/local/spark/resources/data/"
hdfs_path_crm = "/raw/source_crm"
hdfs_path_erp = "/raw/source_erp"

###############################################
# Python callables for PythonOperator
###############################################
def extract_crm_data(**kwargs):
    """Extract CRM data using the Python script in the container"""
    try:
        cmd = ["docker", "exec", "python3", "python", "/usr/local/spark/app/extract/e_source_crm.py"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error extracting CRM data: {e}")
        print(f"Error output: {e.stderr}")
        return False

def extract_erp_data(**kwargs):
    """Extract ERP data using the Python script in the container"""
    try:
        cmd = ["docker", "exec", "python3", "python", "/usr/local/spark/app/extract/e_source_erp.py"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error extracting ERP data: {e}")
        print(f"Error output: {e.stderr}")
        return False

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
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL process for Data Warehouse',
    schedule_interval=timedelta(days=1),
) as dag:

    start = DummyOperator(task_id="start", dag=dag)
    
    # Extract Task Group
    with TaskGroup("extract") as extract_group:
        extract_crm_task = PythonOperator(
            task_id='extract_crm_data',
            python_callable=extract_crm_data,
            provide_context=True,
        )
        
        extract_erp_task = PythonOperator(
            task_id='extract_erp_data',
            python_callable=extract_erp_data,
            provide_context=True,
        )
    
    # Transform Task Group
    with TaskGroup("transform") as transform_group:
        # CRM transformations
        transform_crm_cust_info = SparkSubmitOperator(
            task_id="transform_crm_cust_info",
            application="/usr/local/spark/app/transform/crm_cust_info.py",
            name="transform-crm-customer-info",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        transform_crm_prd_info = SparkSubmitOperator(
            task_id="transform_crm_prd_info",
            application="/usr/local/spark/app/transform/crm_prd_info.py",
            name="transform-crm-product-info",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        transform_crm_sales_details = SparkSubmitOperator(
            task_id="transform_crm_sales_details",
            application="/usr/local/spark/app/transform/crm_sales_details.py",
            name="transform-crm-sales-details",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        # ERP transformations
        transform_erp_cust_info = SparkSubmitOperator(
            task_id="transform_erp_cust_info",
            application="/usr/local/spark/app/transform/erp_cust_info.py",
            name="transform-erp-customer-info",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        transform_erp_cust_loc = SparkSubmitOperator(
            task_id="transform_erp_cust_loc",
            application="/usr/local/spark/app/transform/erp_cust_loc.py",
            name="transform-erp-customer-location",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        transform_erp_prd_category = SparkSubmitOperator(
            task_id="transform_erp_prd_category",
            application="/usr/local/spark/app/transform/erp_prd_category.py",
            name="transform-erp-product-category",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
        
        # Dimensional model creation
        create_dimensional_model = SparkSubmitOperator(
            task_id="create_dimensional_model",
            application="/usr/local/spark/app/transform/dimensional_model.py",
            name="create-dimensional-model",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
    
    # Load Task Group
    with TaskGroup("load") as load_group:
        load_to_dwh = SparkSubmitOperator(
            task_id="load_to_dwh",
            application="/usr/local/spark/app/load/load_dwh.py",
            name="load-to-data-warehouse",
            conn_id="spark_default",
            verbose=1,
            conf={"spark.master": spark_master},
            dag=dag
        )
    
    end = DummyOperator(task_id="end", dag=dag)
    
    # Set task dependencies
    start >> extract_group
    
    # Extract dependencies
    extract_group >> transform_group
    
    # Transform dependencies - first run individual transformation tasks
    [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details, 
     transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category] >> create_dimensional_model
    
    # After all transforms, create dimensional model and load to DWH
    transform_group >> load_group
    
    # Finish
    load_group >> end