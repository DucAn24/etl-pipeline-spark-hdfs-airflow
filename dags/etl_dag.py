from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/spark_app')

from utils.hdfs_utils import check_hdfs_data
from extract.e_source_crm import extract_crm_data
from extract.e_source_erp import extract_erp_data

spark_master = "spark://spark:7077"
spark_conn_id = "spark_default"  # Using spark_default to match the connection in Airflow UI

# Define paths
extract_dir = "/usr/local/spark/resources/data/"
hdfs_path_crm = "/raw/source_crm"
hdfs_path_erp = "/raw/source_erp"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 8),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define DAG
with DAG(
    "etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for data warehouse",
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Define all tasks WITHIN the DAG context
    start = EmptyOperator(task_id="start", dag=dag)
    
    # Extract Task Group within DAG context
    with TaskGroup("extract", dag=dag) as extract_group:
        # Extract from CRM
        extract_from_crm = PythonOperator(
            task_id="extract_from_crm",
            python_callable=extract_crm_data,
            dag=dag
        )
        
        # Extract from ERP
        extract_from_erp = PythonOperator(
            task_id="extract_from_erp",
            python_callable=extract_erp_data,
            dag=dag
        )
        
        # Verify HDFS data
        verify_hdfs_data = PythonOperator(
            task_id="verify_hdfs_data",
            python_callable=check_hdfs_data,
            dag=dag
        )
        
        # Set extract group dependencies
        [extract_from_crm, extract_from_erp] >> verify_hdfs_data
    
    # Transform Task Group within DAG context
    with TaskGroup("transform", dag=dag) as transform_group:
        # CRM transformations
        transform_crm_cust_info = SparkSubmitOperator(
            task_id="transform_crm_cust_info",
            application="/usr/local/spark/app/transform/crm_cust_info.py",
            name="transform-crm-customer-info",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        transform_crm_prd_info = SparkSubmitOperator(
            task_id="transform_crm_prd_info",
            application="/usr/local/spark/app/transform/crm_prd_info.py",
            name="transform-crm-product-info",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        transform_crm_sales_details = SparkSubmitOperator(
            task_id="transform_crm_sales_details",
            application="/usr/local/spark/app/transform/crm_sales_details.py",
            name="transform-crm-sales-details",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        # ERP transformations
        transform_erp_cust_info = SparkSubmitOperator(
            task_id="transform_erp_cust_info",
            application="/usr/local/spark/app/transform/erp_cust_info.py",
            name="transform-erp-customer-info",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        transform_erp_cust_loc = SparkSubmitOperator(
            task_id="transform_erp_cust_loc",
            application="/usr/local/spark/app/transform/erp_cust_loc.py",
            name="transform-erp-customer-location",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        transform_erp_prd_category = SparkSubmitOperator(
            task_id="transform_erp_prd_category",
            application="/usr/local/spark/app/transform/erp_prd_category.py",
            name="transform-erp-product-category",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        # Dimensional model creation
        create_dimensional_model = SparkSubmitOperator(
            task_id="create_dimensional_model",
            application="/usr/local/spark/app/transform/dimensional_model.py",
            name="create-dimensional-model",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            dag=dag
        )
        
        # Set transform internal dependencies
        [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details,
         transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category] >> create_dimensional_model
    
    # Load Task Group within DAG context
    with TaskGroup("load", dag=dag) as load_group:
        load_to_dwh = SparkSubmitOperator(
            task_id="load_to_dwh",
            application="/usr/local/spark/app/load/load_dwh.py",
            name="load-to-data-warehouse",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            executor_memory="1G",
            executor_cores=1,
            num_executors=2,
            jars="/usr/local/spark/resources/jars/postgresql-42.7.5.jar",
            dag=dag
        )
        
        # Validate loaded data
        validate_data = BashOperator(
            task_id="validate_data",
            bash_command="""
                # Connect to PostgreSQL and check row counts
                PGPASSWORD=airflow psql -h postgres_dw -U airflow -d dwh -c "
                SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer
                UNION ALL
                SELECT 'dim_product' as table_name, COUNT(*) as row_count FROM dim_product
                UNION ALL
                SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
                "
            """,
            dag=dag
        )
        
        # Set load group dependencies
        load_to_dwh >> validate_data
    
    end = EmptyOperator(task_id="end", dag=dag)
    
    # Set main task dependencies
    start >> extract_group >> transform_group >> load_group >> end