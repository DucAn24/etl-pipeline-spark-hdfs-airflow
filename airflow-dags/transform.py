from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_conn_id = "spark_default"  # Using spark_default to match the connection in Airflow UI

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
    'transform_data',
    default_args=default_args,
    description='Transform data in HDFS using Spark',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start", dag=dag)
    
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
        num_executors=1,
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
        num_executors=1,
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
        num_executors=1,
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
        num_executors=1,
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
        num_executors=1,
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
        num_executors=1,
        dag=dag
    )
    
    # Dimensional model creation - depends on all other transformations
    create_dimensional_model = SparkSubmitOperator(
        task_id="create_dimensional_model",
        application="/usr/local/spark/app/transform/dimensional_model.py",
        name="create-dimensional-model",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="1G",
        executor_cores=1,
        num_executors=1,
        dag=dag
    )
    
    end = DummyOperator(task_id="end", dag=dag)
    
    # Set task dependencies
    
    # Group CRM transformations
    start >> [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details]
    
    # Group ERP transformations
    start >> [transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category]
    
    # All individual transforms must complete before dimensional model is created
    [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details,
     transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category] >> create_dimensional_model
    
    # Final step
    create_dimensional_model >> end