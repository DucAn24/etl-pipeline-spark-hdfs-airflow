from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from datetime import datetime, timedelta

spark_master = "spark://spark:7077"
spark_conn_id = "spark_default" 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'transform_data',
    default_args=default_args,
    description='Transform data in HDFS using Spark',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    
    # CRM transformations
    transform_crm_cust_info = SparkSubmitOperator(
        task_id="transform_crm_cust_info",
        application="/opt/airflow/spark_app/transform/crm_cust_info.py",
        name="transform-crm-customer-info",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )

    transform_crm_prd_info = SparkSubmitOperator(
        task_id="transform_crm_prd_info",
        application="/opt/airflow/spark_app/transform/crm_prd_info.py",
        name="transform-crm-product-info",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )

    transform_crm_sales_details = SparkSubmitOperator(
        task_id="transform_crm_sales_details",
        application="/opt/airflow/spark_app/transform/crm_sales_details.py",
        name="transform-crm-sales-details",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )
    
    # ERP transformations
    transform_erp_cust_info = SparkSubmitOperator(
        task_id="transform_erp_cust_info",
        application="/opt/airflow/spark_app/transform/erp_cust_info.py",
        name="transform-erp-customer-info",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )

    transform_erp_cust_loc = SparkSubmitOperator(
        task_id="transform_erp_cust_loc",
        application="/opt/airflow/spark_app/transform/erp_cust_loc.py",
        name="transform-erp-customer-location",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )

    transform_erp_prd_category = SparkSubmitOperator(
        task_id="transform_erp_prd_category",
        application="/opt/airflow/spark_app/transform/erp_prd_category.py",
        name="transform-erp-product-category",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )
    
    create_dimensional_model = SparkSubmitOperator(
        task_id="create_dimensional_model",
        application="/opt/airflow/spark_app/transform/dimensional_model.py",
        name="create-dimensional-model",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="512M",
        executor_cores=2,
        num_executors=2,
        py_files="/opt/airflow/spark_app/utils/spark_utils.py",
        dag=dag
    )
    
    end = EmptyOperator(task_id="end", dag=dag)
    
    # Group CRM transformations
    start >> [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details]
    
    # Group ERP transformations
    start >> [transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category]

    [transform_crm_cust_info, transform_crm_prd_info, transform_crm_sales_details,
     transform_erp_cust_info, transform_erp_cust_loc, transform_erp_prd_category] >> create_dimensional_model
    
    create_dimensional_model >> end