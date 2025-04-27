from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
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
    'load_to_dwh',
    default_args=default_args,
    description='Load dimensional model from HDFS to PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Tasks
    start = DummyOperator(task_id="start", dag=dag)
    
    # Loading data from HDFS to PostgreSQL using Spark
    load_to_dwh = SparkSubmitOperator(
        task_id="load_to_dwh",
        application="/usr/local/spark/app/load/load_dwh.py",
        name="load-to-data-warehouse",
        conn_id=spark_conn_id,
        verbose=True,
        deploy_mode="client",
        executor_memory="1G",
        executor_cores=1,
        num_executors=1,
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
    
    end = DummyOperator(task_id="end", dag=dag)
    
    # Set task dependencies
    start >> load_to_dwh >> validate_data >> end