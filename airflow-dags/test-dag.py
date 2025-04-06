from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Định nghĩa hàm Python sẽ được gọi bởi task
def print_hello():
    print("Hello, Airflow!")

# Thiết lập thông tin DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_airflow',
    default_args=default_args,
    description='DAG đơn giản in Hello Airflow',
    schedule_interval=timedelta(days=1), 
)

# Định nghĩa Task
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Thiết lập thứ tự chạy task
task_hello