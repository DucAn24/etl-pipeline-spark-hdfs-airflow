from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import subprocess
import os
import sys
import glob
import csv
import shutil
from hdfs import InsecureClient
import pymysql  # Thay đổi từ mysql.connector sang pymysql

###############################################
# Parameters
###############################################
# Define paths - thay đổi sang thư mục mà Airflow có quyền ghi
extract_dir = "/tmp/airflow_data/"   # Thư mục /tmp thường có quyền ghi
hdfs_path_crm = "/raw/source_crm"
hdfs_path_erp = "/raw/source_erp"

###############################################
# Helper Functions
###############################################
def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def extract_csv_to_processing_area(source_file, destination_dir, filename=None):
    """Copy a CSV file from source to destination directory"""
    try:
        # Make sure destination directory exists
        ensure_dir(destination_dir)
        
        # Use original filename if not specified
        if filename is None:
            filename = os.path.basename(source_file)
        
        destination_file = os.path.join(destination_dir, filename)
        
        # Copy the file
        shutil.copy2(source_file, destination_file)
        
        print(f"Successfully copied {source_file} to {destination_file}")
        return destination_file
    except Exception as e:
        print(f"Error copying file {source_file}: {str(e)}")
        return None

def load_to_hdfs(local_file, hdfs_path):
    """Load a local file to HDFS using WebHDFS API or HDFS client"""
    try:
        # Create HDFS client
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        filename = os.path.basename(local_file)
        hdfs_file = f"{hdfs_path}/{filename}"
        
        # Make sure the directory exists
        hdfs_client.makedirs(hdfs_path)
        
        # Upload the file to HDFS
        with open(local_file, 'rb') as local_f:
            hdfs_client.write(hdfs_file, local_f, overwrite=True)
        
        print(f"Successfully loaded {local_file} to {hdfs_file}")
        return True
    except Exception as e:
        print(f"Error loading to HDFS: {str(e)}")
        return False

def extract_mysql_table(conn, table_name, output_dir):
    """Extract a MySQL table to a CSV file"""
    try:
        cursor = conn.cursor()
        
        # Get column names
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [column[0] for column in cursor.fetchall()]
        
        # Query data
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Create output directory if it doesn't exist
        ensure_dir(output_dir)
        
        # Write to CSV
        output_file = os.path.join(output_dir, f"{table_name}.csv")
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Write header
            writer.writerows(rows)    # Write data rows
        
        print(f"Extracted {len(rows)} rows from {table_name} to {output_file}")
        return output_file
    except Exception as e:
        print(f"Error extracting table {table_name}: {str(e)}")
        return None
    finally:
        cursor.close()

###############################################
# Python callables for PythonOperator
###############################################
def extract_crm_data(**kwargs):
    """Extract CRM data directly within the Airflow container"""
    try:
        # Source directory containing CSV files
        source_dir = "/usr/local/datasets/source_crm"
        
        # Destination directory for extracted files (local staging)
        crm_extract_dir = os.path.join(extract_dir, "source_crm")
        
        # Get all CSV files in the source directory
        csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {source_dir}")
            return False
        
        print(f"Found {len(csv_files)} CSV files to extract")
        
        # Process each CSV file
        success_count = 0
        for csv_file in csv_files:
            print(f"Processing {csv_file}...")
            
            # Extract to local staging
            local_file = extract_csv_to_processing_area(csv_file, crm_extract_dir)
            
            if local_file:
                print(f"CSV extraction to local staging completed successfully for {os.path.basename(csv_file)}")
                
                # Load to HDFS
                result = load_to_hdfs(local_file, hdfs_path_crm)
                if result:
                    print(f"CSV loading to HDFS completed successfully for {os.path.basename(csv_file)}")
                    success_count += 1
                else:
                    print(f"CSV loading to HDFS failed for {os.path.basename(csv_file)}")
            else:
                print(f"CSV extraction to local staging failed for {os.path.basename(csv_file)}")
        
        print(f"Successfully processed {success_count}/{len(csv_files)} CSV files")
        return success_count == len(csv_files)
    except Exception as e:
        print(f"Error in extract_crm_data: {str(e)}")
        return False

def extract_erp_data(**kwargs):
    """Extract ERP data directly within the Airflow container"""
    try:
        print("Starting MySQL extraction process...")
        
        # MySQL connection details
        mysql_config = {
            "host": "mysql_source",  # Docker container name
            "user": "etl",
            "password": "etl",
            "database": "source_erp"
        }
        
        # Destination directory for extracted files (local staging)
        erp_extract_dir = os.path.join(extract_dir, "source_erp")
        
        print(f"Attempting to connect to MySQL at {mysql_config['host']} as {mysql_config['user']}...")
        
        # Try connection with different auth methods
        try:
            # First try with mysql_native_password
            conn = pymysql.connect(
                host=mysql_config["host"],
                user=mysql_config["user"],
                password=mysql_config["password"],
                database=mysql_config["database"],
                auth_plugin='mysql_native_password'
            )
            print("Connected to MySQL successfully with mysql_native_password")
        except Exception as e1:
            print(f"Failed to connect with mysql_native_password: {str(e1)}")
            try:
                # Then try without auth_plugin
                conn = pymysql.connect(
                    host=mysql_config["host"],
                    user=mysql_config["user"],
                    password=mysql_config["password"],
                    database=mysql_config["database"]
                )
                print("Connected to MySQL successfully without specifying auth_plugin")
            except Exception as e2:
                print(f"Failed to connect without auth_plugin: {str(e2)}")
                # Last resort - try localhost instead of service name
                try:
                    conn = pymysql.connect(
                        host="localhost",
                        user=mysql_config["user"],
                        password=mysql_config["password"],
                        database=mysql_config["database"]
                    )
                    print("Connected to MySQL successfully using localhost")
                except Exception as e3:
                    print(f"All connection attempts failed: {str(e3)}")
                    raise Exception("Could not establish MySQL connection after multiple attempts")
        
        # Get list of tables
        print("Getting list of tables...")
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        
        if not tables:
            print("Warning: No tables found in the database!")
        else:
            print(f"Found {len(tables)} tables: {', '.join(tables)}")
        
        # Create extract directory
        ensure_dir(erp_extract_dir)
        print(f"Using extract directory: {erp_extract_dir}")
        
        # Extract each table and load to HDFS
        success_count = 0
        for table in tables:
            print(f"Processing table: {table}")
            # Extract to local staging
            local_file = extract_mysql_table(conn, table, erp_extract_dir)
            
            if local_file:
                print(f"Table {table} extracted to local staging successfully: {local_file}")
                
                # Load to HDFS
                result = load_to_hdfs(local_file, hdfs_path_erp)
                if result:
                    print(f"Table {table} loaded to HDFS successfully")
                    success_count += 1
                else:
                    print(f"Failed to load table {table} to HDFS")
            else:
                print(f"Failed to extract table {table}")
        
        print(f"Successfully extracted and loaded {success_count}/{len(tables)} tables")
        
        # Close connection
        conn.close()
        print("MySQL connection closed")
        return success_count == len(tables) and len(tables) > 0
    except Exception as e:
        import traceback
        print(f"Error in extract_erp_data: {str(e)}")
        print(f"Exception traceback: {traceback.format_exc()}")
        return False

def check_hdfs_data(**kwargs):
    """Check if data was successfully loaded to HDFS"""
    try:
        print("Checking HDFS data availability...")
        
        # Create HDFS client
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        # Check if CRM path exists
        print("\nChecking CRM data...")
        try:
            crm_files = hdfs_client.list(hdfs_path_crm)
            print(f"CRM data in HDFS: {crm_files}")
        except Exception as e:
            print(f"Error or no data found in CRM path: {str(e)}")
            hdfs_client.makedirs(hdfs_path_crm)
            print(f"Created directory: {hdfs_path_crm}")
        
        # Check if ERP path exists
        print("\nChecking ERP data...")
        try:
            erp_files = hdfs_client.list(hdfs_path_erp)
            print(f"ERP data in HDFS: {erp_files}")
        except Exception as e:
            print(f"Error or no data found in ERP path: {str(e)}")
            hdfs_client.makedirs(hdfs_path_erp)
            print(f"Created directory: {hdfs_path_erp}")
        
        return True
    except Exception as e:
        print(f"Unexpected error checking HDFS: {str(e)}")
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