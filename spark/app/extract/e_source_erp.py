import os
import sys
import mysql.connector
import csv
import subprocess
import traceback
from datetime import datetime
from hdfs import InsecureClient
import pymysql  # Adding PyMySQL for Airflow compatibility

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def extract_mysql_table(conn, table_name, output_dir):
    """
    Extract a MySQL table to a CSV file
    """
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

def extract_erp_data(**kwargs):
    """Extract ERP data directly from MySQL to HDFS"""
    try:
        print("Starting MySQL extraction process with direct loading to HDFS...")
        
        # MySQL connection details
        mysql_config = {
            "host": "mysql_source",  # Docker container name
            "user": "etl",
            "password": "etl",
            "database": "source_erp"
        }
        
        # HDFS destination path
        hdfs_path_erp = "/raw/source_erp"
        
        # Create HDFS client
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        # Make sure the HDFS directory exists
        try:
            hdfs_client.makedirs(hdfs_path_erp)
            print(f"Ensured HDFS directory exists: {hdfs_path_erp}")
        except Exception as e:
            print(f"Note: {str(e)}")
        
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
        
        # Extract each table and load directly to HDFS
        success_count = 0
        for table in tables:
            print(f"Processing table: {table}")
            
            try:
                # Get column names
                cursor = conn.cursor()
                cursor.execute(f"SHOW COLUMNS FROM {table}")
                columns = [column[0] for column in cursor.fetchall()]
                
                # Query data
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
                cursor.close()
                
                if not rows:
                    print(f"Warning: Table {table} has no data")
                    continue
                
                # Prepare CSV data in memory
                import io
                import csv
                
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(columns)  # Write header
                csv_writer.writerows(rows)    # Write data rows
                
                # Write directly to HDFS
                hdfs_file = f"{hdfs_path_erp}/{table}.csv"
                hdfs_client.write(hdfs_file, data=csv_buffer.getvalue().encode('utf-8'), overwrite=True)
                
                print(f"Successfully extracted {len(rows)} rows from {table} directly to HDFS: {hdfs_file}")
                success_count += 1
                
            except Exception as e:
                print(f"Error processing table {table}: {str(e)}")
        
        print(f"Successfully extracted and loaded {success_count}/{len(tables)} tables directly to HDFS")
        
        # Close connection
        conn.close()
        print("MySQL connection closed")
        return success_count == len(tables) and len(tables) > 0
    except Exception as e:
        print(f"Error in extract_erp_data: {str(e)}")
        print(f"Exception traceback: {traceback.format_exc()}")
        return False

def extract_all_tables(host, user, password, database, extract_dir, hdfs_path):
    """
    Extract all tables from a MySQL database and load to HDFS
    """
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # Get list of tables
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        
        # Extract each table and load to HDFS
        success_count = 0
        for table in tables:
            # Extract to local staging
            local_file = extract_mysql_table(conn, table, extract_dir)
            
            if local_file:
                print(f"Table {table} extracted to local staging successfully")
                
                # Load to HDFS
                result = load_to_hdfs(local_file, hdfs_path)
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
        return success_count == len(tables)
    except Exception as e:
        print(f"Error connecting to MySQL: {str(e)}")
        return False

if __name__ == "__main__":
    # MySQL connection details
    mysql_config = {
        "host": "mysql_source",  # Docker container name matches docker-compose.yml
        "user": "etl",           # Matches MYSQL_USER in docker-compose.yml
        "password": "etl",       # Matches MYSQL_PASSWORD in docker-compose.yml
        "database": "source_erp" # Matches MYSQL_DATABASE in docker-compose.yml
    }
    
    # Destination directory for extracted files (local staging)
    extract_dir = "/usr/local/spark/resources/data/"
    erp_extract_dir = os.path.join(extract_dir, "source_erp")
    
    # HDFS destination path
    hdfs_path = "/raw/source_erp"
    
    # Extract all tables and load to HDFS
    result = extract_all_tables(
        mysql_config["host"],
        mysql_config["user"],
        mysql_config["password"],
        mysql_config["database"],
        erp_extract_dir,
        hdfs_path
    )
    
    if result:
        print("MySQL extraction and loading to HDFS completed successfully")
    else:
        print("MySQL extraction or loading to HDFS failed")



# docker exec -it python3 python /usr/local/spark/app/extract/e_source_erp.py