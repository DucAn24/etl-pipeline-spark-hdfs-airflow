import os
import sys
import csv
import io
import traceback
from hdfs import InsecureClient
import pymysql

MYSQL_CONFIG = {
    "host": "mysql_source",
    "user": "etl", 
    "password": "etl",
    "database": "source_erp"
}

HDFS_CONFIG = {
    "namenode_url": "http://namenode:9870",
    "user": "root",
    "destination_path": "/raw/source_erp"
}

def get_mysql_connection(config):
    connection_strategies = [

        lambda: pymysql.connect(
            host=config["host"],
            user=config["user"], 
            password=config["password"],
            database=config["database"],
            auth_plugin='mysql_native_password'
        ),

        lambda: pymysql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"], 
            database=config["database"]
        )
    ]
    
    for i, strategy in enumerate(connection_strategies, 1):
        try:
            conn = strategy()
            print(f"✓ Connected to MySQL (Strategy {i})")
            return conn
        except Exception as e:
            print(f"Connection strategy {i} failed: {str(e)}")
    
    raise Exception("Could not create MySQL connection")

def get_hdfs_client(config):
    try:
        client = InsecureClient(config["namenode_url"], user=config["user"])
        return client
    except Exception as e:
        print(f"Error creating HDFS client: {str(e)}")
        raise

def get_table_list(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
        
        if not tables:
            print("No tables found in the database")
        else:
            print(f"Found {len(tables)} tables: {', '.join(tables)}")
        
        return tables
    except Exception as e:
        print(f"Error getting table list: {str(e)}")
        raise

def extract_table_to_csv_string(connection, table_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [column[0] for column in cursor.fetchall()]
            
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            
            if not rows:
                print(f"Warning: Table {table_name} has no data")
                return None, 0
            
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerow(columns)  
            csv_writer.writerows(rows)   
            
            return csv_buffer.getvalue(), len(rows)
    
    except Exception as e:
        print(f"Error extracting table {table_name}: {str(e)}")
        return None, 0

def upload_to_hdfs(hdfs_client, csv_data, hdfs_path, filename):
    try:
        hdfs_client.makedirs(os.path.dirname(hdfs_path))

        hdfs_client.write(hdfs_path, data=csv_data.encode('utf-8'), overwrite=True)
        print(f"Successfully uploaded {filename}")
        return True
        
    except Exception as e:
        print(f"✗ Error uploading {filename}: {str(e)}")
        return False

def extract_erp_data(**kwargs):
    mysql_conn = None
    try:

        mysql_conn = get_mysql_connection(MYSQL_CONFIG)
        hdfs_client = get_hdfs_client(HDFS_CONFIG)
        
        tables = get_table_list(mysql_conn)
        
        if not tables:
            print("No tables to process")
            return False
        
        success_count = 0
        for table_name in tables:
            print(f"Processing table: {table_name}")
            
            # Extract table data
            csv_data, row_count = extract_table_to_csv_string(mysql_conn, table_name)
            
            if csv_data is None:
                print(f"Skipping table {table_name} (no data or error)")
                continue
            
            hdfs_path = f"{HDFS_CONFIG['destination_path']}/{table_name}.csv"
            
            if upload_to_hdfs(hdfs_client, csv_data, hdfs_path, table_name):
                print(f"Processed {table_name}: {row_count} rows")
                success_count += 1
        
        print(f"Completed: {success_count}/{len(tables)} tables processed successfully")
        return success_count == len(tables) and len(tables) > 0
        
    except Exception as e:
        print(f"Error in extract_erp_data: {str(e)}")
        print(f"Exception traceback: {traceback.format_exc()}")
        return False
    finally:
        if mysql_conn:
            mysql_conn.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    
    success = extract_erp_data()
    
    if success:
        print("ERP data extraction completed successfully")
    else:
        print("ERP data extraction failed")
        sys.exit(1)
