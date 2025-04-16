import os
import mysql.connector
import csv
import subprocess
from datetime import datetime

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
    """
    Load a local file to HDFS using Docker exec
    """
    try:
        filename = os.path.basename(local_file)
        hdfs_file = f"{hdfs_path}/{filename}"
        
        # First copy the file to the namenode container
        copy_cmd = ["docker", "cp", local_file, f"namenode:/tmp/{filename}"]
        subprocess.run(copy_cmd, check=True)
        
        # Create directory in HDFS (will not error if it already exists)
        mkdir_cmd = ["docker", "exec", "namenode", 
                    "hdfs", "dfs", "-mkdir", "-p", hdfs_path]
        subprocess.run(mkdir_cmd, check=True)

        # Then use docker exec to run the hdfs command
        hdfs_cmd = ["docker", "exec", "namenode", 
                   "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", hdfs_file]
        subprocess.run(hdfs_cmd, check=True)
        
        print(f"Successfully loaded {local_file} to {hdfs_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error loading to HDFS: {str(e)}")
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