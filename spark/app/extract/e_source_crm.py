import os
import sys
import shutil
import subprocess
import glob
from datetime import datetime
from hdfs import InsecureClient

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def extract_csv_to_processing_area(source_file, destination_dir, filename=None):
    """
    Copy a CSV file from source to destination directory
    """
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

def extract_crm_data(**kwargs):
    """Extract CRM data directly within the Airflow container"""
    try:
        # Source directory containing CSV files
        source_dir = "/usr/local/datasets/source_crm"
        
        # Destination directory for extracted files (local staging)
        extract_dir = "/tmp/airflow_data/"   # Thư mục /tmp thường có quyền ghi
        crm_extract_dir = os.path.join(extract_dir, "source_crm")
        
        # HDFS destination path
        hdfs_path_crm = "/raw/source_crm"
        
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

if __name__ == "__main__":
    # Source directory containing CSV files
    source_dir = "/usr/local/datasets/source_crm"
    
    # Destination directory for extracted files (local staging)
    extract_dir = "/usr/local/spark/resources/data"
    crm_extract_dir = os.path.join(extract_dir, "source_crm")
    
    # HDFS destination path
    hdfs_path = "/raw/source_crm"
    
    # Get all CSV files in the source directory
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {source_dir}")
        exit(1)
    
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
            result = load_to_hdfs(local_file, hdfs_path)
            if result:
                print(f"CSV loading to HDFS completed successfully for {os.path.basename(csv_file)}")
                success_count += 1
            else:
                print(f"CSV loading to HDFS failed for {os.path.basename(csv_file)}")
        else:
            print(f"CSV extraction to local staging failed for {os.path.basename(csv_file)}")
    
    print(f"Successfully processed {success_count}/{len(csv_files)} CSV files")

    # docker exec -it python3 python /usr/local/spark/app/extract/e_source_crm.py