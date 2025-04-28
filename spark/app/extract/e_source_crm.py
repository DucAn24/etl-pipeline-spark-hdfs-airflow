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
    """Extract CRM data directly to HDFS from source CSVs"""
    try:
        # Source directory containing CSV files
        source_dir = "/usr/local/datasets/source_crm"
        
        # HDFS destination path
        hdfs_path_crm = "/raw/source_crm"
        
        # Create HDFS client
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        # Make sure the HDFS directory exists
        try:
            hdfs_client.makedirs(hdfs_path_crm)
            print(f"Ensured HDFS directory exists: {hdfs_path_crm}")
        except Exception as e:
            print(f"Note: {str(e)}")
        
        # Get all CSV files in the source directory
        csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {source_dir}")
            return False
        
        print(f"Found {len(csv_files)} CSV files to extract directly to HDFS")
        
        # Process each CSV file
        success_count = 0
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            hdfs_file = f"{hdfs_path_crm}/{filename}"
            
            print(f"Loading {csv_file} directly to {hdfs_file}...")
            
            try:
                # Upload the file directly to HDFS
                with open(csv_file, 'rb') as local_f:
                    hdfs_client.write(hdfs_file, local_f, overwrite=True)
                
                print(f"Successfully loaded {filename} to HDFS")
                success_count += 1
            except Exception as e:
                print(f"Error loading {filename} to HDFS: {str(e)}")
        
        print(f"Successfully processed {success_count}/{len(csv_files)} CSV files directly to HDFS")
        return success_count == len(csv_files)
    except Exception as e:
        print(f"Error in extract_crm_data: {str(e)}")
        return False

if __name__ == "__main__":
    # Call the extract function directly
    result = extract_crm_data()
    
    if result:
        print("CRM data extraction and loading to HDFS completed successfully")
    else:
        print("CRM data extraction or loading to HDFS failed")
        sys.exit(1)

    # docker exec -it python3 python /usr/local/spark/app/extract/e_source_crm.py