import os
import shutil
import subprocess
import glob
from datetime import datetime

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

if __name__ == "__main__":
    # Source directory containing CSV files
    source_dir = "e:/P1/datasets/source_crm"
    
    # Destination directory for extracted files (local staging)
    extract_dir = "e:/P1/spark/resources/data/"
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