import os
import sys
import glob
from hdfs import InsecureClient

def extract_crm_data(**kwargs):
    try:
        
        source_dir = "/opt/airflow/datasets/source_crm"
        
        hdfs_path_crm = "/raw/source_crm"
        
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        
        hdfs_client.makedirs(hdfs_path_crm)
        print(f"Ensured HDFS directory exists: {hdfs_path_crm}")

        csv_files = glob.glob(os.path.join(source_dir, "*.csv"))

        if not csv_files:
            print(f"No CSV files found in {source_dir}")
            return False

        print(f"Found {len(csv_files)} CSV files to extract to HDFS")

        success_count = 0
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            hdfs_file = f"{hdfs_path_crm}/{filename}"

            try:
                with open(csv_file, 'rb') as local_f:
                    hdfs_client.write(hdfs_file, local_f, overwrite=True)

                print(f"Successfully loaded {filename}")
                success_count += 1
            except Exception as e:
                print(f"Error loading {filename}: {str(e)}")

        print(f"Completed: {success_count}/{len(csv_files)} files processed successfully")
        return success_count == len(csv_files)
        
    except Exception as e:
        print(f"Error in extract_crm_data: {str(e)}")
        return False

if __name__ == "__main__":
    print("Starting CRM data extraction...")
    
    result = extract_crm_data()
    
    if result:
        print("CRM data extraction completed successfully")
    else:
        print("CRM data extraction failed")
        sys.exit(1)
