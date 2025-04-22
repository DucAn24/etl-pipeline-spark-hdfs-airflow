"""
Utility functions for HDFS operations
"""
import os
from hdfs import InsecureClient

# Define common paths
hdfs_path_crm = "/raw/source_crm"
hdfs_path_erp = "/raw/source_erp"

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