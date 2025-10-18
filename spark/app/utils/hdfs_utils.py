from hdfs import InsecureClient

# Common HDFS paths
HDFS_PATHS = {
    "raw_crm": "/raw/source_crm",
    "raw_erp": "/raw/source_erp",
    "transform_crm": "/transform/source_crm",
    "transform_erp": "/transform/source_erp",
    "dim": "/transform/dim",
    "fact": "/transform/fact"
}

def get_hdfs_client(namenode_url='http://namenode:9870', user='root'):
    try:
        return InsecureClient(namenode_url, user=user)
    except Exception as e:
        print(f"Error creating HDFS client: {str(e)}")
        raise

def ensure_hdfs_directory(hdfs_client, path):
    try:
        hdfs_client.makedirs(path)
        return True
    except Exception as e:
        print(f"Error creating HDFS directory {path}: {str(e)}")
        return False

def check_hdfs_data(**kwargs):
    try:
        print("Checking HDFS data availability...")
        
        hdfs_client = get_hdfs_client()
        
        print("\nChecking CRM data...")
        try:
            crm_files = hdfs_client.list(HDFS_PATHS["raw_crm"])
            print(f"CRM data found: {crm_files}")
        except Exception as e:
            print(f"CRM data not found: {str(e)}")
            ensure_hdfs_directory(hdfs_client, HDFS_PATHS["raw_crm"])
            print(f"Created directory: {HDFS_PATHS['raw_crm']}")
        

        print("\nChecking ERP data...")
        try:
            erp_files = hdfs_client.list(HDFS_PATHS["raw_erp"])
            print(f"ERP data found: {erp_files}")
        except Exception as e:
            print(f"ERP data not found: {str(e)}")
            ensure_hdfs_directory(hdfs_client, HDFS_PATHS["raw_erp"])
            print(f"Created directory: {HDFS_PATHS['raw_erp']}")
        
        return True
    except Exception as e:
        print(f"Error checking HDFS: {str(e)}")
        return False