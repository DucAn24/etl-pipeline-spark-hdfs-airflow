
# ETL Pipeline Project

This project implements an end-to-end ETL pipeline using Apache Spark, Airflow, and Docker. It extracts data from MySQL sources, transforms data from CRM & ERP systems, creates a dimensional model, and loads the data into a PostgreSQL data warehouse.
## 🏗️ Data Architecture



## Project Overview

The pipeline is composed of several components:
- **Data Extraction:** Scripts in the `spark/app/extract` folder extract data from MySQL and CRM sources. Examples include [`e_source_erp.py`](e:\etl_pineline_project\spark\app\extract\e_source_erp.py) and [`e_source_crm.py`](e:\etl_pineline_project\spark\app\extract\e_source_crm.py).
- **Data Transformation:** Transformation scripts reside in the `spark/app/transform` folder. They transform data for different domains:
  - CRM data transformations (e.g., [`crm_cust_info.py`](e:\etl_pineline_project\spark\app\transform\crm_cust_info.py), [`crm_prd_info.py`](e:\etl_pineline_project\spark\app\transform\crm_prd_info.py), [`crm_sales_details.py`](e:\etl_pineline_project\spark\app\transform\crm_sales_details.py)).
  - ERP data transformations (e.g., [`erp_cust_info.py`](e:\etl_pineline_project\spark\app\transform\erp_cust_info.py), [`erp_prd_category.py`](e:\etl_pineline_project\spark\app\transform\erp_prd_category.py), [`erp_cust_loc.py`](e:\etl_pineline_project\spark\app\transform\erp_cust_loc.py)).
- **Dimensional Model & Load:** The dimensional model is built in [`dimensional_model.py`](e:\etl_pineline_project\spark\app\transform\dimensional_model.py) and data is loaded to the PostgreSQL data warehouse using [`load_dwh.py`](e:\etl_pineline_project\spark\app\load\load_dwh.py).

## Project Structure

- **airflow-dags/**  
  Contains DAG definitions to orchestrate extraction, transformation, and load tasks:
  - `etl_dag.py` – Main Airflow DAG for coordinating ETL tasks.
  - `to_dwh.py` – DAG for transforming data and loading it to the PostgreSQL warehouse.
  - `to_staging.py` – DAG for extracting and staging data to HDFS.

- **spark/**  
  Contains the Spark ETL application divided as follows:
  - `app/extract/` – Extraction scripts for CRM and ERP (e.g., `e_source_erp.py`, `e_source_crm.py`).
  - `app/transform/` – Transformation scripts for different data types (customer info, product info, sales details, etc.).
  - `app/load/` – Load scripts such as `load_dwh.py` that load data into PostgreSQL.
  - Each transformation script typically creates its own Spark session (see, for example, [`crm_cust_info.py`](e:\etl_pineline_project\spark\app\transform\crm_cust_info.py) and [`crm_prd_info.py`](e:\etl_pineline_project\spark\app\transform\crm_prd_info.py)).

- **docker/**  
  Contains the `docker-compose.yml` which defines the Docker configuration for Spark master, Spark workers, and other services.


## Prerequisites

Before running the project, ensure you have installed and configured:
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Apache Airflow](https://airflow.apache.org/) (or use the provided Docker configuration)
- [Apache Spark](https://spark.apache.org/)
- PostgreSQL database for the data warehouse
- MySQL database for the source ERP system

## Setup and Configuration

1. **Docker Setup:**  
   Use the Docker Compose file located at `docker/docker-compose.yml` to spin up the Spark cluster. Adjust environment variables as needed, especially the `SPARK_MASTER_URL` which is used in Airflow connections.

   **Command:**
   ```
   docker-compose up -d
   ```

2. **Airflow Configuration:**  
   The DAGs in the `airflow-dags/` folder are configured to run Spark jobs via `SparkSubmitOperator` and `BashOperator` commands. Ensure that your Airflow connections (e.g., `spark_default`) are properly configured to point to the Spark master.

3. **Database Connections:**  
   Verify that the MySQL and PostgreSQL connection details in the extraction and load scripts (e.g., within [`e_source_erp.py`](e:\etl_pineline_project\spark\app\extract\e_source_erp.py) and [`load_dwh.py`](e:\etl_pineline_project\spark\app\load\load_dwh.py)) match your environment.

## Running the Pipeline

There are multiple ways to execute the ETL pipeline:

- **Via Airflow:**  
  Deploy the DAGs and trigger the workflow from the Airflow UI. This will coordinate all extraction, transformation, and loading tasks.

- **Direct Execution using Docker:**  
  You can manually trigger individual tasks using Docker exec commands. For example, to run an extraction:
  ```
  docker exec -it python3 python /usr/local/spark/app/extract/e_source_crm.py
  ```

- **Manual Testing:**  
  Run the [`test-worker.py`](e:\etl_pineline_project\test-worker.py) script to verify that the Spark worker nodes are accessible:
  ```
  python test-worker.py
  ```
  

