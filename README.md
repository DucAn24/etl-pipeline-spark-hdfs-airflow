# ETL Pipeline Project

This project implements an end-to-end ETL pipeline using Apache Spark, Airflow, and Docker. It extracts data from MySQL sources, transforms data from CRM & ERP systems, creates a dimensional model, and loads the data into a PostgreSQL data warehouse.
## 🏗️ Data Architecture
![data_architecture](https://github.com/user-attachments/assets/6f41f133-e586-4d12-9f54-ceb23476f7ab)



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
  - `transform.py` – DAG for transforming data and loading it to the PostgreSQL warehouse.
  - `to_staging.py` – DAG for extracting and staging data to HDFS.

- **spark/**  
  Contains the Spark ETL application divided as follows:
  - `app/extract/` – Extraction scripts for CRM and ERP (e.g., `e_source_erp.py`, `e_source_crm.py`).
  - `app/transform/` – Transformation scripts for different data types (customer info, product info, sales details, etc.).
  - `app/load/` – Load scripts such as `load_dwh.py` that load data into PostgreSQL.
  - `app/utils/` – Utility scripts for common Spark and HDFS operations (e.g., `spark_utils.py`, `hdfs_utils.py`).
  - Each transformation script typically creates its own Spark session (see, for example, [`crm_cust_info.py`](e:\etl_pineline_project\spark\app\transform\crm_cust_info.py) and [`crm_prd_info.py`](e:\etl_pineline_project\spark\app\transform\crm_prd_info.py)).

- **docker/**  
  Contains the `docker-compose.yml` which defines the Docker configuration for Spark master, Spark workers, and other services.


## Setup and Configuration

1. **Docker Setup:**  
   Use the Docker Compose file located at `docker/docker-compose.yml` to spin up the Spark cluster. Adjust environment variables as needed, especially the `SPARK_MASTER_URL` which is used in Airflow connections.

   **Command:**
   ```
   docker-compose up -d
   ```

2. **HDFS Directory Setup:**
   Before running any DAGs that interact with HDFS, create the necessary directories and set permissions:
   ```bash
   docker exec -it namenode hdfs dfs -mkdir -p /raw /transform/source_crm /transform/source_erp /transform/dim /transform/fact
   docker exec -it namenode hdfs dfs -chmod -R 777 /raw /transform
   ```

3. **Airflow Configuration:**  
   The DAGs in the `airflow-dags/` folder are configured to run Spark jobs via `SparkSubmitOperator` and `BashOperator` commands. Ensure that your Airflow connections (e.g., `spark_default`) are properly configured to point to the Spark master.

4. **Database Connections:**  
   Verify that the MySQL and PostgreSQL connection details in the extraction and load scripts (e.g., within [`e_source_erp.py`](e:\etl_pineline_project\spark\app\extract\e_source_erp.py) and [`load_dwh.py`](e:\etl_pineline_project\spark\app\load\load_dwh.py)) match your environment.

## Tech Stack

- **Orchestration:** Apache Airflow
- **Data Processing:** Apache Spark (PySpark)
- **Storage:** HDFS (Hadoop Distributed File System)
- **Databases:** MySQL (Source), PostgreSQL (Data Warehouse)
- **Containerization:** Docker, Docker Compose

## Running the Pipeline

There are multiple ways to execute the ETL pipeline:

- **Via Airflow:**  
  Deploy the DAGs and trigger the workflow from the Airflow UI. This will coordinate all extraction, transformation, and loading tasks.

- **Direct Execution using Docker:**  
  You can manually trigger individual tasks using Docker exec commands. For example, to run an extraction:
  ```
  docker exec -it python3 python /usr/local/spark/app/extract/e_source_crm.py
  ```

## Airflow DAG

The main ETL workflow orchestrated by Airflow (`etl_dag.py`) looks like this:

![Screenshot 2025-04-28 010129](https://github.com/user-attachments/assets/8d77e49e-9f57-4aae-88c5-556a4338d8b2)


