import os
import pandas as pd
from sqlalchemy import create_engine, text

def create_postgresql_tables(engine):
    """Create dimension and fact tables in PostgreSQL"""
    # Create dimension tables
    engine.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_key INTEGER PRIMARY KEY,
            customer_id VARCHAR(50),
            customer_number VARCHAR(50),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            country VARCHAR(50),
            marital_status VARCHAR(50),
            gender VARCHAR(10),
            birthdate DATE,
            create_date DATE
        );
        
        CREATE TABLE IF NOT EXISTS dim_products (
            product_key INTEGER PRIMARY KEY,
            product_id VARCHAR(50),
            product_number VARCHAR(100),
            product_name VARCHAR(200),
            category_id VARCHAR(50),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            maintenance VARCHAR(100),
            cost NUMERIC(10,2),
            product_line VARCHAR(50),
            start_date DATE
        );
        
        CREATE TABLE IF NOT EXISTS fact_sales (
            id SERIAL PRIMARY KEY,
            order_number VARCHAR(50),
            product_key INTEGER,
            customer_key INTEGER,
            order_date DATE,
            shipping_date DATE,
            due_date DATE,
            sales_amount NUMERIC(10,2),
            quantity INTEGER,
            price NUMERIC(10,2),
            FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
            FOREIGN KEY (product_key) REFERENCES dim_products(product_key)
        );
    """))

def load_csv_to_postgres(csv_path, table_name, engine, chunksize=10000):
    """Load CSV file to PostgreSQL table"""
    print(f"Loading {csv_path} to {table_name}...")
    
    try:
        # Read and load in chunks to handle large files
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
        
        # Count rows in table
        result = engine.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.fetchone()[0]
        print(f"Loaded {count} rows into {table_name}")
        return True
    except Exception as e:
        print(f"Error loading {csv_path} to {table_name}: {str(e)}")
        return False

def main():
    """Load dimensional model into PostgreSQL DWH"""
    # Connection parameters - replace with your actual connection details
    db_params = {
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': '5432',
        'database': 'data_warehouse'
    }
    
    # Create SQLAlchemy engine
    engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@"
                          f"{db_params['host']}:{db_params['port']}/{db_params['database']}")
    
    try:
        # Create tables
        create_postgresql_tables(engine)
        
        # Path to dimensional model files
        base_path = "output"  # Path where the Spark job wrote the CSV files
        
        # Load dimension tables first
        load_csv_to_postgres(
            os.path.join(base_path, "dimensions/dim_customers/part-*.csv"),
            "dim_customers",
            engine
        )
        
        load_csv_to_postgres(
            os.path.join(base_path, "dimensions/dim_products/part-*.csv"),
            "dim_products",
            engine
        )
        
        # Load fact table last (after dimensions for foreign key constraints)
        load_csv_to_postgres(
            os.path.join(base_path, "facts/fact_sales/part-*.csv"),
            "fact_sales",
            engine
        )
        
        print("Data warehouse loading completed successfully")
        
    except Exception as e:
        print(f"Error loading to PostgreSQL: {str(e)}")

if __name__ == "__main__":
    main()