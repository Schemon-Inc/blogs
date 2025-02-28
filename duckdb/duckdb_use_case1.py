"""
blog: https://schemon.io/duckdb-use-case1-processing-complex-json-data/
"""

# ===========================
# Install the required packages in a temporary directory
# to avoid conflicts with the existing packages
# and to avoid modifying the system-wide packages.
# This script is tested with Python 3.8.10 on Ubuntu 20.04.
# ===========================
import subprocess
import sys
import tempfile

temp_dir = tempfile.mkdtemp()

subprocess.check_call(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-cache-dir",
        "--target",
        temp_dir,
        "duckdb==1.2.0",
        "pyodbc==5.1.0",
    ]
)

sys.path.insert(0, temp_dir)

import duckdb
import pyodbc
import pandas as pd

print(f"DuckDB Version: {duckdb.__version__}")
print(f"pyodbc Version: {pyodbc.version}")


# ===========================
# Configuration Variables
# ===========================
SERVER = "myserver.database.windows.net"
DATABASE = "mydb"
USERNAME = "user"
PASSWORD = "password"
DRIVER = "{ODBC Driver 17 for SQL Server}"  # check python version and install the appropriate driver

# ===========================
# Step 1: Create a sample JSON data to mimic vendor API response
# ===========================
sample_json = {
    "customer_id": 123,
    "orders": [
        {
            "order_id": "A001",
            "items": [
                {"product_id": "P1001", "quantity": 2},
                {"product_id": "P1002", "quantity": 1},
            ],
        },
        {
            "order_id": "A002",
            "items": [
                {"product_id": "P1003", "quantity": 5},
                {"product_id": "P1004", "quantity": 3},
            ],
        },
    ],
}

# ===========================
# Step 2: Convert JSON to DataFrame format
# It adds a new root level key "json_data_col" as only column to the DataFrame for demonstration purposes
# In real-world scenarios, json_normalize() will parse the possible columns automatically
# ===========================
json_df = pd.json_normalize({"json_data_col": sample_json}, max_level=0)

# ===========================
# Step 3: Initialize DuckDB and register JSON Data as json_table
# ===========================
conn = duckdb.connect(database=":memory:")
conn.register("json_table", json_df)

# ===========================
# Step 4: Flattening and Exploding JSON Using DuckDB
# ===========================
query = """
SELECT 
    json_data_col.customer_id as customer_id
    ,order_exploded.unnest.order_id as order_id
    ,item_exploded.unnest.product_id as product_id
    ,item_exploded.unnest.quantity as quantity
FROM json_table,
LATERAL UNNEST(json_data_col.orders) AS order_exploded,
LATERAL UNNEST(order_exploded.unnest.items) AS item_exploded
"""

result_df = conn.execute(query).fetchdf()
print(result_df)

# ===========================
# Step 5: Load Data into SQL Server Staging Table
# ===========================

# Connect to SQL Server
conn_str = (
    f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
)
sql_conn = pyodbc.connect(conn_str)
cursor = sql_conn.cursor()

# Ensure staging table exists
cursor.execute(
    """
IF OBJECT_ID('dbo.stg_sale', 'U') IS NULL
CREATE TABLE dbo.stg_sale (
    customer_id INT,
    order_id NVARCHAR(50),
    product_id NVARCHAR(50),
    quantity INT
);
"""
)
sql_conn.commit()

# Truncate the staging table before loading
cursor.execute("TRUNCATE TABLE dbo.stg_sale")
sql_conn.commit()

# Insert data into staging table
for _, row in result_df.iterrows():
    cursor.execute(
        """
        INSERT INTO dbo.stg_sale (customer_id, order_id, product_id, quantity)
        VALUES (?, ?, ?, ?)
    """,
        row["customer_id"],
        row["order_id"],
        row["product_id"],
        row["quantity"],
    )

sql_conn.commit()

# ===========================
# Step 6: Process Data into Final Table using Stored Procedure
# ===========================

# Ensure final table exists
cursor.execute(
    """
IF OBJECT_ID('dbo.sale', 'U') IS NULL
CREATE TABLE dbo.sale (
    customer_id INT NOT NULL,
    order_id NVARCHAR(50) NOT NULL,
    product_id NVARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    PRIMARY KEY (customer_id, order_id, product_id)
);
"""
)
sql_conn.commit()

# Ensure the stored procedure exists
cursor.execute(
    """
IF OBJECT_ID('dbo.MergeSalesData', 'P') IS NULL
EXEC('
    CREATE PROCEDURE dbo.MergeSalesData AS
    BEGIN
        MERGE INTO dbo.sale AS target
        USING dbo.stg_sale AS source
        ON target.customer_id = source.customer_id 
           AND target.order_id = source.order_id 
           AND target.product_id = source.product_id
        WHEN MATCHED THEN 
            UPDATE SET target.quantity = source.quantity
        WHEN NOT MATCHED THEN 
            INSERT (customer_id, order_id, product_id, quantity)
            VALUES (source.customer_id, source.order_id, source.product_id, source.quantity);
    END;')
"""
)
sql_conn.commit()

# Execute the stored procedure
cursor.execute("EXEC dbo.MergeSalesData")
sql_conn.commit()

# Close the connections
cursor.close()
sql_conn.close()

print("Data processing completed successfully.")
