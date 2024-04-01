import dlt
from sqlalchemy import create_engine
import os

# Set environment variables for PostgreSQL connection
os.environ['FROM_DATABASE__DESTINATION__POSTGRES__CREDENTIALS__DATABASE'] = 'AdventureDB'
os.environ['FROM_DATABASE__DESTINATION__POSTGRES__CREDENTIALS__PASSWORD'] = '2512'
os.environ['FROM_DATABASE__DESTINATION__POSTGRES__CREDENTIALS__USERNAME'] = 'postgres'
os.environ['FROM_DATABASE__DESTINATION__POSTGRES__CREDENTIALS__HOST'] = 'localhost'

tables = {
    "Production": ["Product", "Productcategory", "Productsubcategory"],
    "sales": ["currency", "currencyrate", "customer", "salesorderdetail", "salesorderheader", "salesperson", "salesterritory", "store"],
}

# MSSQL connection string
mssql_connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=B2B00016\\SQLEXPRESS;"
    "DATABASE=AdventureWorks2019;"
    "trusted_connection=yes"
)

# Create MSSQL engine
engine = create_engine(mssql_connection_string)

for schema, tables in tables.items():
    for table in tables:
        with engine.connect() as conn:
            query = f"SELECT * FROM {schema}.{table}"
            result = conn.execution_options(yield_per=100).exec_driver_sql(query)
            rows = result.fetchall()

        # Initialize DLT pipeline
        pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination="postgres",
            dataset_name="stg",
        )

        # Run the pipeline to load data into PostgreSQL
        load_info = pipeline.run(map(lambda row: dict(row._mapping), rows), table_name=table, write_disposition="replace")

        print(load_info)
