import psycopg2
from psycopg2 import sql
import psycopg2.extras
from datetime import datetime as dt  # Renaming datetime to dt

# Function to connect to PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="AdventureDB",
            user="postgres",
            password="2512",
            host="localhost",
            port=5432
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to database:", e)
        return None

# Function to split table by year
def split_table_by_year():
    try:
        conn = connect_to_db()
        if conn is None:
            return

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)  # Use DictCursor

        # Query to select data from original tables and join them
        select_query = """
            SELECT sd.sales_order_id, sd.sales_order_detail_id, sd.carrier_tracking_number,
                   sd.order_qty, sd.product_id, sd.special_offer_id, sd.unit_price,
                   sd.unit_price_discount, sd.line_total, sd.rowguid, sd.modified_date, sh.order_date,
                   sd._dlt_load_id, sd._dlt_id
            FROM stg.salesorderdetail sd
            JOIN stg.salesorderheader sh ON sd.sales_order_id = sh.sales_order_id;
        """

        cur.execute(select_query)
        rows = cur.fetchall()

        # Iterate through rows, split by year, and insert into respective tables
        for row in rows:
            order_date = row['order_date']  # Accessing column by name as a datetime object
            year = order_date.year  # Extract the year
            
            # Insert "nullvaluehere" if _dlt_id is NULL, else use _dlt_id value
            _dlt_id = row['_dlt_id'] if row['_dlt_id'] is not None else "nullvaluehere"

            # Insert 1711998113.0342977 if _dlt_load_id is NULL, else use _dlt_load_id value
            _dlt_load_id = row['_dlt_load_id'] if row['_dlt_load_id'] is not None else 1711998113.0342977

            # Create table for the year if it doesn't exist
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    LIKE stg.salesorderdetail INCLUDING ALL
                );
            """).format(sql.Identifier(f"sales_order_items_{year}"))
            cur.execute(create_table_query)

            # Insert row into the corresponding year's table
            insert_query = sql.SQL("""
                INSERT INTO {} ({}, _dlt_load_id, _dlt_id) VALUES ({}, %s, %s)
            """).format(
                sql.Identifier(f"sales_order_items_{year}"), 
                sql.SQL(', ').join(map(sql.Identifier, [key for key in row.keys() if key not in ('order_date', '_dlt_load_id', '_dlt_id')])),  # Exclude order_date, _dlt_load_id, and _dlt_id
                sql.SQL(', ').join(sql.Placeholder() * (len(row) - 3))  # Exclude order_date, _dlt_load_id, and _dlt_id
            )

            cur.execute(insert_query, list(row.values())[:-3] + [_dlt_load_id, _dlt_id])  # Insert row values without order_date, _dlt_load_id, and _dlt_id

        conn.commit()
        print("Table split operation completed successfully.")

    except psycopg2.Error as e:
        conn.rollback()
        print("Error:", e)

    finally:
        if conn is not None:
            cur.close()
            conn.close()

if __name__ == "__main__":
    split_table_by_year()
