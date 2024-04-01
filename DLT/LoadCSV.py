import psycopg2
import os
import random
from decimal import Decimal

# Database connection string
connection_string = 'postgresql://postgres:2512@localhost:5432/AdventureDB'

# Establish a connection to the database
try:
    conn = psycopg2.connect(connection_string)
except Exception as e:
    print("Error: Unable to connect to the database.")
    print(e)
    exit()

# Create a cursor object
try:
    cur = conn.cursor()
except Exception as e:
    print("Error: Unable to create a cursor.")
    print(e)
    conn.close()
    exit()

# Execute your SQL query
try:
    cur.execute("""SELECT sales_person_id, ROUND(AVG(totalsales), 2) AS Average_sales
                   FROM (SELECT sales_person_id,
                                SUM(sub_total) AS totalsales,
                                EXTRACT(YEAR FROM order_date) AS Years
                         FROM stg.salesorderheader
                         GROUP BY sales_person_id, EXTRACT(YEAR FROM order_date)) AS subquery
                   GROUP BY sales_person_id""")
except Exception as e:
    print("Error: Unable to execute the SQL query.")
    print(e)
    cur.close()
    conn.close()
    exit()

# Fetch all rows from the result set
try:
    rows = cur.fetchall()
except Exception as e:
    print("Error: Unable to fetch rows from the result set.")
    print(e)
    cur.close()
    conn.close()
    exit()

# Define the folder path for the CSV file
folder_path = "C:\\sakila_dbt_course\\sakila_dbt\\seeds"

# Define the path for the CSV file
csv_file_path = os.path.join(folder_path, "target.csv")

# Write the data to a CSV file
try:
    with open(csv_file_path, "w") as csv_file:
        # Write the header row if needed
        csv_file.write("SalesPersonID, AverageSales, Target\n")
        # Write the data rows
        for row in rows:
            sales_person_id, average_sales = row
            random_increase = Decimal(random.uniform(0.10, 0.15))
            target = average_sales * (1 + random_increase)  # Target with random increase
            # Round the target value to two decimal places
            target = round(target, 2)
            csv_file.write(f"{sales_person_id}, {average_sales}, {target}\n")
except Exception as e:
    print("Error: Unable to write data to the CSV file.")
    print(e)
    cur.close()
    conn.close()
    exit()

# Close the cursor and the connection
try:
    cur.close()
    conn.close()
except Exception as e:
    print("Error: Unable to close the cursor or the connection.")
    print(e)
    exit()
