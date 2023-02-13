import pyodbc
import pandas as pd

# Connect to the database using the ODBC driver
conn = pyodbc.connect("DSN=proprietary_db;UID=user;PWD=password")
cursor = conn.cursor()

# Define the one-hour window for data extraction
start_time = "2020-01-01 00:00:00"
end_time = "2020-01-01 00:59:59"

# Define the SQL query to extract data within the specified time window
query = f"SELECT * FROM sensors WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'"

# Execute the query and store the result in a pandas DataFrame
df = pd.read_sql(query, conn)

# Write the extracted data to multiple Parquet files
file_name = f"{start_time.replace(':', '-')}_{end_time.replace(':', '-')}.parquet"
df.to_parquet(file_name)

# Close the database connection
cursor.close()
conn.close()
