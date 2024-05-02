import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('sales.db')

# Query the database and load into a DataFrame
df = pd.read_sql_query(query, conn)

# Close connection
conn.close()

# Filter DataFrame to exclude items with total quantity = 0
df = df[df['total_quantity'] > 0]

# Store DataFrame in a CSV file
df.to_csv('output_pandas.csv', sep=';', index=False)
