import pandas as pd
import sqlite3

try:
    # Define the SQL query
    query = '''
            SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
            FROM customers c
            JOIN Sales s ON c.customer_id = s.customer_id
            JOIN Orders o ON s.sales_id = o.sales_id
            JOIN Items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, i.item_name
            HAVING total_quantity > 0
            '''

    # Connect to the SQLite database
    conn = sqlite3.connect('Data_Engineer_ETL_Assignment.db')

    # Query the database and load into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Close connection
    conn.close()

    # Filter DataFrame to exclude items with total quantity = 0
    df = df[df['total_quantity'] > 0]

    # Store DataFrame in a CSV file
    df.to_csv('output_pandas.csv', sep=';', index=False)

except Exception as e:
    print(f"An error occurred: {e}")
