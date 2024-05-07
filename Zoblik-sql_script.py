import sqlite3
import csv

try:
    conn = sqlite3.connect('Data_Engineer_ETL_Assignment.db')
    c = conn.cursor()

    query = '''
            SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
            FROM Customers c
            JOIN Sales s ON c.customer_id = s.customer_id
            JOIN Orders o ON s.sales_id = o.sales_id
            JOIN Items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, i.item_name
            HAVING total_quantity > 0
            '''

    # Execute the query
    c.execute(query)

    # Fetch all results
    results = c.fetchall()

    # Store results in a CSV file
    with open('output_sql.csv', mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
        for row in results:
            writer.writerow(row)

except sqlite3.Error as e:
    print(f"SQLite error occurred: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        conn.close()
