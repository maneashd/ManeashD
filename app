from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

# Database connection details
DB_CONFIG = {
    "dbname": "your_db_name",
    "user": "your_user",
    "password": "your_password",
    "host": "your_host",
    "port": 5432
}

@app.route('/')
def index():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch data and column names
        query = "SELECT col1, col2, col3, col4, col5, spark_output FROM your_table"  # Replace with your query
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]  # Extract column names

        # Close connection
        cursor.close()
        conn.close()

        # Pass data and column names to the template
        return render_template('output.html', column_names=column_names, rows=rows)

    except Exception as e:
        return f"An error occurred: {e}"

if __name__ == '__main__':
    app.run(debug=True)
