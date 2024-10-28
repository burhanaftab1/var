#!/usr/bin/python3
from flask import Flask, request, jsonify
import json
import pymysql
import pandas as pd
import os

app = Flask(__name__)

# Test route
@app.route('/')
def hello():
    return 'Hello!'

# Route to process JSON data
@app.route('/', methods=['POST'])
def process_json():

    # Function to insert variables into MySQL table
    def insert_variables_into_table(data):
        try:
            # Connect to the MySQL database
            db = pymysql.connect(
                host='your-host-url',
                user='dbmasteruser',
                password='your-password',
                database='dbmaster'
            )

            # Create a cursor object
            cur = db.cursor()

            # Insert data row by row into MySQL
            for i in range(len(data['Date'])):
                insert_query = """INSERT INTO created_data_test_upload
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                record = (
                    data['Date'][str(i)],
                    data['Headset'][str(i)],
                    data['Location'][str(i)],
                    data['VrVersion'][str(i)],
                    data['TenseRelaxedPre'][str(i)],
                    data['TenseRelaxedPost'][str(i)],
                    data['AnxiousCalmPre'][str(i)],
                    data['AnxiousCalmPost'][str(i)],
                    data['DispiritedEmpoweredPre'][str(i)],
                    data['DispiritedEmpoweredPost'][str(i)]
                )
                cur.execute(insert_query, record)

            db.commit()  # Commit changes
            cur.close()  # Close cursor
            db.close()  # Close database connection
            return "Data inserted successfully"

        except Exception as e:
            return str(e)

    # Function to write data to CSV file
    def save_data_to_csv(data):
        # Convert the JSON data into a pandas DataFrame
        df = pd.DataFrame(data)

        # Define the file path
        file_path = 'output_data.csv'

        # If the file does not exist, create it; otherwise, append the new data
        if not os.path.isfile(file_path):
            df.to_csv(file_path, index=False)  # Create file with headers
            print(f"CSV created at: {file_path}")
        else:
            df.to_csv(file_path, mode='a', header=False, index=False)  # Append without headers
            print(f"Data appended to CSV at: {file_path}")

    # Parse the JSON data from the POST request
    data = json.loads(request.data)

    # Insert data into MySQL
    db_result = insert_variables_into_table(data)

    # Save data to CSV file
    save_data_to_csv(data)

    return jsonify({"status": "success", "message": db_result}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
