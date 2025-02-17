"""
This file is expected to contain a Flask endpoint which takes ocr_data_id as input parameter to extract specific data from OCR data of a file.
We will use OpenAI LLM for this task.
We are expected to combine three OCR texts into one to create a "Consensus Document". This will be added after we add second OCR provider.

Architecture:

Our Data extraction process is divided in five layers:

"Runsheet -> extracted_data -> OCR_data -> Files -> Files in S3"

Here "Runsheets" and "Files in S3" are accesible to the user, everything else is abstracted away.

Files is the files table we use to maintain references to the original uploaded files
OCR_data is the raw text and confidence score of each OCR Block of a file. 
extracted_data is the LLM response for the runsheet prompts. This can contain the multiline response by the LLM.
Runsheet holds the specific information which will be useful for the Title Attorney


The code in this file is expected to provide following:
1. Flask Endpoint that is called from file_ocr.py file, which takes file_id and ocr_data_id as input.
2. Ignore Consensus Document as we only have one OCR in first milestone.
3. Use ChatGPT to prompt and extract data from files which have completed OCR
    - Use pyscopg2 to connect with postgresql database to query OCR data for a specific file 
    - Use OpenAI extract data from this OCR text
    - Use json files in ocr_data table to get confidence scores
4. Store the output from ChatGPT in json format in the extracted_data table.
        {
        "gpt_response": "<Multiline Response from ChatGPT>",
        "specific_data": "<Exact Value>",
        "confidence_score": "99.00"
        }
5. Get specific data from this table and store it in respective runsheet column.

Limitations:
1. Currently, this code is not expected to handle multiple OCR data. 

"""
import os
import json
import psycopg2
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# Function to get a database connection
def get_db_connection():
    try:
        return psycopg2.connect(Config.DATABASE_URL)
    except Exception as e:
        print(f"Error getting DB connection: {e}")
        return None

# Function to fetch OCR text from the database
def fetch_ocr_text(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT ocr_json_1 FROM ocr_data WHERE file_id = %s"

                try:
                    cur.execute(query, (int(file_id),))  # If file_id is an integer
                except ValueError:
                    cur.execute(query, (file_id,))  # If file_id is a string
                
                result = cur.fetchone()

                if not result:
                    return None, "OCR text not found"

                ocr_json = result[0]

                try:
                    return json.loads(ocr_json) if isinstance(ocr_json, str) else ocr_json, None
                except json.JSONDecodeError:
                    return None, "Invalid JSON format"
    except Exception as e:
        print(f"Error fetching OCR text: {e}")
        return None, str(e)



# API Endpoint to fetch OCR text (file_id passed in URL as part of the route)


# Flask App Factory
def create_app():
    app = Flask(__name__)
    @app.route("/api/v1/get_ocr_text/<file_id>", methods=["GET"])
    def get_ocr_text(file_id):
        try:
            ocr_text, error = fetch_ocr_text(file_id)

            if error:
                return jsonify({"error": error}), 404

            return jsonify({"message": "OCR text retrieved successfully", "ocr_text": ocr_text})

        except Exception as e:
            print(f"Error retrieving OCR text: {e}")
            return jsonify({"error": str(e)}), 500
    return app

# Run the Flask application
if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
