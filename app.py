"""
This file is expected to contain all the code related to Batch File OCR handling.
As per the development plan, we are expecting to add multiple OCR Providers to increase our coverage of document processing.
During the first milestone phase, we are specifically working with Google's Document AI.

The code in this file is expected to provide the following:

A Flask endpoint that takes project_id as input.
Fetch all files under the given project_id where OCR processing is incomplete.
Download all these files from their respective S3 URLs concurrently.
Perform OCR processing on these files using Google’s Document AI.
Store the extracted OCR text in the database table OCR_data (columns: ocr_text_1 and ocr_json_1).
Along with OCR Text data, extract the JSON with OCR confidence from Document AI and store it in ocr_json_1.
When OCR processing is complete, update the ocr_status of all processed files to "Completed".
Limitations:

Currently working with only one OCR provider.
Document AI has a file size limit of 20 MB for single-file processing.
Processing is limited by API rate limits of Google Document AI.
To-Do:

Add a second OCR Provider: Amazon Textract or Anthropic.
Switch to Batch Processing Mode on Document AI to disable the file size limit.
API Endpoint:
https://host:port/api/v1/batch_ocr/:project_id

Response:

Processing:
{
  "status": "processing",
  "project_id": "123"
}

Completed:
{
  "status": "completed",
  "project_id": "123"
}


Failed:
{
  "status": "failed",
  "project_id": "123"
}


Libraries Used:

Flask
psycopg2
dotenv
requests
google.cloud
json
os
concurrent.futures (for parallel processing)


The .env file is expected to contain the following environment variables:

GOOGLE_APPLICATION_CREDENTIALS=
CREDENTIALS_PATH=

DB_NAME=
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=

PROJECT_ID = 
LOCATION = 
PROCESSOR_ID = 

"""







import threading
from flask_cors import CORS
import config
import os
import psycopg2
import psycopg2.extras
import requests
import json
import PyPDF2
from PyPDF2 import PdfReader, PdfWriter
from PyPDF2.errors import PdfReadError
from flask import Flask, jsonify, request
from google.cloud import documentai_v1 as documentai
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
from extract_data import *
from db_operations.db import *
from aws_textract_ocr import *
from document_ai_ocr import *


# Initialize Flask App
app = Flask(__name__)
CORS(app)


for handler in app.logger.handlers[:]:
    app.logger.removeHandler(handler)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.INFO)  # Ensure INFO logs are captured
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)


# Establish a database connection
connection = get_db_connection() 


# This function saves the project ID and file IDs to a variable and prints the value.
def save_project_files_to_variable(project_id, user_id, files):
    """Save project ID and file IDs to a variable and print the value."""

    data = {
    "project_id": project_id,
    "file_ids": [
        {
            "file_id": file[0], 
            "user_id": file[1],
            "project_id": file[2],
            "file_name": file[3],
            "s3_url": file[4], 
            "s3_file_key": urllib.parse.unquote(urllib.parse.urlparse(file[4]).path.lstrip('/')),
            "ocr_status": file[5]
        } 
        for file in files
    ]
    }
    # print(json.dumps(data, indent=4))  # Print the data in a readable format
    logging.info(f"Project ID: {project_id}, User ID: {user_id}, Files: {json.dumps(data, indent=4)}")

    return data




# Get files which not completed ocr by project ID from the database and save them to a JSON file
def get_files_by_project(project_id): 
    """Fetch all file IDs for a given project."""
    
    # NOTE: ocr_status can be: 
    # Processing: The default status after file upload. File is being processed for OCR with Google Document AI
    #           - ALTER TABLE public.files ALTER COLUMN ocr_status SET DEFAULT 'Processing';
    # Extracting: OCR is complete and OpenAI Extraction is in progress
    # Completed: Runsheet is inserted for this file.


    files = select_file_by_projectid(connection,project_id)
    print("Get files which not completed ocr by project ID")
    # print(files)
    print("Files fetched:", files)
    connection.commit()

    if files:
        for file in files:
            print("File:", file)
        if all(len(file) >= 6 for file in files):
            user_id = files[0][1]
            print("Printed project ID, file IDs, s3_urls, and s3_file_keys to the terminal")
            data = save_project_files_to_variable(project_id, user_id, files)
            return data
        else:
            print("Some files do not have the expected structure.")
            return None
    else:
        print("No files found for this project.")
        return None




# Download files concurrently and save them to a folder
def get_single_file_by_file_id(file_id): 
    files = select_file_by_fileid(connection,file_id)
    connection.close()
    print(files)

    # Save project ID and file IDs to a JSON file
    if files:
        user_id = files[0][1]
        save_project_files_to_variable(file_id, user_id, files)

    return files





# This function inserts or updates OCR data for multiple files in the database and updates their OCR status to 'Completed'.
def save_and_update_ocr_data_batch(project_id, all_extracted_data,results):
    cur = connection.cursor()
    
    try:
        new_records = []
        for data in all_extracted_data:
            # extracted_data = data['extracted_data']
            extracted_data = data.get('extracted_data', {})
            if isinstance(extracted_data, list):
                extracted_data = {"text": "", "confidence_scores": extracted_data}
            new_records.append(
                (data['file_id'], project_id, json.dumps(extracted_data), extracted_data.get('text', '').replace("\n", " "))
        )
        

        new_records_1 = []
        for data_1 in results:
            # extracted_data = data['extracted_data']
            extracted_data_1 = data_1.get('extracted_data', {})
            if isinstance(extracted_data_1, list):
                extracted_data_1 = {"text": "", "confidence_scores": extracted_data_1}
            new_records_1.append(
                (data_1['file_id'], project_id, json.dumps(extracted_data_1), extracted_data_1.get('text', '').replace("\n", " "))
        )

        insert_or_update_ocr_data(connection, new_records)
        connection.commit()

        insert_or_update_ocr_data_1(connection, new_records_1)
        connection.commit()
        
        file_ids = [data['file_id'] for data in all_extracted_data] 
        update_file_status(connection, file_ids)
        connection.commit()
        
    except Exception as e:
        connection.rollback()
        logging.error(f"Error in save_and_update_ocr_data_batch: {e} project_id: {project_id}")
    finally:
        cur.close()
        connection.close()







def start_ocr(project_id):
    files = get_files_by_project(project_id)
    if not files:
        logging.error(f"No files found for OCR in this project: {project_id}")
    else:
        downloaded_files, file_sizes = download_files_concurrently(files)
        all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
        save_and_update_ocr_data_batch(project_id, all_extracted_data)
        logging.info(f"OCR data saved successfully in the database for project_id: {project_id}")
    



# Start the OpenAI extraction process
def start_openai(project_id):
    try:
        file_ids, error = fetch_file_ids_by_project(project_id)
        if error:
            logging.error(f"Error fetching file IDs for project {project_id}: {error}")

        if not file_ids:
            logging.info(f"No files to process for project {project_id}")

        # Process each file_id
        results = []
        for file_id in file_ids:
            logging.info(f"Processing file ID: {file_id}")
            result = process_single_document(file_id)
            results.append({
                "file_id": file_id,
                "result": result
            })
            logging.info(f"Completed processing file ID {file_id}: {result}")
        
        # Return the results in JSON format
        logging.info({
            "project_id": project_id,
            "results": results,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        logging.error(f"Error processing project {project_id}: {e}")



# Start the OCR and OpenAI extraction process
def start_extraction(project_id):
    
    start_ocr(project_id)
    logging.info(f"Starting OpenAI Extraction: {project_id}")
    start_openai(project_id)
    logging.info(f"OpenAI Extraction Completed: {project_id}")
    




@app.route("/api/v1/perform_ocr/<int:project_id>", methods=["POST"])
def perform_ocr(project_id):
    try:
        files = get_files_by_project(project_id)        
        s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]

        if not files:
            return jsonify({"error": "No files found for this project."}), 404
        if not s3_file_keys:
            return jsonify({"error": "No files found for this project."}), 404

        downloaded_files, file_sizes = download_files_concurrently(files["file_ids"])

        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_keys,files)

        all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)

        if (all_extracted_data and results):
            save_and_update_ocr_data_batch(project_id, all_extracted_data,results)

        else:
            print("No files found for all_extracted_data and results.")
            return jsonify({"error": "No files found for all_extracted_data and results.."}), 404

        # Return the results as JSON
        return jsonify("Execute successfully !")
    

    except Exception as e:
        return jsonify({"error": str(e)}), 500




@app.route("/api/v1/docai_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    files = get_files_by_project(project_id)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404
    downloaded_files, file_sizes = download_files_concurrently(files["file_ids"])
    all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
    save_and_update_ocr_data_batch(project_id, all_extracted_data)
    logging.info(f"OCR data saved successfully in the database.{project_id}")
    return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200




@app.route("/api/v1/aws_textract/<int:project_id>", methods=["POST"])
def aws_textract_1(project_id):
    try:
        files = get_files_by_project(project_id)        
        s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]
        if not s3_file_keys:
            return jsonify({"error": "No files found for this project."}), 404

        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_keys,files)
        save_and_update_ocr_data_batch(project_id, results)

        # Return the results as JSON
        return jsonify("Execute process_images_with_textract successfully !")
    

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    




# @app.route("/api/v1/file_ocr/<int:project_id>/<int:file_id>", methods=["POST"])
# def file_ocr(project_id, file_id):
#     files = get_single_file_by_file_id(file_id)
#     if not files:
#         return jsonify({"error": "File does not match the OCR criteria, Check ocr_status."}), 404
#     downloaded_files, file_sizes = download_files_concurrently(files)
#     all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
#     save_and_update_ocr_data_batch(project_id, all_extracted_data)
#     logging.info("OCR data saved successfully in the database.")
#     return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200



@app.route('/start-extraction/<int:project_id>', methods=['GET'])
def start_task(project_id):
    thread = threading.Thread(target=start_extraction, args=(project_id,))
    thread.start()
    return jsonify({"message": "OCR and Extraction started", "project_id": project_id}), 202

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)