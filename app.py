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
from db_operations.db import *
from common_code import *
from document_ai_ocr import *
from aws_textract_ocr import *
from extract_data import *


# Initialize Flask App
app = Flask(__name__)
CORS(app)

# Folder to store downloaded and OCR files
OUTPUT_FOLDER = "output_file"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)



for handler in app.logger.handlers[:]:
    app.logger.removeHandler(handler)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.INFO)  # Ensure INFO logs are captured
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)


# Establish a database connection
connection = get_db_connection() 





@app.route("/api/v1/docai_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    try:
        files = get_files_by_project(project_id)
        if not files:
            return jsonify({"error": "No files found for this project."}), 404
        downloaded_files, file_sizes = download_files_concurrently(files["file_ids"])
        logging.info(f"Downloaded files successfully for Document AI OCR.")

        all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
        logging.info(f"Extracted text with confidence successfully for Document AI OCR.")

        if (all_extracted_data):
            save_and_update_ocr_data_batch(project_id, all_extracted_data, None)
            logging.info(f"OCR data saved successfully in the database.{project_id}")
            return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200
        else:
            return jsonify({"error": "No files found for all_extracted_data."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/v1/aws_textract/<int:project_id>", methods=["POST"])
def aws_textract_1(project_id):
    try:
        files = get_files_by_project(project_id)        
        s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]
        
        if not s3_file_keys:
            return jsonify({"error": "No files found for this project."}), 404
        
        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_keys,files)
        logging.info(f"Execute process_images_with_textract successfully.")
        
        if(results):
            save_and_update_ocr_data_batch(project_id,None, results)
            logging.info(f"Execute process_images_with_textract successfully.")
            return jsonify("Execute process_images_with_textract successfully !")
        else:
            return jsonify({"error": "No files found for results."}), 404
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    



@app.route("/api/v1/perform_ocr/<int:project_id>", methods=["POST"])
def perform_ocr(project_id,file_id):
    try:
        files = get_files_by_project(project_id)        
        s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]

        if not files:
            #return jsonify({"error": "No files found for OCR to this project."}), 404
            logging.info(f"No files found for OCR to this project.")
        if not s3_file_keys:
            # return jsonify({"error": "No any s3 file key found for this project."}), 404
            logging.info(f"No any s3 file key found for this project.")

        downloaded_files, file_sizes = download_files_concurrently(files["file_ids"])
        logging.info(f"Downloaded files successfully for Document AI OCR.")

        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_keys,files)
        all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
        logging.info(f"Extracted text with confidence successfully for Document AI OCR and AWS Textract.")

        if (all_extracted_data and results):
            save_and_update_ocr_data_batch(project_id, all_extracted_data,results)
        else:
            # return jsonify({"error": "No files found for all_extracted_data and results.."}), 404
            logging.info(f"No files found for all_extracted_data and results.")

        # Return the results as JSON
        # return jsonify("Execute successfully !")
        logging.info(f"Execute Document AI and AWS Textract successfully !")

    except Exception as e:
        return jsonify({"error": str(e)}), 500




@app.route('/process_project/<project_id>', methods=['GET'])
def process_project(project_id):
    try:
        # Fetch file IDs associated with the project
        file_ids, error = fetch_file_ids_by_project(project_id)
        if error:
            logging.error(f"Error fetching file IDs for project {project_id}: {error}")
            return jsonify({"error": f"Error fetching file IDs: {error}"}), 500

        if not file_ids:
            logging.info(f"No files to process for project {project_id}")
            return jsonify({"message": f"No files to process for project ID {project_id}"}), 404

        results = []
        for file_id in file_ids:
            logging.info(f"Processing file ID: {file_id}")
            result = process_single_document(file_id)

            if "error" not in result:
                storeprocessed_extracted_data(file_id, result, project_id)
                store_runsheet_data(file_id, result, project_id)

            results.append({
                "file_id": file_id,
                "result": result
            })
            logging.info(f"Completed processing file ID {file_id}")

        return jsonify({
            "project_id": project_id,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as e:
        logging.error(f"Error processing project {project_id}: {e}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500





@app.route("/api/v1/combine_ocr/<int:project_id>", methods=["POST"])
def combine_ocr(project_id):
    try:
        files = get_files_by_project(project_id)        
        s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]

        if not files:
            #return jsonify({"error": "No files found for OCR to this project."}), 404
            logging.info(f"No files found for OCR to this project.")

        # s3_file_keys = [file["s3_file_key"] for file in files["file_ids"]]
        if not s3_file_keys:
            # return jsonify({"error": "No any s3 file key found for this project."}), 404
            logging.info(f"No any s3 file key found for this project.")

        downloaded_files, file_sizes = download_files_concurrently(files["file_ids"])
        logging.info(f"Downloaded files successfully for Document AI OCR.")

        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_keys,files)
        all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
        logging.info(f"Extracted text with confidence successfully for Document AI OCR and AWS Textract.")

        if (all_extracted_data and results):
            save_and_update_ocr_data_batch(project_id, all_extracted_data,results)
        else:
            # return jsonify({"error": "No files found for all_extracted_data and results.."}), 404
            logging.info(f"No files found for all_extracted_data and results.")
            # return jsonify({"error": "No files found for all_extracted_data and results."}), 404


        # Return the results as JSON
        # return jsonify("Execute successfully !")
        logging.info(f"Execute Document AI and AWS Textract successfully.")
        logging.info(f"Starting Extraction: {project_id}")

        # Fetch file IDs associated with the project
        file_ids, error = fetch_file_ids_by_project(project_id)
        if error:
            logging.error(f"Error fetching file IDs for project {project_id}: {error}")
            return jsonify({"error": f"Error fetching file IDs: {error}"}), 500

        if not file_ids:
            logging.info(f"No files to process for project {project_id}")
            return jsonify({"message": f"No files to process for project ID {project_id}"}), 404

        results = []
        for file_id in file_ids:
            logging.info(f"Processing file ID: {file_id}")
            result = process_single_document(file_id)

            if "error" not in result:
                storeprocessed_extracted_data(file_id, result, project_id)
                store_runsheet_data(file_id, result, project_id)

            results.append({
                "file_id": file_id,
                "result": result
            })
            logging.info(f"Completed processing file ID {file_id}")

        return jsonify({
            "project_id": project_id,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as e:
        logging.error(f"Error processing project {project_id}: {e}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500




if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)