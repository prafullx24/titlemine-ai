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


# Folder to store downloaded and OCR files
DOWNLOAD_FOLDER = "download_file"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


connection = get_db_connection() # Establish a database connection

# This function saves the project ID and file IDs to a variable and prints the value.
def save_project_files_to_variable(project_id, user_id, files):
    """Save project ID and file IDs to a variable and print the value."""
    data = {
        "project_id": project_id,
        "file_ids": [file[0] for file in files]
    }
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
    connection.commit()
    print(files)

    # Save project ID and file IDs to a JSON file
    if files:
        user_id = files[0][1]
        save_project_files_to_variable(project_id, user_id, files)

    return files




def get_single_file_by_file_id(file_id): 
    files = select_file_by_fileid(connection,file_id)
    connection.close()
    print(files)

    # Save project ID and file IDs to a JSON file
    if files:
        user_id = files[0][1]
        save_project_files_to_variable(file_id, user_id, files)

    return files





# The download_file_from_s3 function downloads a file from an S3 URL and saves it locally.
def download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 URL and save it locally"""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
    if os.path.exists(file_path):
        return file_path
    response = requests.get(s3_url, stream=True)
    if response.status_code == 200:
        with open(file_path, "wb") as file: 
            for chunk in response.iter_content(1024):
                file.write(chunk)
        logging.info(f"Downloaded file from S3: file_id: {file_id}; project_id {project_id}")
        return file_path 
    logging.error(f"Failed to download file from S3: {s3_url}")
    return None





# The code defines a function to download files concurrently from S3 URLs using a thread pool, handling errors and printing the download status for each file.
def download_files_concurrently(files):
    downloaded_files = [] 
    file_sizes = []  
    temp_project_id = files[0][2]

    def download_file(file):
        try:
            id, user_id, project_id, file_name, s3_url, ocr_status = file
            file_extension = os.path.splitext(file_name)[1] 
            pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, id, file_extension)
            if pdf_file_path:
                downloaded_files.append(pdf_file_path)
            else:
                logging.error(f"Failed to download file from S3: {file_name} : {s3_url} project_id: {project_id}")
                
        except Exception as e:
            logging.error(f"Error downloading file {file}: {e}")

    if not files:
        logging.error(f"No files to download. project_id: {temp_project_id}")
        return []

    with ThreadPoolExecutor() as executor:    # No limit on concurrent downloads
        executor.map(download_file, files)

    logging.info(f"All files downloaded. project_id: {temp_project_id}")

    for file_path in downloaded_files:
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB
        file_size_formatted = f"{file_size:.2f}"  # Format file size to 2 decimal places
        file_sizes.append({"file_name": os.path.basename(file_path), "file_size": file_size_formatted})

    return downloaded_files, file_sizes




# This function saves the OCR extracted data as a JSON file in a specified download folder, using the user ID, project ID, and file ID to name the file.
def save_ocr_output_as_json(user_id, project_id, file_id, extracted_data):
    """Save OCR output as JSON."""
    ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_json_{user_id}_{project_id}_{file_id}.json")
    with open(ocr_file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
        logging.info(f"OCR JSON saved successfully: {ocr_file_path}")



#  This function iterates through a list of extracted OCR data and saves each entry as a JSON file using the save_ocr_output_as_json function.
def save_ocr_outputs_as_json(extracted_data_list):
    """Save multiple OCR outputs as JSON."""
    for data in extracted_data_list:
        user_id = data['user_id']
        project_id = data['project_id']
        file_id = data['file_id']
        extracted_data = data['extracted_data']
        save_ocr_output_as_json(user_id, project_id, file_id, extracted_data)



def extract_text_with_confidence(file_path, start_page_offset=0):
    """Extracts text and confidence scores from a document using Google Document AI"""
    
    if not os.path.exists(config.credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {config.credentials_path}")

    def split_pdf(file_path, start_page, end_page):
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            writer = PdfWriter()
            for page_num in range(start_page, end_page):
                writer.add_page(reader.pages[page_num])
            split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
            with open(split_file_path, 'wb') as output_file:
                writer.write(output_file)
            return split_file_path

    def process_document(file_path, start_page_offset):
        client = documentai.DocumentProcessorServiceClient()
        with open(file_path, "rb") as file:
            content = file.read()
        raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
        name = f"projects/{config.PROJECT_ID}/locations/{config.LOCATION}/processors/{config.PROCESSOR_ID}"
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)

        # Debugging statement to log request details
        logging.info(f"Processing document: {file_path}, Size: {len(content)} bytes")

        response = client.process_document(request=request)

        # Debugging statement to log response details
        logging.info(f"Document processed: {file_path}, Size: {len(content)} bytes")

        document_dict = documentai.Document.to_dict(response.document)
        extracted_text = document_dict.get("text", "")
        extracted_data = {"text": extracted_text, "confidence_scores": []}

        for page in response.document.pages:
            for block in page.blocks:
                for segment in block.layout.text_anchor.text_segments:
                    segment_text = document_dict["text"][segment.start_index:segment.end_index]
                    confidence = block.layout.confidence
                    extracted_data["confidence_scores"].append({
                        "text": segment_text,
                        "confidence": confidence,
                        "page_no": page.page_number + start_page_offset
                    })

        return extracted_data

    def split_and_process(file_path, max_size_mb=20, max_pages=15, start_page_offset=0):
        total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            total_pages = len(reader.pages)
            size_per_page_mb = total_size_mb / total_pages

        if total_size_mb <= max_size_mb and total_pages <= max_pages:
            return [process_document(file_path, start_page_offset)]

        start_page = 0
        extracted_data = []

        while start_page < total_pages:
            end_page = start_page
            current_size_mb = 0
            while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
                current_size_mb += size_per_page_mb
                end_page += 1

            split_file_path = split_pdf(file_path, start_page, end_page)
            extracted_data.extend(split_and_process(split_file_path, max_size_mb, max_pages, start_page_offset + start_page))

            start_page = end_page

        return extracted_data

    total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    with open(file_path, 'rb') as file:
        reader = PdfReader(file)
        num_pages = len(reader.pages)

    # Extract file ID from the file path
    file_id = os.path.basename(file_path).split('_')[4].split('.')[0]

    if total_size_mb > 20 and num_pages == 1:
        logging.error(f'Splitting for this file is not possible! File ID: {file_id}')
    elif total_size_mb > 20:
        return split_and_process(file_path, start_page_offset=start_page_offset)
    elif num_pages > 15:
        extracted_data = []
        for start_page in range(0, num_pages, 15):
            end_page = min(start_page + 15, num_pages)
            split_file_path = split_pdf(file_path, start_page, end_page)
            extracted_data.extend(split_and_process(split_file_path, start_page_offset=start_page))
        return extracted_data
    else:
        return process_document(file_path, start_page_offset)


















# This function processes multiple documents using Google Document AI to extract text and confidence scores, saves the extracted data as JSON files, and returns the aggregated results.
def extract_text_with_confidence_batch(downloaded_files, file_sizes):
    """Extracts text and confidence scores from multiple documents using Google Document AI."""
    
    all_extracted_data = [] # List to store all extracted data from multiple documents

    def process_file(file_path):
        try:
            logging.info(f"Processing file: {file_path}")
            extracted_data = extract_text_with_confidence(file_path) # Extract text and confidence scores from the document

            # Extract user_id, project_id, and file_id from the file name
            file_name_parts = os.path.basename(file_path).split('_')
            user_id = file_name_parts[2]
            project_id = file_name_parts[3]
            file_id = file_name_parts[4].split('.')[0]
            
            return {
                'user_id': user_id,
                'project_id': project_id,
                'file_id': file_id,
                'extracted_data': extracted_data
            }
        
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return None

    

    # with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent threads
    with ThreadPoolExecutor() as executor:  # No limit on concurrent threads
        results = list(executor.map(process_file, downloaded_files))

    # Filter out any None results due to errors
    results = [result for result in results if result is not None]

    all_extracted_data.extend(results)

    # Save all OCR outputs as JSON
    save_ocr_outputs_as_json(all_extracted_data)
    logging.info("Done with Extracts text and confidence scores ")
    return all_extracted_data




# This function inserts or updates OCR data for multiple files in the database and updates their OCR status to 'Completed'.
def save_and_update_ocr_data_batch(project_id, all_extracted_data):
    cur = connection.cursor()
    
    try:
        new_records = []
        for data in all_extracted_data:
            extracted_data = data['extracted_data']
            if isinstance(extracted_data, list):
                extracted_data = {"text": "", "confidence_scores": extracted_data}
            new_records.append(
                (data['file_id'], project_id, json.dumps(extracted_data), extracted_data.get('text', '').replace("\n", " "))
        )
        
        insert_or_update_ocr_data(connection, new_records)
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

def start_extraction(project_id):
    
    start_ocr(project_id)
    logging.info(f"Starting OpenAI Extraction: {project_id}")
    start_openai(project_id)
    logging.info(f"OpenAI Extraction Completed: {project_id}")
    


@app.route("/api/v1/batch_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    files = get_files_by_project(project_id)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404
    downloaded_files, file_sizes = download_files_concurrently(files)
    all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
    save_and_update_ocr_data_batch(project_id, all_extracted_data)
    logging.info(f"OCR data saved successfully in the database.{project_id}")
    return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200

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