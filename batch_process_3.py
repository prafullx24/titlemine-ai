import os
import requests
import psycopg2
import json
from flask import Flask, jsonify
from google.cloud import documentai
from google.api_core.client_options import ClientOptions
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Flask App
app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}


# Google Document AI Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

# Folder to store downloaded and OCR files
LOCAL_DOWNLOAD_PATH = "downloaded_pdfs"
OCR_OUTPUT_PATH = "ocr_results"

# Ensure directories exist
os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(OCR_OUTPUT_PATH, exist_ok=True)

# Initialize Document AI Client
# documentai.DocumentProcessorServiceClient is used to create a client instance for interacting with the Document AI service.
# ClientOptions is used to specify the api_endpoint option. The api_endpoint is constructed using the LOCATION variable, resulting in a URL like us-documentai.googleapis.com or eu-documentai.googleapis.com
# Purpose:
# Custom API Endpoint:
# By specifying the api_endpoint with ClientOptions, the client is directed to use the Document AI service endpoint specific to the given location. This is useful for optimizing latency and ensuring compliance with regional data processing requirements.
# Usage:
# Once the client is initialized, it can be used to make requests to the Document AI service, such as processing documents, retrieving results, and managing operations.
client = documentai.DocumentProcessorServiceClient( 
    client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com") 
)








# def get_file_url_from_db(file_id):
#     """Fetches file URL from database using file ID"""
#     conn = sqlite3.connect("files.db")
#     cursor = conn.cursor()
#     cursor.execute("SELECT file_url FROM file_data WHERE file_id = ?", (file_id,))
#     result = cursor.fetchone()
#     conn.close()
#     return result[0] if result else None

# The get_file_info function fetches file details (user_id, project_id, file_name, s3_url) from the database based on the provided file_id.
def get_file_info(file_id):
    """Fetch file details from the database based on file_id"""
    conn = psycopg2.connect(**DB_CONFIG) 
    cur = conn.cursor() # cursor object to execute queries
    query = "SELECT user_id, project_id, file_name, s3_url, ocr_status FROM public.files WHERE id = %s"
    cur.execute(query, (file_id,)) # Execute the query with the file_id parameter 
    file_data = cur.fetchone() # Fetch the first row from the result set 
    cur.close()
    conn.close()
    return file_data








# The download_file_from_s3 function downloads a file from an S3 URL and saves it locally.
def download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 URL and save it locally"""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(LOCAL_DOWNLOAD_PATH, file_name) # Path to save the downloaded file
    if os.path.exists(file_path):
        return file_path
    response = requests.get(s3_url, stream=True) # Send a GET request to the S3 URL to download the file 
    if response.status_code == 200:
        with open(file_path, "wb") as file: # Open the file in write binary mode 
            for chunk in response.iter_content(1024): 
                file.write(chunk) # Write the chunk to the file 
        return file_path 
    return None


def process_document_with_ocr(pdf_path):
    """Processes the PDF with Document AI OCR and saves JSON output"""
    with open(pdf_path, "rb") as pdf_file:
        pdf_content = pdf_file.read()

    # Configure OCR request
    processor_name = client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    raw_document = documentai.RawDocument(content=pdf_content, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=processor_name, raw_document=raw_document)

    # Process the document
    result = client.process_document(request)
    document_text = result.document.text

    # Save JSON response locally
    json_output_path = os.path.join(OCR_OUTPUT_PATH, os.path.basename(pdf_path) + ".json")
    with open(json_output_path, "w", encoding="utf-8") as json_file:
        json_file.write(document_text)

    return json_output_path


@app.route("/process-file/<file_id>", methods=["GET"])
def process_file(file_id):
    """Handles file processing using file ID"""
    file_url = get_file_info(file_id)
    if not file_url:
        return jsonify({"error": "File ID not found in database"}), 404

    # Download the PDF
    user_id, project_id, file_name, s3_url, ocr_status = get_file_info(file_id)
    file_extension = os.path.splitext(file_name)[1]
    local_pdf_path = download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension)
    if not local_pdf_path:
        return jsonify({"error": "Failed to download PDF"}), 500

    # Process the document with OCR
    ocr_output_path = process_document_with_ocr(local_pdf_path)

    return jsonify({
        "message": "OCR processing completed",
        "ocr_output_file": ocr_output_path
    })


if __name__ == "__main__":
    app.run(debug=True)
