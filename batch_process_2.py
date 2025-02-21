import os
import re
from flask import Flask, request, jsonify
from google.cloud import storage, documentai
from google.api_core.client_options import ClientOptions

from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Flask App
app = Flask(__name__)

# Google Document AI Configuration
project_id = os.getenv("PROJECT_ID")
location = os.getenv("LOCATION")
processor_id = os.getenv("PROCESSOR_ID")

BUCKET_NAME = "your-gcs-bucket-name"
GCS_INPUT_PREFIX = "documents/input/"  # Folder for input files 
GCS_OUTPUT_PREFIX = "documents/output/"  # Folder for output files

MIME_TYPE = "application/pdf"  # or "image/png", "image/jpeg"


# Initialize Clients
storage_client = storage.Client()
documentai_client = documentai.DocumentProcessorServiceClient(
    client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
)


def upload_to_gcs(file, destination_blob_name):
    """Uploads a file to Google Cloud Storage"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(file)
    return f"gs://{BUCKET_NAME}/{destination_blob_name}"


@app.route("/upload", methods=["POST"])
def upload_files():
    """Handles file uploads and stores them in GCS"""
    if "files" not in request.files:
        return jsonify({"error": "No files provided"}), 400

    uploaded_files = request.files.getlist("files")
    gcs_uris = []

    for file in uploaded_files:
        gcs_uri = upload_to_gcs(file, f"{GCS_INPUT_PREFIX}{file.filename}")
        gcs_uris.append(gcs_uri)

    return jsonify({"message": "Files uploaded successfully", "gcs_uris": gcs_uris})


@app.route("/process", methods=["POST"])
def batch_process():
    """Triggers batch processing in Document AI"""
    gcs_output_uri = f"gs://{BUCKET_NAME}/{GCS_OUTPUT_PREFIX}"
    
    # Create input configuration
    input_config = documentai.BatchDocumentsInputConfig(
        gcs_prefix=documentai.GcsPrefix(gcs_uri_prefix=f"gs://{BUCKET_NAME}/{GCS_INPUT_PREFIX}")
    )

    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(gcs_uri=gcs_output_uri)
    )

    processor_name = documentai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    request = documentai.BatchProcessRequest(
        name=processor_name,
        input_documents=input_config,
        document_output_config=output_config
    )

    operation = documentai_client.batch_process_documents(request)
    return jsonify({"message": "Batch processing started", "operation_id": operation.operation.name})


@app.route("/results", methods=["GET"])
def fetch_results():
    """Fetches processed results from GCS"""
    output_blobs = storage_client.list_blobs(BUCKET_NAME, prefix=GCS_OUTPUT_PREFIX)

    processed_texts = []
    for blob in output_blobs:
        if blob.content_type == "application/json":
            document = documentai.Document.from_json(blob.download_as_bytes(), ignore_unknown_fields=True)
            processed_texts.append(document.text)

    return jsonify({"processed_texts": processed_texts})


if __name__ == "__main__":
    app.run(debug=True)
