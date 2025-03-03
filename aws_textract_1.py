

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
import boto3
import re
import urllib.parse
# import logging



# Load environment variables
load_dotenv()


# Set Google Application Credentials
credentials_path = os.getenv("CREDENTIALS_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


# Set AWS Credentials 
region_name = os.getenv("AWS_REGION") 
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID") 
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("AWS_BUCKET")


# Print environment variables to debug
# print(f"AWS_REGION_NAME: {region_name}")
# print(f"AWS_ACCESS_KEY_ID: {aws_access_key_id}")
# print(f"AWS_SECRET_ACCESS_KEY: {aws_secret_access_key}")
# print(f"TEXTRACT_S3_BUCKET_NAME: {bucket_name}")

# Initialize Flask App
app = Flask(__name__)



# Database Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# Google Document AI Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")


# Folder to store downloaded and OCR files
PDF_FOLDER = "pdf_folder"
os.makedirs(PDF_FOLDER, exist_ok=True)

OUTPUT_FOLDER = "output_folder"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)



def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def download_from_s3(s3_key, download_path):
    s3_client = boto3.client(
        's3',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket_name = os.getenv("AWS_BUCKET")
    # print(f"TEXTRACT_S3_BUCKET_NAME: {bucket_name}")  # Debug print statement
    # print(f"S3_FILE_KEY: {s3_key}")  # Debug print statement
    print(f"DOWNLOAD_PATH: {download_path}")  # Debug print statement
    
    
    try:
        s3_client.download_file(bucket_name, s3_key, download_path)
        print(f"File downloaded successfully to = {download_path}")
        return download_path
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return None





@app.route("/api/v1/aws_textract/s3_file_key", methods=["POST"])
def aws_textract():
    try:
        # Get the s3_file_key from the request
        s3_file_key = request.json.get("s3_file_key")
        if not s3_file_key:
            return jsonify({"error": "s3_file_key is required"}), 400

        # Decode the URL-encoded s3_file_key
        s3_file_key = urllib.parse.unquote(s3_file_key)

        # Sanitize the file name
        sanitized_filename = sanitize_filename(os.path.basename(s3_file_key))
        print(f"Sanitized filename: {sanitized_filename}")

        pdf_path = os.path.join(PDF_FOLDER, sanitized_filename)
        
        # Download the file from S3
        download_path = download_from_s3(s3_file_key, pdf_path)
        if not download_path:
            return jsonify({"error": "Failed to download file from S3"}), 500
        
        return jsonify({"message": "File downloaded successfully"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)