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
import time
import io
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
# PDF_FOLDER = "pdf_folder"
# os.makedirs(PDF_FOLDER, exist_ok=True)

OUTPUT_FOLDER = "output_folder"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def process_images_with_textract(output_folder, s3_file_key):
    print("Wait! Text extraction is in progress...")

    # Create a Textract client 
    textract_client = boto3.client(
        'textract',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    results = [] # Store the results for each file
    
    # Check supported formats
    supported_formats = ['.png', '.jpeg', '.jpg', '.pdf', '.tiff']
    file_extension = os.path.splitext(s3_file_key)[1].lower() # Get the file extension
    if file_extension not in supported_formats:
        error_message = f"Unsupported document format: {file_extension}. Supported formats are: {', '.join(supported_formats)}"
        print(error_message)
        return [{"s3_file_key": s3_file_key, "error": error_message}]

    try:
        if file_extension == ".pdf":
            # Start document analysis (Asynchronous for PDFs)
            response = textract_client.start_document_analysis( # Asynchronous for PDFs 
                DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': s3_file_key}}, # S3 location of the document 
                FeatureTypes=["TABLES", "FORMS"] # Specify the feature types to extract 
            )
            job_id = response["JobId"] # Get the job ID from the response  
            print(f"Started document analysis. Job ID: {job_id}")

            # Polling to check job status
            while True:
                status_response = textract_client.get_document_analysis(JobId=job_id) # Get the job status response
                status = status_response['JobStatus'] # Get the job status from the response 
                if status in ["SUCCEEDED", "FAILED"]:
                    break
                print("Waiting for Textract to complete...")
                time.sleep(5)  # Wait before polling again

            if status == "FAILED":
                return [{"s3_file_key": s3_file_key, "error": "Textract failed"}]

            response = status_response  # Get the final response after completion
        
        else:
            # Process non-PDF files directly
            response = textract_client.analyze_document( # Synchronous for images 
                Document={'S3Object': {'Bucket': bucket_name, 'Name': s3_file_key}},
                FeatureTypes=["TABLES", "FORMS"]
            )

        extracted_text = []
        for block in response.get('Blocks', []):
            if block['BlockType'] == 'LINE':
                extracted_text.append({
                    "text": block['Text'],
                    "confidence": block['Confidence']
                })

        # Save results
        output_filename = os.path.splitext(os.path.basename(s3_file_key))[0] + ".json"
        output_path = os.path.join(output_folder, output_filename)
        with open(output_path, "w") as json_file:
            json.dump({"extracted_text": extracted_text}, json_file, indent=4)

        print(f"Text extraction completed successfully. Results saved to {output_path}")
        return [{
            "s3_file_key": s3_file_key,
            "output_path": output_path,
            "extracted_text": "completed successfully"
        }]

    except Exception as e:
        print(f"Error processing file with Textract: {e}")
        return [{"s3_file_key": s3_file_key, "error": str(e)}]






@app.route("/api/v1/aws_textract/s3_file_key", methods=["POST"])
def aws_textract():
    try:
        # Get the s3_file_key from the request
        s3_file_key = request.json.get("s3_file_key")
        if not s3_file_key:
            return jsonify({"error": "s3_file_key is required"}), 400

        # Process the file with Textract
        results = process_images_with_textract(OUTPUT_FOLDER, s3_file_key)
        # print("Execute process_images_with_textract successfully !")

        # Return the results as JSON
        return jsonify("Execute process_images_with_textract successfully !")
    

    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
