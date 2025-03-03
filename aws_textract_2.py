

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
from pdf2image import convert_from_path
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








# def process_images_with_textract(pdf_folder, output_folder,download_path):
#     print("Wait! Text extraction is in progress..")
#     s3originalkey = s3_file_key.split(".")[0]
#     # Create a Textract client
#     textract_client = boto3.client(
#         'textract',
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )

#     results = []

#     # Create 'images' folder inside the provided output_folder


#     for pdf_file in sorted(os.listdir(pdf_folder)):
#         if pdf_file.lower().endswith(".pdf"):
#             pdf_path = os.path.join(pdf_folder, pdf_file)

#             import time
#             # Upload the image to S3 with a timestamp in the filename
#             timestamp = time.time()
#             s3_key = f"pdf/{timestamp}.pdf"
#             s3_url = upload_to_s3(pdf_path, s3_key)
#             os.remove(pdf_path)
#             if s3_url:
#                 # Process the image with Textract
#                 try:
#                     response = textract_client.detect_document_text(
#                         Document={
#                             'S3Object': {
#                                 'Bucket': os.getenv("TEXTRACT_S3_BUCKET_NAME"),
#                                 'Name': s3_key
#                             }
#                         }
#                     )
#                     extracted_text = [
#                         block['Text'] for block in response.get('Blocks', []) if
#                         block['BlockType'] == 'LINE'
#                     ]
#                     text = "\n".join(extracted_text)
#                     results.append({
#                         "text": text,
#                         "s3_url": s3_url  # Store the S3 URL
#                     })
#                     print(results)
#                 except Exception as e:
#                     results[pdf_path] = {
#                         "text": f"Error: {str(e)}",
#                         "s3_url": s3_url
#                     }


#     return results











# def process_images_with_textract(pdf_folder, output_folder, download_path):
#     print("Wait! Text extraction is in progress..")
    
#     # Create a Textract client using AWS credentials
#     textract_client = boto3.client(
#         'textract',
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )

#     results = []

#     try:
#         # Read the PDF file as bytes
#         with open(download_path, 'rb') as document:
#             image_bytes = document.read()

#         # Process the file with Textract
#         response = textract_client.detect_document_text(
#             Document={
#                 'Bytes': image_bytes
#             }
#         )
#         extracted_text = [
#             {
#                 "text": block['Text'],
#                 "confidence": block['Confidence']
#             } for block in response.get('Blocks', []) if block['BlockType'] == 'LINE'
#         ]
#         results.append({
#             "extracted_text": extracted_text,
#             "file_path": download_path  # Store the local file path
#         })
#         print(results)
#     except Exception as e:
#         results.append({
#             "error": str(e),
#             "file_path": download_path
#         })

#     return results
















# def process_images_with_textract(pdf_folder, output_folder, download_path):
#     print("Wait! Text extraction is in progress..")
    
#     # Create a Textract client using AWS credentials
#     textract_client = boto3.client(
#         'textract',
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )

#     results = []

#     try:
#         # Read the PDF file as bytes
#         with open(download_path, 'rb') as document:
#             image_bytes = document.read()

#         # Start the asynchronous text detection job
#         response = textract_client.start_document_text_detection(
#             DocumentLocation={
#                 'Bytes': image_bytes
#             }
#         )
#         job_id = response['JobId']
#         print(f"Started text detection job with ID: {job_id}")

#         # Wait for the job to complete
#         while True:
#             response = textract_client.get_document_text_detection(JobId=job_id)
#             status = response['JobStatus']
#             if status in ['SUCCEEDED', 'FAILED']:
#                 break
#             print("Waiting for job to complete...")
#             time.sleep(5)

#         if status == 'SUCCEEDED':
#             extracted_text = [
#                 {
#                     "text": block['Text'],
#                     "confidence": block['Confidence']
#                 } for block in response.get('Blocks', []) if block['BlockType'] == 'LINE'
#             ]
#             results.append({
#                 "extracted_text": extracted_text,
#                 "file_path": download_path  # Store the local file path
#             })
#             print(results)
#         else:
#             results.append({
#                 "error": "Text detection job failed",
#                 "file_path": download_path
#             })

#     except Exception as e:
#         results.append({
#             "error": str(e),
#             "file_path": download_path
#         })

#     return results













def process_images_with_textract(pdf_folder, output_folder, download_path):
    print("Wait! Text extraction is in progress..")
    
    # Create a Textract client using AWS credentials
    textract_client = boto3.client(
        'textract',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    results = []

    try:
        # Convert PDF to images
        images = convert_from_path(download_path)
        
        for i, image in enumerate(images):
            # Convert image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            image_bytes = img_byte_arr.getvalue()

            # Process the image with Textract
            response = textract_client.detect_document_text(
                Document={
                    'Bytes': image_bytes
                }
            )
            extracted_text = [
                {
                    "text": block['Text'],
                    "confidence": block['Confidence']
                } for block in response.get('Blocks', []) if block['BlockType'] == 'LINE'
            ]
            results.append({
                "page": i + 1,
                "extracted_text": extracted_text,
                "file_path": download_path  # Store the local file path
            })
            print(results)

    except Exception as e:
        results.append({
            "error": str(e),
            "file_path": download_path
        })

    return results










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
        
        # return jsonify({"message": "File downloaded successfully"})


        # Process the file with Textract
        results = process_images_with_textract(PDF_FOLDER, OUTPUT_FOLDER, download_path)
        # return jsonify("Execute process_images_with_textract successfully !")

        # Return the results as JSON
        return jsonify(results)
    

        # # Save the text output in the output_folder
        # for result in results:
        #     text_output_path = os.path.join(OUTPUT_FOLDER, f"{os.path.basename(s3_file_key)}.txt")
        #     with open(text_output_path, "w") as text_file:
        #         text_file.write(result["text"])

        # # Return the results as JSON
        # return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

















# def process_images_with_textract(pdf_folder, output_folder,s3_file_key):
#     print("Wait! Text extraction is in progress..")
#     s3originalkey = s3_file_key.split(".")[0]
#     # Create a Textract client
#     textract_client = boto3.client(
#         'textract',
#         region_name=region_name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )

#     results = []

#     # Create 'images' folder inside the provided output_folder


#     for pdf_file in sorted(os.listdir(pdf_folder)):
#         if pdf_file.lower().endswith(".pdf"):
#             pdf_path = os.path.join(pdf_folder, pdf_file)

#             import time
#             # Upload the image to S3 with a timestamp in the filename
#             timestamp = time.time()
#             s3_key = f"pdf/{timestamp}.pdf"
#             s3_url = upload_to_s3(pdf_path, s3_key)
#             os.remove(pdf_path)
#             if s3_url:
#                 # Process the image with Textract
#                 try:
#                     response = textract_client.detect_document_text(
#                         Document={
#                             'S3Object': {
#                                 'Bucket': os.getenv("TEXTRACT_S3_BUCKET_NAME"),
#                                 'Name': s3_key
#                             }
#                         }
#                     )
#                     extracted_text = [
#                         block['Text'] for block in response.get('Blocks', []) if
#                         block['BlockType'] == 'LINE'
#                     ]
#                     text = "\n".join(extracted_text)
#                     results.append({
#                         "text": text,
#                         "s3_url": s3_url  # Store the S3 URL
#                     })
#                     print(results)
#                 except Exception as e:
#                     results[pdf_path] = {
#                         "text": f"Error: {str(e)}",
#                         "s3_url": s3_url
#                     }


#     return results







# @app.route("/api/v1/aws_textract/s3_file_key", methods=["POST"])
# def aws_textract():
#     try:
#         # Get the s3_file_key from the request
#         s3_file_key = request.json.get("s3_file_key")
#         if not s3_file_key:
#             return jsonify({"error": "s3_file_key is required"}), 400

#         # Download the file from S3
#         pdf_path = os.path.join(PDF_FOLDER, os.path.basename(s3_file_key))
#         download_path = download_from_s3(s3_file_key, pdf_path)
#         if not download_path:
#             return jsonify({"error": "Failed to download file from S3"}), 500
        
#         return jsonify({"message": "File downloaded successfully"})
    
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

#     #     # Process the file with Textract
#     #     results = process_images_with_textract(PDF_FOLDER, OUTPUT_FOLDER, s3_file_key)

#     #     # Save the text output in the output_folder
#     #     for result in results:
#     #         text_output_path = os.path.join(OUTPUT_FOLDER, f"{os.path.basename(s3_file_key)}.txt")
#     #         with open(text_output_path, "w") as text_file:
#     #             text_file.write(result["text"])

#     #     # Return the results as JSON
#     #     return jsonify(results)

#     # except Exception as e:
#     #     return jsonify({"error": str(e)}), 500




# if __name__ == "__main__":
#     app.run(debug=True, host="0.0.0.0", port=5000)