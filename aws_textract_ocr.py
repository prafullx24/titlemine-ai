import os
import config
import psycopg2
import psycopg2.extras
import json
from PyPDF2 import PdfReader, PdfWriter
from PyPDF2.errors import PdfReadError
from flask import Flask, jsonify, request
from google.cloud import documentai_v1 as documentai
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import boto3
import urllib.parse
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader, PdfWriter
from flask_cors import CORS
import logging
from db_operations.db import *


# Establish a database connection
connection = get_db_connection() 


def process_images_with_textract(output_folder, s3_file_keys, files):
    logging.info("AWS Text extraction is in progress")

    # Create a Textract client
    textract_client = boto3.client(
        'textract',
        region_name=config.AWS_REGION,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    results = []  # List to store the processing results
    supported_formats = ['.png', '.jpeg', '.jpg', '.pdf', '.tiff']

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    def process_file(s3_file_key, user_id, project_id, file_id):
        file_extension = os.path.splitext(s3_file_key)[1].lower()
        if file_extension not in supported_formats:
            error_message = f"Unsupported document format: {file_extension}. Supported formats: {', '.join(supported_formats)}"
            #print(error_message)
            logging.error(error_message)
            return {"s3_file_key": s3_file_key, "error": error_message}

        try:
            extracted_text = []  # List to store the extracted text
            s3 = boto3.client('s3', aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
            
            # Check if the file exists in the S3 bucket
            try:
                s3.head_object(Bucket=config.AWS_BUCKET, Key=s3_file_key)
            except s3.exceptions.NoSuchKey:
                error_message = f"The specified key does not exist: {s3_file_key}"
                #print(error_message)
                logging.error(error_message)
                return {"s3_file_key": s3_file_key, "error": error_message}

            if file_extension == ".pdf":
                # Download the PDF file from S3
                s3 = boto3.client('s3', aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
                pdf_obj = s3.get_object(Bucket=config.AWS_BUCKET, Key=s3_file_key)
                pdf_content = pdf_obj['Body'].read()

                # Split the PDF into individual pages
                pdf_reader = PdfReader(io.BytesIO(pdf_content))
                num_pages = len(pdf_reader.pages)

                if num_pages > 1:
                    for page_num in range(num_pages):
                        pdf_writer = PdfWriter()
                        pdf_writer.add_page(pdf_reader.pages[page_num])

                        # Save the single page PDF to a BytesIO object
                        temp_pdf_bytes = io.BytesIO()
                        pdf_writer.write(temp_pdf_bytes)
                        temp_pdf_bytes.seek(0)

                        # Start Asynchronous Textract Job for the single page PDF
                        response = textract_client.analyze_document(
                            Document={'Bytes': temp_pdf_bytes.read()},
                            FeatureTypes=["TABLES", "FORMS"]
                        )

                        for block in response.get('Blocks', []):
                            if block['BlockType'] == 'LINE':
                                for word in block['Relationships']:
                                    if word['Type'] == 'CHILD':
                                        word_block = next(b for b in response['Blocks'] if b['Id'] == word['Ids'][0])
                                        if word_block['BlockType'] == 'WORD':
                                            text = word_block['Text']
                                            vertices = word_block.get('Geometry', {}).get('Polygon', [])
                                            extracted_text.append({
                                                "word": text,
                                                "vertices": vertices
                                            })

                else:
                    # Process single page PDF directly
                    response = textract_client.analyze_document(
                        Document={'S3Object': {'Bucket': config.AWS_BUCKET, 'Name': s3_file_key}},
                        FeatureTypes=["TABLES", "FORMS"]
                    )

                    for block in response.get('Blocks', []):
                        if block['BlockType'] == 'LINE':
                            for word in block['Relationships']:
                                if word['Type'] == 'CHILD':
                                    word_block = next(b for b in response['Blocks'] if b['Id'] == word['Ids'][0])
                                    if word_block['BlockType'] == 'WORD':
                                        text = word_block['Text']
                                        vertices = word_block.get('Geometry', {}).get('Polygon', [])
                                        extracted_text.append({
                                            "word": text,
                                            "vertices": vertices
                                        })

            else:
                # Process Non-PDF Files Directly
                response = textract_client.analyze_document(
                    Document={'S3Object': {'Bucket': config.AWS_BUCKET, 'Name': s3_file_key}},
                    FeatureTypes=["TABLES", "FORMS"]
                )

                for block in response.get('Blocks', []):
                    if block['BlockType'] == 'LINE':
                        for word in block['Relationships']:
                            if word['Type'] == 'CHILD':
                                word_block = next(b for b in response['Blocks'] if b['Id'] == word['Ids'][0])
                                if word_block['BlockType'] == 'WORD':
                                    text = word_block['Text']
                                    vertices = word_block.get('Geometry', {}).get('Polygon', [])
                                    extracted_text.append({
                                        "word": text,
                                        "vertices": vertices
                                    })


            # Save results as a single JSON file
            output_filename = os.path.splitext(os.path.basename(s3_file_key))[0] + ".json"
            output_filename = output_filename.replace(':', '_').replace('\\', '_').replace('/', '_')
            output_path = os.path.join(output_folder, output_filename)

            with open(output_path, "w") as json_file:
                json.dump({"words": extracted_text}, json_file, indent=4)


            #print(f"Text extraction completed successfully. Results saved to {output_path}")
            logging.info(f"Text extraction completed successfully. Results saved to {output_path}")

            return {
                'user_id': user_id,
                'project_id': project_id,
                'file_id': file_id,
                'extracted_data': {
                    'words': extracted_text

                }
            }

        except Exception as e:
            print(f"Error processing {s3_file_key}: {e}")
            return {"s3_file_key": s3_file_key, "error": str(e)}

    # Use ThreadPoolExecutor to process files in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(process_file, file_info['s3_file_key'], file_info['user_id'], file_info['project_id'], file_info['file_id']): file_info['s3_file_key'] for file_info in files['file_ids']}
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)

    return results