
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
import urllib.parse
import openai
from db_operations.db import *
from document_ai_ocr import *
from extract_data import *

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
    #  logging.info(f"Project ID: {project_id}, User ID: {user_id}, Files: {json.dumps(data, indent=4)}")

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
    logging.info(f"Files fetched for ocr : {files}")
    connection.commit()

    if files:
        for file in files:
            logging.info(f"File: {file}")
        if all(len(file) >= 6 for file in files):
            user_id = files[0][1]
            # print("Printed project ID, file IDs, s3_urls, and s3_file_keys to the terminal")
            data = save_project_files_to_variable(project_id, user_id, files)
            return data
        else:
            logging.error(f"Some files do not have the expected structure.")
            return None
    else:
        logging.error(f"No files found for this project.")
        return None




# Download files concurrently and save them to a folder
def get_single_file_by_file_id(file_id): 
    files = select_file_by_fileid(connection,file_id)
    connection.close()
    logging.info(f"Files fetched for ocr : {files}")

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
        if all_extracted_data:
            for data in all_extracted_data:
                # extracted_data = data['extracted_data']
                extracted_data = data.get('extracted_data', {})
                if isinstance(extracted_data, list):
                    extracted_data = {"text": "", "confidence_scores": extracted_data}
                new_records.append(
                    (data['file_id'], project_id, json.dumps(extracted_data), extracted_data.get('text', '').replace("\n", " "))
            )
            insert_or_update_ocr_data(connection, new_records)
            connection.commit()
        

        new_records_1 = []
        if results:
            for data_1 in results:
                # extracted_data = data['extracted_data']
                extracted_data_1 = data_1.get('extracted_data', {})
                if isinstance(extracted_data_1, list):
                    extracted_data_1 = {"text": "", "confidence_scores": extracted_data_1}
                new_records_1.append(
                    (data_1['file_id'], project_id, json.dumps(extracted_data_1), extracted_data_1.get('text', '').replace("\n", " "))
            )
            
            insert_or_update_ocr_data_1(connection, new_records_1)
            connection.commit()





        logging.info(f"Open AI and AWS Textract OCR data saved for project_id: {project_id}")

        if all_extracted_data or results:
            file_ids = [data['file_id'] for data in (all_extracted_data or results)]
            update_file_status(connection, file_ids)
            logging.info(f"OCR status updated to 'Extracting' for project_id: {project_id}")
            connection.commit()
        
    except Exception as e:
        connection.rollback()
        logging.error(f"Error in save_and_update_ocr_data_batch: {e} project_id: {project_id}")
    finally:
        cur.close()
        connection.close()
