import psycopg2.extras
import psycopg2
from psycopg2 import OperationalError, IntegrityError
import json
from datetime import datetime
import re
import os
import logging
from dotenv import load_dotenv
import config
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establish and return a database connection."""
    # print("Database credentials:", config.DB_CONFIG)  # Print the database credentials
    connection = psycopg2.connect(**config.DB_CONFIG)

    return connection

def select_file_by_projectid(connection, project_id):
    select_query = """
        SELECT id, user_id, project_id, file_name, s3_url, ocr_status 
        FROM public.files 
        WHERE project_id = %s 
        AND ocr_status = 'Processing'
    """


    with connection.cursor() as cursor:
        cursor.execute(select_query, (project_id,))
        files = cursor.fetchall()

    return files



def select_file_by_fileid(connection, fileid):
    select_query = """
        SELECT id, user_id, project_id, file_name, s3_url, ocr_status 
        FROM public.files 
        WHERE id = %s 
        AND ocr_status = 'Processing'
    """

    with connection.cursor() as cursor:
        cursor.execute(select_query, (fileid,))
        files = cursor.fetchall()

    return files


def insert_or_update_ocr_data(connection, new_records):
    insert_query = """
    INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
    VALUES %s
    ON CONFLICT (file_id, project_id)  
    DO UPDATE SET 
        ocr_json_1 = EXCLUDED.ocr_json_1,
        ocr_text_1 = EXCLUDED.ocr_text_1
    """
    
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, insert_query, new_records)
        # connection.commit()


def insert_or_update_ocr_data_1(connection, new_records):
    insert_query = """
    INSERT INTO public.ocr_data (file_id, project_id, ocr_json_2, ocr_text_2)
    VALUES %s
    ON CONFLICT (file_id, project_id)  
    DO UPDATE SET 
        ocr_json_2 = EXCLUDED.ocr_json_2,
        ocr_text_2 = EXCLUDED.ocr_text_2
    """
    
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, insert_query, new_records)
        # connection.commit()


def update_file_status(connection, file_ids, status='Extracting'):
    update_status_query = "UPDATE public.files SET ocr_status = %s WHERE id = ANY(%s::int[])"
    with connection.cursor() as cursor:
        cursor.execute(update_status_query, (status, file_ids))


def fetch_ocr_data_by_file_id(connection, file_id):
    # TO DO: Update to fetch data from files table
    # Files table is expected to contain consensus document merged from multiple OCRs.
    query = "SELECT file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"
    with connection.cursor() as cur:
        cur.execute(query, (file_id,))
        response = cur.fetchone()
    return response

