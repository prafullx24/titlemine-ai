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






# Functions to store the extracted data from the file Exracted data.py


# for extracting data
# Define queries as constants
EXTRACTED_DATA_UPSERT_QUERY = """
    INSERT INTO public.extracted_data(
        file_id, project_id, user_id, inst_no, instrument_type, 
        volume_page, effective_date, execution_date, file_date, 
        remarks, created_at, updated_at, grantor, grantee, 
        land_description, volume_page_number, document_case_number, 
        execution_date_extra, effective_date_extra, recording_date, 
        property_description
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (file_id)
    DO UPDATE SET
        project_id = EXCLUDED.project_id,
        instrument_type = EXCLUDED.instrument_type,
        volume_page = EXCLUDED.volume_page,
        effective_date = EXCLUDED.effective_date,
        execution_date = EXCLUDED.execution_date,
        updated_at = EXCLUDED.updated_at,
        grantor = EXCLUDED.grantor,
        grantee = EXCLUDED.grantee,
        document_case_number = EXCLUDED.document_case_number,
        recording_date = EXCLUDED.recording_date,
        property_description = EXCLUDED.property_description
"""

# Function to store insert the extracted data in the Extracted data table 
def store_extracted_data(file_id, extracted_data, project_id):
    try:
        connection = psycopg2.connect(**config.DB_CONFIG)
        if connection is None:
            logger.error("Failed to connect to database")
            return False
        
        with connection:
            with connection.cursor() as cur:
                current_time = datetime.now()
                instrument_type_json = json.dumps(extracted_data.get('instrument_type', {}))
                volume_page_json = json.dumps(extracted_data.get('volume_page', {}))
                effective_date_json = json.dumps(extracted_data.get('effective_date', {}))
                execution_date_json = json.dumps(extracted_data.get('execution_date', {}))
                grantor_json = json.dumps(extracted_data.get('grantor', {}))
                grantee_json = json.dumps(extracted_data.get('grantee', {}))
                document_case_number_json = json.dumps(extracted_data.get('document_case_number', {}))
                recording_date_json = json.dumps(extracted_data.get('recording_date', {}))
                property_description_json = json.dumps(extracted_data.get('property_description', {}))
                created_at_json = json.dumps({'timestamp': current_time.isoformat()})
                updated_at_json = json.dumps({'timestamp': current_time.isoformat()})

                cur.execute(EXTRACTED_DATA_UPSERT_QUERY, (
                    file_id, project_id, None, None, instrument_type_json,
                    volume_page_json, effective_date_json, execution_date_json, None,
                    None, created_at_json, updated_at_json, grantor_json, grantee_json,
                    None, volume_page_json, document_case_number_json, None, None,
                    recording_date_json, property_description_json
                ))

                connection.commit()
                logger.info(f"Successfully stored/updated data in Extracted Data Table for file_id: {file_id}")
                return True
    except (IntegrityError, OperationalError) as e:
        logger.error(f"Database integrity or operational error: {e}")
        connection.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error storing data: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            connection.close()


# function to store the extracted values in the Runsheets table 
def clean_date_string(date_str):
    """Remove ordinal suffixes from date strings (e.g., '17th' → '17')."""
    return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

def parse_date(date_str):
    """Convert various date formats to YYYY-MM-DD or return None if invalid."""
    if not date_str or date_str.strip().upper() in ["N/A", "NONE", ""]:
        logger.debug(f"Skipping date parsing for placeholder value: {date_str}")
        return None

    date_str = clean_date_string(date_str)  # Clean input

    formats = ['%B %d, %Y', '%b %d, %Y', '%d %B %Y', '%d %b %Y']
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue  # Try next format

    logger.warning(f"Invalid date format: {date_str}")
    return None

def store_runsheet_data(file_id, extracted_data, project_id):
    """Insert extracted data into the runsheets table."""
    connection = None
    try:
        connection = psycopg2.connect(**config.DB_CONFIG)
        if connection is None or connection.closed:
            logger.error("[store_runsheet_data] Failed to connect to database for runsheets")
            return False
        
        logger.debug(f"[store_runsheet_data] Extracted data for file_id {file_id}: {extracted_data}")
        
        with connection:
            with connection.cursor() as cur:
                # Check sequence vs max id
                cur.execute("SELECT MAX(id) FROM public.runsheets")
                max_id = cur.fetchone()[0] or 0  # Default to 0 if table is empty
                cur.execute("SELECT last_value FROM public.runsheets_id_seq")
                seq_value = cur.fetchone()[0]
                logger.debug(f"[store_runsheet_data] Max id: {max_id}, Sequence value: {seq_value}")

                if seq_value <= max_id:
                    logger.warning(f"[store_runsheet_data] Sequence out of sync (seq={seq_value}, max_id={max_id}). Resetting sequence.")
                    cur.execute("SELECT setval('public.runsheets_id_seq', %s, false)", (max_id + 1,))
                    logger.info(f"[store_runsheet_data] Sequence reset to {max_id + 1}")

                current_time = datetime.now().isoformat()

                # Extract data
                instrument_type = extracted_data.get('instrument_type', {}).get('value', '')
                volume_page = extracted_data.get('volume_page', {}).get('value', '')
                document_case = extracted_data.get('document_case_number', {}).get('value', '')
                execution_date = parse_date(extracted_data.get('execution_date', {}).get('value', ''))
                effective_date = parse_date(extracted_data.get('effective_date', {}).get('value', ''))
                file_date = parse_date(extracted_data.get('recording_date', {}).get('value', ''))
                grantor = extracted_data.get('grantor', {}).get('value', '')
                grantee = extracted_data.get('grantee', {}).get('value', '')
                property_description = extracted_data.get('property_description', {}).get('value', '')

                # Validate instrument_type against prompts.json
                try:
                    with open("prompts.json", "r", encoding="utf-8") as file:
                        data = json.load(file)
                    top_level_keys = list(data.keys())
                    logger.info(f"[store_runsheet_data] Loaded top-level keys from prompts.json: {top_level_keys}")
                    if instrument_type and instrument_type not in top_level_keys:
                        logger.warning(f"[store_runsheet_data] Instrument type '{instrument_type}' not found in prompts.json. Setting to 'Other'.")
                        instrument_type = "Other"
                except FileNotFoundError:
                    logger.warning("[store_runsheet_data] prompts.json file not found. Proceeding with extracted instrument_type.")
                except json.JSONDecodeError as e:
                    logger.error(f"[store_runsheet_data] Error decoding prompts.json: {e}. Proceeding with extracted instrument_type.")

                # Simple INSERT query
                insert_query = """
                    INSERT INTO public.runsheets (
                        file_id, project_id, user_id, document_case, instrument_type, 
                        volume_page, effective_date, execution_date, file_date, 
                        grantor, grantee, property_description, remarks, 
                        created_at, updated_at, sort_sequence
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                params = (
                    file_id, project_id, None, document_case, instrument_type,
                    volume_page, effective_date, execution_date, file_date,
                    grantor, grantee, property_description, None,
                    current_time, current_time, None
                )

                logger.debug(f"[store_runsheet_data] Executing query: {insert_query}")
                logger.debug(f"[store_runsheet_data] With parameters: {params}")

                cur.execute(insert_query, params)
                result = cur.fetchone()  # Get the returned id
                rows_affected = cur.rowcount

                if rows_affected == 0:
                    logger.warning("[store_runsheet_data] Query executed but no rows were inserted.")
                    return False
                else:
                    inserted_id = result[0] if result else None
                    logger.info(f"[store_runsheet_data] Inserted new row with ID: {inserted_id}")

                connection.commit()
                logger.info(f"[store_runsheet_data] Successfully inserted runsheet data for file_id: {file_id}")
                return True
    except (IntegrityError, OperationalError) as e:
        logger.error(f"[store_runsheet_data] Database error: {e}")
        if connection:
            connection.rollback()
        return False
    except Exception as e:
        logger.error(f"[store_runsheet_data] Unexpected error storing runsheet data: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection and not connection.closed:
            connection.close()