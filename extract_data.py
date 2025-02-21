import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import time
import concurrent.futures
import logging

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get a database connection
def get_db_connection():
    try:
        return psycopg2.connect(Config.DATABASE_URL)
    except Exception as e:
        logging.error(f"Error getting DB connection: {e}")
        return None

# Function to fetch OCR text from the database
def fetch_ocr_text(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"

                try:
                    cur.execute(query, (int(file_id),))  # If file_id is an integer
                except ValueError:
                    cur.execute(query, (file_id,))  # If file_id is a string

                response = cur.fetchone()

                if not response:
                    return None, None, None, "file_id not found in ocr_data table"

                file_id_from_db, project_id, ocr_json_1 = response

                try:
                    ocr_data = json.loads(ocr_json_1) if isinstance(ocr_json_1, str) else ocr_json_1
                    return file_id_from_db, project_id, ocr_data, None
                except json.JSONDecodeError:
                    return file_id_from_db, project_id, None, "Invalid JSON format"
    except Exception as e:
        logging.error(f"Error fetching OCR text: {e}")
        return None, str(e)

def extract_instrument_type(ocr_text):
    """
    Extracts the instrument type from the provided OCR text using OpenAI's GPT-3.5-turbo.
    """
    client = openai.OpenAI()

    system_prompt = """
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """

    user_prompt_doc_type = f"""
    Extract legal information from the following document:\n\n{ocr_text}. 
    Carefully analyze the first few lines of the document to determine the instrument type.
    Instrument Type can be one of following: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. 
    If the type is an amendment, return what kind of instrument it is amending.
    If the instrument type is not explicitly stated, return "Other".
    Please return the result as a JSON object with a key named "instrument_type".
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt_doc_type}]
        )

        resp = completion.choices[0].message.content.strip("```").lstrip("json\n").strip()

        try:
            json_resp = json.loads(resp)
            return json_resp
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": resp}

    except Exception as e:
        logging.error(f"Error communicating with OpenAI: {e}")
        return {"error": f"OpenAI API error: {e}"}

# Load Prompts
def load_prompts(filepath="prompts.json"):
    with open(filepath, "r") as f:
        prompts = json.load(f)
    return prompts

prompts = load_prompts()

def prompts_by_instrument_type(instrument_type):
    fields = prompts.get(instrument_type, {}).get("fields", {})
    return json.dumps(fields, indent=4)

def extract_and_process_document(ocr_text):
    try:
        client = openai.OpenAI()
        instrument_type_data = extract_instrument_type(ocr_text)
        instrument_type = instrument_type_data.get("instrument_type", "")
        if not instrument_type:
            raise ValueError("Instrument type could not be extracted.")
        prompt_output = prompts_by_instrument_type(instrument_type)
        user_prompt_doc_type = f"""{prompt_output} according to these parameters, find the corresponding information and return the values in similar json."""
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format."},
                      {"role": "user", "content": user_prompt_doc_type}]
        )
        result = response.choices[0].message.content.strip("```").lstrip("json\n").strip()
        try:
            result_json = json.loads(result)
            return result_json
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing json from LLM: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": result}

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return str(e)

def store_extracted_data(file_id, project_id, extracted_data):
    try:
        conn = get_db_connection()
        if conn is None:
            return "Database connection error"

        with conn:
            with conn.cursor() as cur:
                try:
                    conn.autocommit = False

                    def convert_date(date_str):
                        if date_str and date_str.lower() != "none found" and date_str.lower() != "n/a":
                            try:
                                return datetime.strptime(date_str, "%B %d, %Y").date()
                            except ValueError:
                                return None
                        return None

                    if isinstance(extracted_data, str):
                        try:
                            extracted_data = json.loads(extracted_data)
                        except json.JSONDecodeError:
                            return "Invalid JSON data"

                    execution_date = convert_date(extracted_data.get("execution_date"))
                    effective_date = convert_date(extracted_data.get("effective_date"))
                    recording_date = convert_date(extracted_data.get("recording_date"))

                    instrument_type = extracted_data.get("instrument_type", "N/A")
                    volume_page = extracted_data.get("volume_page", "N/A")
                    document_case = extracted_data.get("document_case_number", "N/A")
                    grantor = extracted_data.get("grantor", "N/A")
                    grantee = json.dumps(extracted_data.get("grantee", []))
                    property_description = json.dumps(extracted_data.get("property_description", []))
                    sort_sequence = 0
                    remarks = "N/A" #add remarks if needed.
                    file_date = recording_date #add file_date if needed.

                    check_query = "SELECT COUNT(*) FROM public.runsheets WHERE file_id = %s"
                    cur.execute(check_query, (file_id,))
                    exists = cur.fetchone()[0] > 0

                    if exists:
                        update_query = """
                        UPDATE public.runsheets SET 
                            instrument_type = %s, document_case=%s, volume_page = %s, effective_date = %s,
                            execution_date = %s, file_date =%s, grantor = %s, grantee = %s, property_description = %s, remarks = %s
                        WHERE file_id = %s
                        """
                        cur.execute(update_query, (
                            instrument_type, document_case, volume_page, effective_date,
                            execution_date, file_date, grantor, grantee, property_description, remarks, file_id
                        ))
                    else:
                        insert_query = """
                        INSERT INTO public.runsheets (file_id, project_id, instrument_type, document_case, volume_page, 
                            effective_date, execution_date, file_date, grantor, grantee, property_description, remarks)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(insert_query, (
                            file_id, project_id, instrument_type, document_case, volume_page,
                            effective_date, execution_date, file_date, grantor, grantee, property_description, remarks
                        ))

                    conn.commit()
                    return "Data successfully stored/updated."
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error storing extracted data: {e}")
                    return f"Error storing data: {e}"

    except Exception as e:
        logging.error(f"Error with DB operation: {e}")
        return f"DB error: {e}"

# Concurrent execution for handling multiple documents
def process_documents_concurrently(file_ids):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(process_single_document, file_ids))
    return results

def process_single_document(file_id):
    file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(file_id)
    if error:
        logging.error(f"Error fetching OCR text for file {file_id}: {error}")
        return error

    if not ocr_data:
        return f"No OCR data found for file_id {file_id}"

    extracted_data = extract_and_process_document(ocr_data)
    if "error" in extracted_data:
        logging.error(f"Error processing document {file_id}: {extracted_data}")
        return f"Error processing document {file_id}: {extracted_data.get('error')}"

    result = store_extracted_data(file_id_from_db, project_id, extracted_data)
    return result

# Example usage: process multiple file IDs concurrently
file_ids_to_process = [66,67,77]  
results = process_documents_concurrently(file_ids_to_process)

for result in results:
    logging.info(f"Processing result: {result}")
