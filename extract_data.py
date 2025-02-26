import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import concurrent.futures
import logging


# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# # Configure logging
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
    Extracts the instrument type from the provided OCR text using OpenAI's GPT-4o-mini.
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
            model="gpt-4o-mini",
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
        user_prompt_doc_type = f"""
        Find the following parameters in the text data added at the end of this prompt. 
        Parameters: 
        {prompt_output}
        Search in this text data: 
        {ocr_text} 
        """
        
        print(user_prompt_doc_type)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format. The Text that you have to search this information from is at the end of the prompt."},
                      {"role": "user", "content": user_prompt_doc_type}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "document_extraction",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "instrument_type": {"type": "string"},
                            "volume_page": {"type": "string"},
                            "document_case_number": {"type": "string"},
                            "execution_date": {"type": "string"},
                            "effective_date": {"type": "string"},
                            "recording_date": {"type": "string"},
                            "grantee": {"type": "string"},
                            "grantor": {"type": "string"},
                            "property_description": {"type": "string"}
                        },
                        "required": [
                            "instrument_type",
                            "volume_page",
                            "document_case_number",
                            "execution_date",
                            "effective_date",
                            "recording_date",
                            "grantee",
                            "grantor",
                            "property_description"
                        ],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )
        result = response.choices[0].message.content
        print(result)
        try:
            result_json = json.loads(result)
            return result_json
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing json from LLM: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": result}

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return str(e)

def store_extracted_data(user_id, file_id, project_id, extracted_data):
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
                    remarks = "N/A" 
                    file_date = recording_date #add file_date if needed.

                    check_query = "SELECT file_name FROM public.runsheets WHERE file_id = %s and project_id = %s"
                    existing_file_name = cur.execute(check_query, (file_id, project_id))

                    if existing_file_name:
                        update_query = """
                            UPDATE public.runsheets 
                            SET 
                                instrument_type = COALESCE(%s, instrument_type), 
                                document_case = COALESCE(%s, document_case), 
                                volume_page = COALESCE(%s, volume_page), 
                                effective_date = COALESCE(%s, effective_date),
                                execution_date = COALESCE(%s, execution_date), 
                                file_date = COALESCE(%s, file_date), 
                                grantor = COALESCE(%s, grantor), 
                                grantee = COALESCE(%s, grantee), 
                                property_description = COALESCE(%s, property_description), 
                                remarks = COALESCE(%s, remarks), 
                                user_id = COALESCE(user_id, user_id),
                                file_name = COALESCE(%s, file_name)
                            WHERE file_id = %s AND project_id = %s
                            """

                        cur.execute(update_query, (
                            instrument_type, document_case, volume_page, effective_date,
                            execution_date, file_date, grantor, grantee, property_description, remarks, existing_file_name, file_id, project_id
                        ))
                    else:
                        insert_query = """
                        INSERT INTO public.runsheets (file_id, project_id, instrument_type, document_case, volume_page, 
                            effective_date, execution_date, file_date, grantor, grantee, property_description, remarks, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        insert_output = cur.execute(insert_query, (
                            file_id, project_id, instrument_type, document_case, volume_page,
                            effective_date, execution_date, file_date, grantor, grantee, property_description, remarks, user_id
                        ))
                        # return "Runsheet Entry not found."

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

def fetch_user_id(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT user_id FROM files WHERE files.id = %s"

                try:
                    cur.execute(query, (int(file_id),))  # If file_id is an integer
                except ValueError:
                    cur.execute(query, (file_id,))  # If file_id is a string

                response = cur.fetchone()

                if not response:
                    return "user_id not found in files table"
                else:
                    return response
    except Exception as e:
        logging.error(f"Error fetching user_id: {e}")
        return None, str(e)

def process_single_document(file_id):
    file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(file_id)
    if error:
        logging.error(f"Error fetching OCR text for file {file_id}: {error}")
        return error

    if not ocr_data:
        return f"No OCR data found for file_id {file_id}"

    ocr_text = ocr_data.get("text", "")
    extracted_data = extract_and_process_document(ocr_text)
    if "error" in extracted_data:
        logging.error(f"Error processing document {file_id}: {extracted_data}")
        return f"Error processing document {file_id}: {extracted_data.get('error')}"
    user_id = fetch_user_id(file_id)
    result = store_extracted_data(user_id, file_id_from_db, project_id, extracted_data)
    return result

# Example usage: process multiple file IDs concurrently
# file_ids_to_process = [
#     28, 33, 34, 36, 37, 38, 39, 41, 42, 43, 47, 48, 49, 50, 51, 52, 53, 
#     56, 57, 58, 59, 61, 62, 63, 65, 66, 67, 69, 70, 71, 72, 73, 74, 75, 
#     76, 77, 78, 80, 81, 82, 84, 87, 89, 91
# ]

file_ids_to_process = [527]  
results = process_documents_concurrently(file_ids_to_process)


for result in results:
    logging.info(f"Processing result: {result}")
