import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import logging
import config
from flask import Flask, jsonify
from db_operations.db import *

   

# Load Prompts (unchanged)
def load_prompts(filepath="prompts.json"):
    try:
        with open(filepath, "r") as f:
            prompts = json.load(f)
        return prompts
    except Exception as e:
        logging.error(f"Error loading prompts from {filepath}: {e}")
        return {f"Error loading prompts from {filepath}: {e}"}


def prompts_by_instrument_type(instrument_type):
    try:
        prompts = load_prompts()
    except Exception as e:
        logging.error(f"Failed to load prompts: {e}")
        return json.dumps({"error": f"Failed to load prompts: {e}"}, indent=4)

    types_list = prompts.get('Types', [])

    if instrument_type not in types_list:
        logging.warning(f"No prompts found for instrument type '{instrument_type}', defaulting to 'Other'")
        instrument_type = "Other"

    fields = prompts[instrument_type].get("fields", {})

    return json.dumps(fields, indent=4)

def system_prompt_by_instrument_type(instrument_type):
    try:
        prompts = load_prompts()
    except Exception as e:
        logging.error(f"Failed to load prompts: {e}")
        return json.dumps({"error": f"Failed to load prompts: {e}"}, indent=4)

    types_list = prompts.get('Types', [])

    if instrument_type not in types_list:
        logging.warning(f"No prompts found for instrument type '{instrument_type}', defaulting to 'Other'")
        instrument_type = "Other"

    system = prompts[instrument_type].get("system", {})

    return json.dumps(system, indent=4)


def fetch_ocr_data(connection, file_id): 
    # returns: file_id_from_db, project_id, ocr_json_1, error
    try:
        response = fetch_ocr_data_by_file_id(connection, file_id)

        if not response:
            return None, None, None, "file_id not found in ocr_data table"

        file_id_from_db, project_id, ocr_json_1 = response

        try:
            ocr_data = json.loads(ocr_json_1) if isinstance(ocr_json_1, str) else ocr_json_1
            return file_id_from_db, project_id, ocr_data, None
        except json.JSONDecodeError:
            return file_id_from_db, project_id, None, "Invalid JSON format in ocr_json_1 data"

    except Exception as e:
        logging.error(f"Error fetching OCR text: {e}")
        return None, None, None, f"Error: {e}"


def extract_instrument_type(ocr_text):
    client = openai.OpenAI()
    try:
        prompts = load_prompts()
    except Exception as e:
        logging.error(f"Failed to load prompts: {e}")
        return json.dumps({"error": f"Failed to load prompts: {e}"}, indent=4)
    
    system_prompt = prompts["Instrument Type"].get("system", {})
    system_prompt_ocr = system_prompt + ocr_text
    user_prompt_doc_type = json.dumps(prompts["Instrument Type"].get("fields", {}))

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt_ocr},                
                      {"role": "user", "content": user_prompt_doc_type}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "instrument_type",
                    "schema": {
                        "type": "object",
                        "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                        "required": [
                            "value", "score", "source", "summary"
                        ],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )

        raw_resp = completion.choices[0].message.content
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for instrument_type: {total_tokens}")
        logging.debug(f"Raw response from OpenAI: {raw_resp}")
        try:
            parsed_resp = json.loads(raw_resp)
        except json.JSONDecodeError as e:
            logging.info(f"Error parsing JSON: {e}")
            parsed_resp = {}  # Default to an empty dictionary

        return parsed_resp
    
    except Exception as e:
        logging.error(f"Error communicating with OpenAI in extract_instrument_type: {e}")
    
 




#  extract_and_process_document to accept instrument_type_data 
def extract_and_process_document(ocr_text, instrument_type_data):
    # Returns JSON for extracted_data table
    try:
        client = openai.OpenAI()
        
        # Ensure ocr_text is a string and handle potential slice object
        if not isinstance(ocr_text, str):
            ocr_text = str(ocr_text)
        
        # Use the passed instrument_type_data 
        instrument_type_value = instrument_type_data.get("value")

        if not instrument_type_value:
            raise ValueError("Instrument type could not be extracted.")
        
        # Safely get prompts for the instrument type
        try:
            prompt_output = prompts_by_instrument_type(instrument_type_value)
        except Exception as prompt_error:
            logging.warning(f"Error getting prompts: {prompt_error}")
            return None
        # Validate prompt_output
        try:
            json.loads(prompt_output)
        except json.JSONDecodeError:
            logging.error("Invalid prompt output from prompts.json file.")
            return None
        
        system_prompt = system_prompt_by_instrument_type(instrument_type_value)

        user_prompt_doc_type = f"""
        Find the following parameters in the text data added at the end of this prompt. 
        Parameters: 
        {prompt_output}
        Search in this text data: 
        {ocr_text}
        """
        
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": system_prompt
                },
                {
                    "role": "user", 
                    "content": user_prompt_doc_type
                }
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "document_extraction",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "volume_page": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "document_case_number": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "execution_date": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "effective_date": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "recording_date": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "grantee": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "grantor": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            },
                            "property_description": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source", "summary"],
                                "additionalProperties": False
                            }
                        },
                        "required": [
                            "volume_page", "document_case_number", 
                            "execution_date", "effective_date", 
                            "recording_date", "grantee", 
                            "grantor", "property_description"
                        ],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )
    
        result = completion.choices[0].message.content
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for data extraction: {total_tokens}")

        
        try:
            result_json = json.loads(result)
            # Combine the instrument_type_data with the extracted data
            combined_result = {
                "instrument_type": instrument_type_data,  # Renamed key
                **result_json  # Merge the rest of the data
            }
            return combined_result
        except json.JSONDecodeError as e:
            logging.error(f"Error Combining JSON for Instrument Type and Runsheet Values: {e}")
            return None

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return {"error": str(e), **instrument_type_data}


def process_single_document(connection, file_id):
    try:
        # Fetch OCR text for the given file_id
        file_id_from_db, project_id, ocr_data, Error = fetch_ocr_data(connection, file_id)

        if not ocr_data:
            logging.error(f"No OCR text available for file_id: {file_id}")
            return {}
        
        # Extract instrument type
        ocr_text = ocr_data.get("text")
        instrument_type_data = extract_instrument_type(ocr_text)
        logger.info("instrument_type_data: %s",instrument_type_data)
        if not instrument_type_data.get("value"):
            logging.warning(f"Could not extract instrument type for file_id: {file_id}")
            return {}

        # Extract and process the document
        extracted_data = extract_and_process_document(ocr_text, instrument_type_data)
        logger.info("extracted_data: %s",extracted_data)
        if "error" in extracted_data:
            logging.error(f"Error extracting data for file_id: {file_id}, {extracted_data['error']}")
            return {}

        return extracted_data
 
    except Exception as e:
        logging.error(f"Error processing document {file_id}: {e}")
        return {}
    


def fetch_file_ids_by_project(project_id):
    try:
        connection = psycopg2.connect(**config.DB_CONFIG)
        if connection is None:
            return None, "Database connection error"

        with connection:
            with connection.cursor() as cur:
                query = "SELECT id FROM public.files WHERE project_id = %s AND ocr_status = 'Extracting'"
                cur.execute(query, (project_id,))
                file_ids = cur.fetchall()

                if not file_ids:
                    return [], "No file IDs found for this project ID"
                
                return [row[0] for row in file_ids], None
    except Exception as e:
        logging.error(f"Error fetching file IDs for project {project_id}: {e}")
        return None, str(e)

def process_documents_by_project(project_id):
    file_ids, error = fetch_file_ids_by_project(project_id)
    if error:
        logging.error(f"Error: {error}")
        return [error]
    
    if not file_ids:
        logging.info(f"No files to process for project ID {project_id}")
        return ["No files to process"]

    results = []
    for file_id in file_ids:
        logging.info(f"Processing file ID: {file_id}")
        result = process_single_document(file_id)
        # results.append(f"File ID {file_id}: {result}")

        logging.info(f"Completed processing file ID {file_id}: {result}")
    
    return results




def store_extracted_data_old(file_id, results, project_id):
    try:
        results = results.get("results", [])
        if isinstance(results, dict):
            results = results.get("results", [])
        if not results:
            logger.error(f"No results found in extracted_data for file_id: {file_id}")
            return False

        result = results[0].get("result", {})

        def get_json_field(field_name):
            return json.dumps(result.get(field_name, {}))

        instrument_type_json = get_json_field("instrument_type")
        volume_page_json = get_json_field("volume_page")
        effective_date_json = get_json_field("effective_date")
        execution_date_json = get_json_field("execution_date")
        grantor_json = get_json_field("grantor")
        grantee_json = get_json_field("grantee")
        document_case_number_json = get_json_field("document_case_number")
        recording_date_json = get_json_field("recording_date")
        property_description_json = get_json_field("property_description")

        log_result = [ file_id, project_id, instrument_type_json,
        volume_page_json, effective_date_json, execution_date_json, grantor_json, grantee_json, document_case_number_json,
        recording_date_json, property_description_json]

        logger.info(log_result)

    except Exception as e:
        logger.error(f"Unexpected error storing data: {e}")
        return False



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
                    # logger.info(f"[store_runsheet_data] Loaded top-level keys from prompts.json: {top_level_keys}")
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


def insert_extracted_data(file_id, project_id, result, connection):
    """ Inserts extracted JSON data into the extracted_data table """
    try:
        # Helper function to get full JSON
        def get_json_field(field_name):
            return json.dumps(result.get(field_name, {}))

        # Prepare JSON fields for extracted_data table
        instrument_type_json = get_json_field("instrument_type")
        volume_page_json = get_json_field("volume_page")
        effective_date_json = get_json_field("effective_date")
        execution_date_json = get_json_field("execution_date")
        grantor_json = get_json_field("grantor")
        grantee_json = get_json_field("grantee")
        document_case_number_json = get_json_field("document_case_number")
        recording_date_json = get_json_field("recording_date")  # recording_date is file_date
        property_description_json = get_json_field("property_description")

        # SQL query for inserting JSON data into extracted_data
        insert_extracted_data_query = """
            INSERT INTO public.extracted_data(
                file_id, project_id, instrument_type, volume_page, 
                effective_date, execution_date, file_date, grantor, grantee, 
                property_description, document_case_number
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id) DO UPDATE 
            SET instrument_type = EXCLUDED.instrument_type, 
                volume_page = EXCLUDED.volume_page, 
                effective_date = EXCLUDED.effective_date, 
                execution_date = EXCLUDED.execution_date, 
                file_date = EXCLUDED.file_date,
                grantor = EXCLUDED.grantor,
                grantee = EXCLUDED.grantee,
                property_description = EXCLUDED.property_description,
                document_case_number = EXCLUDED.document_case_number
            RETURNING file_id;
        """

        extracted_data_values = (
            file_id, project_id, instrument_type_json, volume_page_json,
            effective_date_json, execution_date_json, recording_date_json,
            grantor_json, grantee_json, property_description_json,
            document_case_number_json
        )

        # Execute the query
        with connection.cursor() as cursor:
            cursor.execute(insert_extracted_data_query, extracted_data_values)
        
        logger.info(f"Inserted into extracted_data for file_id: {file_id}")

        return True

    except Exception as e:
        connection.rollback()
        logger.error(f"Error inserting extracted data for file_id {file_id}: {e}")
        return False


def insert_runsheet_data(file_id, project_id, result, connection):
    """ Inserts extracted value fields into the runsheets table """
    try:
        # Helper function to get only the "value" field
        def get_value_field(field_name):
            return result.get(field_name, {}).get("value", None)

        # Prepare value fields for runsheets table
        instrument_type_value = get_value_field("instrument_type")
        volume_page_value = get_value_field("volume_page")
        effective_date_value = get_value_field("effective_date")
        execution_date_value = get_value_field("execution_date")
        grantor_value = get_value_field("grantor")
        grantee_value = get_value_field("grantee")
        document_case_number_value = get_value_field("document_case_number")
        recording_date_value = get_value_field("recording_date")  # recording_date is file_date
        property_description_value = get_value_field("property_description")

        # SQL query for inserting "value" data into runsheets
        insert_runsheet_query = """
            INSERT INTO public.runsheets(
                project_id, file_id, instrument_type, volume_page, 
                document_case, execution_date, effective_date, file_date, 
                grantor, grantee, property_description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id, project_id) DO UPDATE 
            SET instrument_type = EXCLUDED.instrument_type, 
                volume_page = EXCLUDED.volume_page, 
                effective_date = EXCLUDED.effective_date, 
                execution_date = EXCLUDED.execution_date, 
                file_date = EXCLUDED.file_date,
                grantor = EXCLUDED.grantor,
                grantee = EXCLUDED.grantee,
                property_description = EXCLUDED.property_description,
                document_case = EXCLUDED.document_case
            RETURNING file_id;
        """

        runsheet_values = (
            project_id, file_id, instrument_type_value, volume_page_value,
            document_case_number_value, execution_date_value, effective_date_value,
            recording_date_value, grantor_value, grantee_value, property_description_value
        )

        # Execute the query
        with connection.cursor() as cursor:
            cursor.execute(insert_runsheet_query, runsheet_values)
        
        logger.info(f"Inserted into runsheets for file_id: {file_id}")

        return True

    except Exception as e:
        connection.rollback()
        logger.error(f"Error inserting runsheet data for file_id {file_id}: {e}")
        return False


def store_extracted_data(file_id, result, project_id, connection):
    """ Calls both functions to insert data into extracted_data and runsheets """
    try:
        # extracted_success = insert_extracted_data(file_id, project_id, result, connection)
        runsheet_success = insert_runsheet_data(file_id, project_id, result, connection)

        # if extracted_success and runsheet_success:
        if runsheet_success:
            connection.commit()
            logger.info(f"Successfully stored extracted data for file_id: {file_id}")
            return True
        else:
            connection.rollback()
            logger.error(f"Failed to store extracted data for file_id: {file_id}")
            return False

    except Exception as e:
        connection.rollback()
        logger.error(f"Unexpected error storing data for file_id {file_id}: {e}")
        return False




# Flask app initialization
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
