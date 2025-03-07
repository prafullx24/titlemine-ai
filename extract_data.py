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



connection = get_db_connection() # Establish a database connection




def fetch_ocr_text(file_id):
    try:
        # connection = get_db_connection() 
        if connection is None:
            return None, None, None, None, "Database connection error"

        response = fetch_ocr_data_by_file_id(connection, file_id)

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
        return None, None, None, f"Error: {e}"


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
    
    Return a JSON object with the key "instrument_type" containing:
    - "value": the instrument type as a string
    - "score": confidence score (0-10) as an integer
    - "source": brief text snippet from document justifying the type
    - "summary": short explanation of why this type was chosen
    
    Example:
    {{
        "instrument_type": {{
            "value": "Deed",
            "score": 6,
            "source": "This Deed made this 1st day of January",
            "summary": "Document begins with 'This Deed', indicating a property transfer"
        }}
    }}
    
    ONLY return the JSON, nothing else.
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt_doc_type}],
            response_format={"type": "json_object"}
        )

        raw_resp = completion.choices[0].message.content.strip()
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for instrument_type: {total_tokens}")

        logging.debug(f"Raw response from OpenAI: {raw_resp}")

        try:
            json_resp = json.loads(raw_resp)
            logging.debug(f"Parsed JSON response: {json_resp}")

            if "instrument_type" not in json_resp or not all(k in json_resp["instrument_type"] for k in ["value", "score", "source", "summary"]):
                logging.warning("Incomplete 'instrument_type' data in JSON response")
                json_resp["instrument_type"] = {
                    "value": "Other",
                    "score": 50,
                    "source": "Unknown",
                    "summary": "Instrument type not clearly identified"
                }
            return json_resp
            
        

        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON: {e}")
            return {
                "instrument_type": {
                    "value": "Other",
                    "score": 50,
                    "source": "Unknown",
                    "summary": f"Failed to parse response: {e}"
                }
            }

    except Exception as e:
        logging.error(f"Error communicating with OpenAI: {e}")
        return {
            "instrument_type": {
                "value": "Other",
                "score": 1,
                "source": "Error",
                "summary": f"API error: {e}"
            }
        }

# Load Prompts (unchanged)
def load_prompts(filepath="prompts.json"):
    try:
        with open(filepath, "r") as f:
            prompts = json.load(f)
        return prompts
    except Exception as e:
        logging.error(f"Error loading prompts from {filepath}: {e}")
        return {"Other": {"fields": {"instrument_type": "string"}}}

try:
    prompts = load_prompts()
except Exception as e:
    logging.error(f"Failed to load prompts: {e}")
    prompts = {"Other": {"fields": {"instrument_type": "string"}}}

# Prompts by Instrument Type (unchanged)
def prompts_by_instrument_type(instrument_type):
    if instrument_type not in prompts:
        logging.warning(f"No prompts found for instrument type '{instrument_type}', defaulting to 'Other'")
        instrument_type = "Other"
        
    if instrument_type not in prompts:
        return json.dumps({"instrument_type": "string"})
        
    fields = prompts.get(instrument_type, {}).get("fields", {})
    return json.dumps(fields, indent=4)



#  extract_and_process_document to accept instrument_type_data
def extract_and_process_document(ocr_text, instrument_type_data):
    try:
        client = openai.OpenAI()
        
        # Ensure ocr_text is a string and handle potential slice object
        if not isinstance(ocr_text, str):
            ocr_text = str(ocr_text)
        
        # Use the passed instrument_type_data 
        instrument_type_value = instrument_type_data.get("instrument_type", {}).get("value", "")

        if not instrument_type_value:
            raise ValueError("Instrument type could not be extracted.")
        
        # Safely get prompts for the instrument type
        try:
            prompt_output = prompts_by_instrument_type(instrument_type_value)
        except Exception as prompt_error:
            logging.warning(f"Error getting prompts: {prompt_error}")
            prompt_output = json.dumps({"instrument_type": "string"})
        
        # Validate prompt_output
        try:
            json.loads(prompt_output)
        except json.JSONDecodeError:
            logging.error("Invalid prompt output, using default")
            prompt_output = json.dumps({"instrument_type": "string"})
        
        # Truncate OCR text safely
        safe_ocr_text = ocr_text[:1000] if isinstance(ocr_text, str) else str(ocr_text)[:1000]
        
        user_prompt_doc_type = f"""
        Find the following parameters in the text data added at the end of this prompt. 
        Parameters: 
        {prompt_output}
        Search in this text data: 
        {safe_ocr_text}
        """
        
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format."
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
        # logging.debug(f"Raw OpenAI response: {result}")
        # logging.info(result)
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for data extraction: {total_tokens}")
        
        try:
            result_json = json.loads(result)
            # Combine the instrument_type_data with the extracted data
            combined_result = {**instrument_type_data, **result_json}
            return combined_result
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing json from LLM: {e}")
            return {
                "error": "Invalid JSON response from OpenAI", 
                "raw_response": result, 
                **instrument_type_data
            }

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return {"error": str(e), **instrument_type_data}


def process_single_document(file_id):
    try:
        # Fetch OCR text for the given file_id
        ocr_text = fetch_ocr_text(file_id)
        if not ocr_text:
            logging.error(f"No OCR text available for file_id: {file_id}")
            return {}

        # Extract instrument type
        instrument_type_data = extract_instrument_type(ocr_text)
        if not instrument_type_data.get("instrument_type", {}).get("value"):
            logging.warning(f"Could not extract instrument type for file_id: {file_id}")
            return {}

        # Extract and process the document
        extracted_data = extract_and_process_document(ocr_text,instrument_type_data)
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
        results.append(f"File ID {file_id}: {result}")
        logging.info(f"Completed processing file ID {file_id}: {result}")
    
    return results



#  # to store the data into the database 
# def storeprocessed_extracted_data(file_id, extracted_data, project_id):
#     # Pass-through to db.py; no additional logic needed here
#     return store_extracted_data(file_id, extracted_data, project_id)





# EXTRACTED_DATA_UPSERT_QUERY = """
#     INSERT INTO public.extracted_data(
#         file_id, project_id, user_id, inst_no, instrument_type, 
#         volume_page, effective_date, execution_date, file_date, 
#         remarks, created_at, updated_at, grantor, grantee, 
#         land_description, volume_page_number, document_case_number, 
#         execution_date_extra, effective_date_extra, recording_date, 
#         property_description
#     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (file_id)
#     DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         instrument_type = EXCLUDED.instrument_type,
#         volume_page = EXCLUDED.volume_page,
#         effective_date = EXCLUDED.effective_date,
#         execution_date = EXCLUDED.execution_date,
#         updated_at = EXCLUDED.updated_at,
#         grantor = EXCLUDED.grantor,
#         grantee = EXCLUDED.grantee,
#         document_case_number = EXCLUDED.document_case_number,
#         recording_date = EXCLUDED.recording_date,
#         property_description = EXCLUDED.property_description
# """

# EXTRACTED_DATA_UPSERT_QUERY = """
#     INSERT INTO public.extracted_data(
#         id, file_id, project_id, user_id, inst_no, instrument_type, 
#         volume_page, effective_date, execution_date, file_date, 
#         remarks, created_at, updated_at, grantor, grantee, 
#         land_description, volume_page_number, document_case_number, 
#         execution_date_extra, effective_date_extra, recording_date, 
#         property_description
#     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (id)
#     DO UPDATE SET
#         file_id = EXCLUDED.file_id,
#         project_id = EXCLUDED.project_id,
#         user_id = EXCLUDED.user_id,
#         inst_no = EXCLUDED.inst_no,
#         instrument_type = EXCLUDED.instrument_type,
#         volume_page = EXCLUDED.volume_page,
#         effective_date = EXCLUDED.effective_date,
#         execution_date = EXCLUDED.execution_date,
#         file_date = EXCLUDED.file_date,
#         remarks = EXCLUDED.remarks,
#         updated_at = EXCLUDED.updated_at,
#         grantor = EXCLUDED.grantor,
#         grantee = EXCLUDED.grantee,
#         land_description = EXCLUDED.land_description,
#         volume_page_number = EXCLUDED.volume_page_number,
#         document_case_number = EXCLUDED.document_case_number,
#         execution_date_extra = EXCLUDED.execution_date_extra,
#         effective_date_extra = EXCLUDED.effective_date_extra,
#         recording_date = EXCLUDED.recording_date,
#         property_description = EXCLUDED.property_description;
# """





EXTRACTED_DATA_UPSERT_QUERY = """
    INSERT INTO public.extracted_data(
        id, file_id, project_id, user_id, inst_no, instrument_type, 
        volume_page, effective_date, execution_date, file_date, 
        remarks, created_at, updated_at, grantor, grantee, 
        land_description, volume_page_number, document_case_number, 
        execution_date_extra, effective_date_extra, recording_date, 
        property_description
    ) VALUES (
        nextval('extracted_data_id_seq'::regclass), %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
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

# def store_extracted_data(file_id, extracted_data, project_id):
#     try:
#         connection = psycopg2.connect(**config.DB_CONFIG)
#         if connection is None:
#             logger.error("Failed to connect to database")
#             return False
        
#         with connection:
#             with connection.cursor() as cur:
#                 current_time = datetime.now()
#                 instrument_type_json = json.dumps(extracted_data.get('instrument_type', {}))
#                 volume_page_json = json.dumps(extracted_data.get('volume_page', {}))
#                 effective_date_json = json.dumps(extracted_data.get('effective_date', {}))
#                 execution_date_json = json.dumps(extracted_data.get('execution_date', {}))
#                 grantor_json = json.dumps(extracted_data.get('grantor', {}))
#                 grantee_json = json.dumps(extracted_data.get('grantee', {}))
#                 document_case_number_json = json.dumps(extracted_data.get('document_case_number', {}))
#                 recording_date_json = json.dumps(extracted_data.get('recording_date', {}))
#                 property_description_json = json.dumps(extracted_data.get('property_description', {}))
#                 created_at_json = json.dumps({'timestamp': current_time.isoformat()})
#                 updated_at_json = json.dumps({'timestamp': current_time.isoformat()})

#                 cur.execute(EXTRACTED_DATA_UPSERT_QUERY, (
#                     file_id, project_id, None, None, instrument_type_json,
#                     volume_page_json, effective_date_json, execution_date_json, None,
#                     None, created_at_json, updated_at_json, grantor_json, grantee_json,
#                     None, volume_page_json, document_case_number_json, None, None,
#                     recording_date_json, property_description_json
#                 ))

#                 connection.commit()
#                 logger.info(f"Successfully stored/updated data in Extracted Data Table for file_id: {file_id}")
#                 return True
#     except (IntegrityError, OperationalError) as e:
#         logger.error(f"Database integrity or operational error: {e}")
#         connection.rollback()
#         return False
#     except Exception as e:
#         logger.error(f"Unexpected error storing data: {e}")
#         if connection:
#             connection.rollback()
#         return False
#     finally:
#         if connection:
#             connection.close()








# def store_extracted_data(file_id, extracted_data, project_id):
#     try:
#         connection = psycopg2.connect(**config.DB_CONFIG)
#         if connection is None:
#             logger.error("Failed to connect to database")
#             return False

#         with connection:
#             with connection.cursor() as cur:
#                 current_time = datetime.now()

#                 # Extract values from JSON response
#                 result = extracted_data.get("results", [{}])[0].get("result", {})

#                 inst_no = None  # Not present in JSON, setting default as None
#                 instrument_type_json = json.dumps(result.get("instrument_type", {}))
#                 volume_page_json = json.dumps(result.get("volume_page", {}))
#                 effective_date_json = json.dumps(result.get("effective_date", {}))
#                 execution_date_json = json.dumps(result.get("execution_date", {}))
#                 file_date = None  # Not present in JSON, setting default as None
#                 remarks = None  # Not present in JSON, setting default as None
#                 grantor_json = json.dumps(result.get("grantor", {}))
#                 grantee_json = json.dumps(result.get("grantee", {}))
#                 land_description = None  # Not present in JSON, setting default as None
#                 volume_page_number = None  # Not present in JSON, setting default as None
#                 document_case_number_json = json.dumps(result.get("document_case_number", {}))
#                 execution_date_extra = None  # Not present in JSON, setting default as None
#                 effective_date_extra = None  # Not present in JSON, setting default as None
#                 recording_date_json = json.dumps(result.get("recording_date", {}))
#                 property_description_json = json.dumps(result.get("property_description", {}))
#                 created_at_json = json.dumps({'timestamp': current_time.isoformat()})
#                 updated_at_json = json.dumps({'timestamp': current_time.isoformat()})

#                 # Log the values being passed to the query
#                 logger.debug(f"Values for UPSERT query: file_id={file_id}, project_id={project_id}, inst_no={inst_no}, instrument_type_json={instrument_type_json}, volume_page_json={volume_page_json}, effective_date_json={effective_date_json}, execution_date_json={execution_date_json}, file_date={file_date}, remarks={remarks}, created_at_json={created_at_json}, updated_at_json={updated_at_json}, grantor_json={grantor_json}, grantee_json={grantee_json}, land_description={land_description}, volume_page_number={volume_page_number}, document_case_number_json={document_case_number_json}, execution_date_extra={execution_date_extra}, effective_date_extra={effective_date_extra}, recording_date_json={recording_date_json}, property_description_json={property_description_json}")

#                 # Execute the UPSERT query
#                 cur.execute(EXTRACTED_DATA_UPSERT_QUERY, (
#                     file_id, project_id, None, inst_no, instrument_type_json,
#                     volume_page_json, effective_date_json, execution_date_json, file_date,
#                     remarks, created_at_json, updated_at_json, grantor_json, grantee_json,
#                     land_description, volume_page_number, document_case_number_json,
#                     execution_date_extra, effective_date_extra, recording_date_json, property_description_json
#                 ))

#                 connection.commit()
#                 logger.info(f"Successfully stored/updated data in Extracted Data Table for file_id: {file_id}")
#                 return True

#     except (IntegrityError, OperationalError) as e:
#         logger.error(f"Database integrity or operational error: {e}")
#         if connection:
#             connection.rollback()
#         return False

#     except Exception as e:
#         logger.error(f"Unexpected error storing data: {e}")
#         if connection:
#             connection.rollback()
#         return False

#     finally:
#         if connection:
#             connection.close()













def store_extracted_data(file_id, results, project_id):
    try:
        connection = psycopg2.connect(**config.DB_CONFIG)
        if connection is None:
            logger.error("Failed to connect to database")
            return False

        print("extracted_data==============",results)
        results = results.get("results", [])
        if isinstance(results, dict):
            results = results.get("results", [])
        if not results:
            logger.error(f"No results found in extracted_data for file_id: {file_id}")
            return False

        result = results[0].get("result", {})
        current_time = datetime.now()

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

        inst_no = None  
        file_date = None  
        remarks = None  
        land_description = None  
        volume_page_number = None  
        execution_date_extra = None  
        effective_date_extra = None  
        created_at_json = json.dumps({'timestamp': current_time.isoformat()})
        updated_at_json = json.dumps({'timestamp': current_time.isoformat()})

        with connection:
            with connection.cursor() as cur:
                cur.execute(EXTRACTED_DATA_UPSERT_QUERY, (
                    file_id, project_id, None, inst_no, instrument_type_json,
                    volume_page_json, effective_date_json, execution_date_json, file_date,
                    remarks, created_at_json, updated_at_json, grantor_json, grantee_json,
                    land_description, volume_page_number, document_case_number_json,
                    execution_date_extra, effective_date_extra, recording_date_json, property_description_json
                ))

                connection.commit()
                logger.info(f"Successfully stored/updated data in Extracted Data Table for file_id: {file_id}")
                return True

    except (IntegrityError, OperationalError) as e:
        logger.error(f"Database integrity or operational error: {e}")
        if connection:
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




# Flask app initialization
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
