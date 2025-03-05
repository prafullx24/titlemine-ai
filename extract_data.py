import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import concurrent.futures
import logging
import config
from flask import Flask, jsonify
from db_operations.db import *
# from psycopg2 import OperationalError, IntegrityError
from db_operations.db import *


connection = get_db_connection() # Establish a database connection


def fetch_ocr_text(file_id):
    try:
         #   conn = get_db_connection()
        connection = get_db_connection() 
        if connection is None:
            return None, None, None, None, "Database connection error"

        with connection:
            with connection.cursor() as cur:
                query = "SELECT file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"
                cur.execute(query, (file_id,))
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
            print(json_resp)
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
        logging.debug(f"Raw OpenAI response: {result}")
        logging.info(result)
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for data extraction: {total_tokens}")
        
        try:
            result_json = json.loads(result)
            # Combine the instrument_type_data with the extracted data
            combined_result = {**instrument_type_data, **result_json}
            print(combined_result)
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



 # to store the data into the database 
def storeprocessed_extracted_data(file_id, extracted_data, project_id):
    # Pass-through to db.py; no additional logic needed here
    return store_extracted_data(file_id, extracted_data, project_id)


# Flask app initialization
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# @app.route('/process_project/<project_id>', methods=['GET'])
# def process_project(project_id):
#     try:
#         # Fetch file IDs associated with the project
#         file_ids, error = fetch_file_ids_by_project(project_id)
#         if error:
#             logging.error(f"Error fetching file IDs for project {project_id}: {error}")
#             return jsonify({"error": f"Error fetching file IDs: {error}"}), 500

#         if not file_ids:
#             logging.info(f"No files to process for project {project_id}")
#             return jsonify({"message": f"No files to process for project ID {project_id}"}), 404

#         results = []
#         for file_id in file_ids:
#             logging.info(f"Processing file ID: {file_id}")
#             result = process_single_document(file_id)

#             if "error" not in result:
#                 storeprocessed_extracted_data(file_id, result, project_id)
#                 store_runsheet_data(file_id, result, project_id)

#             results.append({
#                 "file_id": file_id,
#                 "result": result
#             })
#             logging.info(f"Completed processing file ID {file_id}")

#         return jsonify({
#             "project_id": project_id,
#             "results": results,
#             "timestamp": datetime.now().isoformat()
#         }), 200

#     except Exception as e:
#         logging.error(f"Error processing project {project_id}: {e}")
#         return jsonify({
#             "error": str(e),
#             "timestamp": datetime.now().isoformat()
#         }), 500



if __name__ == '__main__':  
    app.run(debug=True, host='0.0.0.0', port=5000)
    #   app.run(debug=True)