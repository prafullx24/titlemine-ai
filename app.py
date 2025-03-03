import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import logging
from flask import Flask, jsonify
import re

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    try:
        return psycopg2.connect(Config.DATABASE_URL)
    except Exception as e:
        logging.error(f"Error getting DB connection: {e}")
        return None

def fetch_ocr_text(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, None, None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT id, file_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"
                cur.execute(query, (file_id,))
                response = cur.fetchone()

                if not response:
                    return None, None, None, "file_id not found in ocr_data table"

                id_db, file_id_from_db, ocr_json_1 = response

                try:
                    ocr_data = json.loads(ocr_json_1) if isinstance(ocr_json_1, str) else ocr_json_1
                    return id_db, file_id_from_db, ocr_data, None
                except json.JSONDecodeError:
                    return id_db, file_id_from_db, None, "Invalid JSON format"
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
    Your response must be a valid JSON object with the key "instrument_type".
    """

    user_prompt_doc_type = f"""
    Extract legal information from the following document:\n\n{ocr_text}. 
    Carefully analyze the first few lines of the document to determine the instrument type.
    Instrument Type can be one of following: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. 
    If the type is an amendment, return what kind of instrument it is amending.
    If the instrument type is not explicitly stated, return "Other".
    
    I need your response to be a valid JSON object with a single key "instrument_type". For example:
    {{"instrument_type": "Deed"}}
    
    ONLY return the JSON, nothing else.
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt_doc_type}],
            response_format={"type": "json_object"}  # Use JSON response format
        )

        # Get the raw response
        raw_resp = completion.choices[0].message.content.strip()
        total_tokens = completion.usage.total_tokens
        logging.info(f"Total Token used for instrument_type: {total_tokens}")
       
        
        try:
            # Try to parse as JSON directly
            json_resp = json.loads(raw_resp)
            # logging.info(f"Successfully parsed JSON: {json_resp}")
            
            # Check if the expected key exists
            if "instrument_type" not in json_resp:
                logging.warning("Missing 'instrument_type' key in JSON response")
                json_resp["instrument_type"] = "Other"  # Default to Other if key missing
                
            return json_resp
            
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON: {e}")
            
            # Try to extract JSON with regex as fallback
            try:
                # Look for anything that looks like JSON
                json_match = re.search(r'({.*})', raw_resp)
                if json_match:
                    potential_json = json_match.group(1)
                    json_resp = json.loads(potential_json)
                    # logging.info(f"Extracted JSON with regex: {json_resp}")
                    
                    # Check if the expected key exists
                    if "instrument_type" not in json_resp:
                        logging.warning("Missing 'instrument_type' key in extracted JSON")
                        json_resp["instrument_type"] = "Other"
                        
                    return json_resp
            except:
                pass
                
            # Try to extract just the instrument type with regex as last resort
            try:
                # Look for "instrument_type": "X" pattern
                type_match = re.search(r'"instrument_type"\s*:\s*"([^"]+)"', raw_resp)
                if type_match:
                    instrument_type = type_match.group(1)
                    # logging.info(f"Extracted instrument type with regex: {instrument_type}")
                    return {"instrument_type": instrument_type}
            except:
                pass
                
            # If all parsing attempts fail, set a default value
            logging.warning("All parsing attempts failed, defaulting to 'Other'")
            return {"instrument_type": "Other", "error": "Failed to parse model response", "raw_response": raw_resp}

    except Exception as e:
        logging.error(f"Error communicating with OpenAI: {e}")
        return {"instrument_type": "Other", "error": f"OpenAI API error: {e}"}

# Load Prompts
def load_prompts(filepath="prompts.json"):
    try:
        with open(filepath, "r") as f:
            prompts = json.load(f)
        return prompts
    except Exception as e:
        logging.error(f"Error loading prompts from {filepath}: {e}")
        # Return a default structure if prompts file is missing
        return {"Other": {"fields": {"instrument_type": "string"}}}

try:
    prompts = load_prompts()
except Exception as e:
    logging.error(f"Failed to load prompts: {e}")
    prompts = {"Other": {"fields": {"instrument_type": "string"}}}

def prompts_by_instrument_type(instrument_type):
    # Ensure we have prompts for this instrument type, default to "Other" if not
    if instrument_type not in prompts:
        logging.warning(f"No prompts found for instrument type '{instrument_type}', defaulting to 'Other'")
        instrument_type = "Other"
        
    # If "Other" is also not defined, create a minimal default
    if instrument_type not in prompts:
        return json.dumps({"instrument_type": "string"})
        
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
        
        # Fixed indentation here - moved completion inside try block
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format. The Text that you have to search this information from is at the end of the prompt."},
                {"role": "user", "content": user_prompt_doc_type}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "document_extraction",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "instrument_type": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source","summary"],
                                "additionalProperties": False
                            },
                            "volume_page": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source","summary"],
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
                                 "required": ["value", "score", "source","summary"],
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
                                "required": ["value", "score", "source","summary"],
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
                                "required": ["value", "score", "source","summary"],
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
                                "required": ["value", "score", "source","summary"],
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
                               "required": ["value", "score", "source","summary"],
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
                                 "required": ["value", "score", "source","summary"],
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
                               "required": ["value", "score", "source","summary"],
                                "additionalProperties": False
                            },
                            "reservations": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source","summary"],
                                "additionalProperties": False
                            },
                            "conditions": {
                                "type": "object",
                                "properties": {
                                    "value": {"type": "string"},
                                    "score": {"type": "integer"},
                                    "source": {"type": "string"},
                                    "summary": {"type": "string"}
                                },
                                "required": ["value", "score", "source","summary"],
                                "additionalProperties": False
                            }
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
                            "property_description",
                            "reservations",
                            "conditions"
                        ],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )
    
        result = completion.choices[0].message.content
        logging.info(result)
        total_tokens = completion.usage.total_tokens
        # logging.info(f"Total Token used for data extraction: {total_tokens}")
        try:
            result_json = json.loads(result)
            return result_json
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing json from LLM: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": result}

    except Exception as e:
        logging.error(f"Error processing document: {e}")
        return str(e)
def process_single_document(file_id):
    id_db, file_id_from_db, ocr_data, error = fetch_ocr_text(file_id)
    if error:
        logging.error(f"Error fetching OCR text for file {file_id}: {error}")
        return {"error": error}

    if not ocr_data:
        error_msg = f"No OCR data found for file_id {file_id}"
        logging.error(error_msg)
        return {"error": error_msg}
    
    ocr_text = ocr_data.get("text", "") if isinstance(ocr_data, dict) else ""
    
    if not ocr_text:
        error_msg = f"Empty OCR text for file_id {file_id}"
        logging.error(error_msg)
        return {"error": error_msg}
        
    logging.info(f"Processing document with file_id: {file_id}, OCR text length: {len(ocr_text)}")
    
    # # For debugging, log a small sample of the OCR text
    # sample_length = min(200, len(ocr_text))
    # logging.info(f"OCR text sample: {ocr_text[:sample_length]}...")
    
    extracted_data = extract_and_process_document(ocr_text)
    
    return extracted_data

# Create a simple Flask app to expose the functionality
app = Flask(__name__)

# @app.route('/extract/<file_id>', methods=['GET'])
# def extract_endpoint(file_id):
#     result = process_single_document(file_id)
#     return jsonify(result)

if __name__ == "__main__":
    # Test with a specific file ID
    file_id_to_test = "63"  # Replace with your actual file ID
    print(f"Processing document with file_id: {file_id_to_test}")
    result = process_single_document(file_id_to_test)
    print(json.dumps(result, indent=2))
    
    # Uncomment to run the Flask server instead
    # app.run(debug=True, host='0.0.0.0', port=5000)