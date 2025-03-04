import os
import json
from psycopg2 import OperationalError, IntegrityError
import psycopg2
from dotenv import load_dotenv
import openai
import logging
from flask import Flask, jsonify
import re
from datetime import datetime
from flask import Flask, jsonify
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
            return None, None, None, None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
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
    Extracts the instrument type from the provided OCR text with additional details.
    """
    client = openai.OpenAI()


    system_prompt = """
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    Your response must be a valid JSON object with a detailed "instrument_type" structure.
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

        # Log the raw response for debugging purposes
        logging.debug(f"Raw response from OpenAI: {raw_resp}")

        try:
            json_resp = json.loads(raw_resp)
            logging.debug(f"Parsed JSON response: {json_resp}")

            # Check if the required fields are present in the JSON response
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
        
        # Get instrument type data first
        instrument_type_data = extract_instrument_type(ocr_text)
        instrument_type_value = instrument_type_data.get("instrument_type", {}).get("value", "")

        if not instrument_type_value:
            raise ValueError("Instrument type could not be extracted.")
        
        prompt_output = prompts_by_instrument_type(instrument_type_value)
        try:
            json.loads(prompt_output)  # Validate prompt_output
        except json.JSONDecodeError as e:
            logging.error(f"Invalid prompt output: {e}")
            prompt_output = json.dumps({"instrument_type": "string"})  # Fallback
        
        user_prompt_doc_type = f"""
        Find the following parameters in the text data added at the end of this prompt. 
        Parameters: 
        {prompt_output}
        Search in this text data: 
        {ocr_text[:1000]}  # Truncate OCR text to the first 1000 characters
        """
        
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
                        "required": ["volume_page", "document_case_number", "execution_date", "effective_date", "recording_date", "grantee", "grantor", "property_description"],  # Specify required fields
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
            return combined_result
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing json from LLM: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": result, **instrument_type_data}

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
        extracted_data = extract_and_process_document(ocr_text)
        if "error" in extracted_data:
            logging.error(f"Error extracting data for file_id: {file_id}, {extracted_data['error']}")
            return {}

        return extracted_data

    except Exception as e:
        logging.error(f"Error processing document {file_id}: {e}")
        return {}






def clean_date_string(date_str):
    return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)  # Removes "17th" → "17"

def parse_date(date_str):
    """Convert various date formats to YYYY-MM-DD or return None if invalid."""
    if not date_str:
        return None
    date_str = clean_date_string(date_str)  # Clean input

    formats = ['%B %d, %Y', '%b %d, %Y', '%d %B %Y', '%d %b %Y']
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue  # Try next format
    logging.warning(f"Invalid date format: {date_str}")
    return None



def store_extracted_data(file_id, extracted_data, project_id):
    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Failed to connect to database")
            return False
        
        with conn:
            with conn.cursor() as cur:
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

                # Upsert query
                upsert_query = """
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
                cur.execute(upsert_query, (
                    file_id, project_id, None, None, instrument_type_json,
                    volume_page_json, effective_date_json, execution_date_json, None,
                    None, created_at_json, updated_at_json, grantor_json, grantee_json,
                    None, volume_page_json, document_case_number_json, None, None,
                    recording_date_json, property_description_json
                ))

                conn.commit()
                logging.info(f"Successfully stored/updated data for file_id: {file_id}")
                return True
    except (IntegrityError, OperationalError) as e:
        logging.error(f"Database integrity or operational error: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"Unexpected error storing data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()



def store_runsheet_data(file_id, extracted_data, project_id):
    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Failed to connect to database for runsheets")
            return False

        with conn:
            with conn.cursor() as cur:
                current_time = datetime.now().isoformat()
                instrument_type = extracted_data.get('instrument_type', {}).get('value', '')
                volume_page = extracted_data.get('volume_page', {}).get('value', '')
                document_case = extracted_data.get('document_case_number', {}).get('value', '')
                execution_date = parse_date(extracted_data.get('execution_date', {}).get('value', ''))
                effective_date = parse_date(extracted_data.get('effective_date', {}).get('value', ''))
                file_date = parse_date(extracted_data.get('recording_date', {}).get('value', ''))
                grantor = extracted_data.get('grantor', {}).get('value', '')
                grantee = extracted_data.get('grantee', {}).get('value', '')
                property_description = extracted_data.get('property_description', {}).get('value', '')

                upsert_query = """
                    INSERT INTO public.runsheets(
                        file_id, project_id, user_id, document_case, instrument_type, 
                        volume_page, effective_date, execution_date, file_date, 
                        grantor, grantee, property_description, remarks, 
                        created_at, updated_at, sort_sequence
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        project_id = EXCLUDED.project_id,
                        document_case = EXCLUDED.document_case,
                        instrument_type = EXCLUDED.instrument_type,
                        volume_page = EXCLUDED.volume_page,
                        effective_date = EXCLUDED.effective_date,
                        execution_date = EXCLUDED.execution_date,
                        file_date = EXCLUDED.file_date,
                        grantor = EXCLUDED.grantor,
                        grantee = EXCLUDED.grantee,
                        property_description = EXCLUDED.property_description,
                        updated_at = EXCLUDED.updated_at
                """
                cur.execute(upsert_query, (
                    file_id, project_id, None, document_case, instrument_type,
                    volume_page, effective_date, execution_date, file_date,
                    grantor, grantee, property_description, None,
                    current_time, current_time, None
                ))

                conn.commit()
                logging.info(f"Successfully stored/updated runsheet data for file_id: {file_id}")
                return True
    except (IntegrityError, OperationalError) as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"Unexpected error storing runsheet data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()




def fetch_file_ids_by_project(project_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
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









# Create a simple Flask app to expose the functionality
app = Flask(__name__)

@app.route('/process_project/<project_id>', methods=['GET'])
def process_project(project_id):
    try:
        # Fetch file IDs associated with the project
        file_ids, error = fetch_file_ids_by_project(project_id)
        if error:
            logging.error(f"Error fetching file IDs for project {project_id}: {error}")
            return jsonify({"error": f"Error fetching file IDs: {error}"}), 500

        if not file_ids:
            logging.info(f"No files to process for project {project_id}")
            return jsonify({"message": f"No files to process for project ID {project_id}"}), 404

        results = []
        for file_id in file_ids:
            logging.info(f"Processing file ID: {file_id}")
            result = process_single_document(file_id)

            if "error" not in result:
                store_extracted_data(file_id, result, project_id)
                store_runsheet_data(file_id, result, project_id)

            results.append({
                "file_id": file_id,
                "result": result
            })
            logging.info(f"Completed processing file ID {file_id}")

        return jsonify({
            "project_id": project_id,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as e:
        logging.error(f"Error processing project {project_id}: {e}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

if __name__ == "__main__":
    app.run(debug=True)

    # ocr_text = f"""
    # 459 office of the County Clerk in and for said County, in Book at page_ In Witness Whereof, the undersigned owner and releas or has signed this instrument this 23rd day of December 1939. W. H. Mannes THE STATE OF TEXAS, County of Harris, BEFORE ME, the undersigned, a Notary Public in and for said County and State, on this day personally appeared W. H. Mannes known to me to be the person whose name is subscribed to the foregoing instrument, and acknowledged to me that he executed the same for the purposes and consideration therein expressed. "LS "Given under my hand and seal of office this the 4th day of January, A. D. 1940. H. G. Gwinnup Notary Public in and for Harris County, Texas. Filed for record at 4:15 o'clock P. M. February 5th, A. D. 1940. Recorded at 11:50 o'clock A. M. February 15th, A. D. 1940. D. B. Huton Clk.Co.Ct.M.Co.Tex. BJ John J. Frick Deputy #6874 Partial PARTIAL RELEASE OF OIL AND GAS LEASE Rel O & GL THE STATE OF TEXAS, County of Matagorda, KNOW ALL MEN BY THESE PRESENTS: That W. H. Mannes for W. H. Mannes and in consideration of One Dollar ($1.00) and other valuable considerations, the receipt of to Tom Petrucha et ux which is hereby acknowledged, does hereby release, relinquish and surrender to Tom Petrucha and Julia Petrucha, husband and wife, their heirs or assigns, all right, title and interest in and to a certain oil and gas mining lease made and entered into by and between Tom Petrucha and Julia Petrucha, husband and wife, of Matagorda County, Texas, as lessors, and W. H. Mannes, as lessee dated the 23rd day of June, 1939, insofar as it covers the following described land in the County of Matagorda and State of Texas, to-wit: The East 50 acres of the 165.3 acre tract in the D. McFarland League, and the East 766 acres of the 1316 acre tract in the F. W. Dempsey Survey, as described in said lease. Whereas, under the terms and provisions of said lease the undersigned Lessee did on December 23, 1939, make selection of the West 115.3 acres of the 165.3 acre tract and the West 550 acres of the 1316 acre tract described in said lease and paid the rental of $665.30 into the Bay City Bank & Trust Co. of Bay City, Texas, covering the acreage selected, and said lease as to the 665.3 acres upon which the rental was paid is hereby expressly retained, and as to which the lease shall be and remain in full force and effect. This is a partial release, only, it being the intention to release back to the lessors the balance of the land not retain- ed by rental payment as first above described. said lease being recorded in the office of the County Clerk in and for said County, in Book at page _. In Witness Whereof, the undersigned owner and releasor has signed this instrument this 23rd day of December 1939. W. H. Mannes THE STATE OF TEXAS, County of Harris, BEFORE ME, the undersigned, a Notary Public in and for said County and State, on this day personally appeared W. H. Mannes known to me to be the per- son whose name is subscribed to the foregoing instrument, and acknowledged to me that he executed the same for the purposes and consideration therein expressed. "LS 11 Given under my hand and seal of office this the 4th day of January, A.D. 1940. H. G. Gwinnup Notary Public in and for Harris County, Texas. Filed for record at 4:15 o'clock P. M. February 5th, A. D. 1940. Recorded at 1:30 o'clock P. M. February 15th, A. D. 1940. D. B. shtan Clk.Co.Ct.M.Co.Tex. By John J. Frick Deputy 
    # """
    # # extract_instrument_type(ocr_text)
    # extract_and_process_document(ocr_text)