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
        logging.warning(f"instrument type: {instrument_type}")
        if not instrument_type:
            raise ValueError("Instrument type could not be extracted.")
        prompt_output = prompts_by_instrument_type(instrument_type)
        logging.warning(f"prompt_output: {prompt_output}")
        user_prompt_doc_type = f"""
        Find the following parameters in the text data added at the end of this prompt. 
        Parameters: 
        {prompt_output}
        Search in this text data: 
        {ocr_text} 
        """
        logging.warning(f"user_prompt_doc_type: {user_prompt_doc_type}")
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
    
    extracted_data = extract_and_process_document(ocr_text)
    return extracted_data

def store_extracted_data(file_id, extracted_data, project_id):
    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Failed to connect to database")
            return False
        
        with conn:
            with conn.cursor() as cur:
                current_time = datetime.now()

                # Convert extracted fields to JSON strings
                instrument_type_json = json.dumps(extracted_data.get('instrument_type', {}))
                volume_page_json = json.dumps(extracted_data.get('volume_page', {}))
                effective_date_json = json.dumps(extracted_data.get('effective_date', {}))
                execution_date_json = json.dumps(extracted_data.get('execution_date', {}))
                grantor_json = json.dumps(extracted_data.get('grantor', {}))
                grantee_json = json.dumps(extracted_data.get('grantee', {}))
                document_case_number_json = json.dumps(extracted_data.get('document_case_number', {}))
                recording_date_json = json.dumps(extracted_data.get('recording_date', {}))
                property_description_json = json.dumps(extracted_data.get('property_description', {}))

                # Convert timestamps to JSON
                created_at_json = json.dumps({'timestamp': current_time.isoformat()})
                updated_at_json = json.dumps({'timestamp': current_time.isoformat()})

                # Corrected INSERT query (removed 'id' column)
                insert_query = """
                    INSERT INTO public.extracted_data(
                        file_id, project_id, user_id, inst_no, instrument_type, 
                        volume_page, effective_date, execution_date, file_date, 
                        remarks, created_at, updated_at, grantor, grantee, 
                        land_description, volume_page_number, document_case_number, 
                        execution_date_extra, effective_date_extra, recording_date, 
                        property_description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """  # 21 placeholders

                # Insert values (21 total)
                cur.execute(insert_query, (
                    file_id,
                    project_id,
                    None,  # user_id
                    None,  # inst_no
                    instrument_type_json,
                    volume_page_json,
                    effective_date_json,
                    execution_date_json,
                    None,  # file_date
                    None,  # remarks
                    created_at_json,
                    updated_at_json,
                    grantor_json,
                    grantee_json,
                    None,  # land_description
                    volume_page_json,  # volume_page_number
                    document_case_number_json,
                    None,  # execution_date_extra
                    None,  # effective_date_extra
                    recording_date_json,
                    property_description_json
                ))  # 21 values

                conn.commit()
                logging.info(f"Successfully stored data for file_id: {file_id}")
                return True
    except Exception as e:
        logging.error(f"Error storing data in database: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()





def parse_date(date_str):
    """Convert a date string in 'Month Day, Year' format to YYYY-MM-DD or return None if invalid."""
    if not date_str:
        return None
    try:
        # Handle 'April 17, 1880' format
        parsed_date = datetime.strptime(date_str, '%B %d, %Y')
        return parsed_date.strftime('%Y-%m-%d')  # Convert to '1880-04-17'
    except ValueError as e:
        logging.warning(f"Invalid date format: {date_str}, error: {e}")
        return None

def store_runsheet_data(file_id, extracted_data, project_id):
    """
    Store simplified extracted data into the runsheets table, using only the 'value' fields.
    """
    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Failed to connect to database for runsheets")
            return False

        with conn:
            with conn.cursor() as cur:
                current_time = datetime.now()

                # Extract and parse fields
                instrument_type = extracted_data.get('instrument_type', {}).get('value', '')
                volume_page = extracted_data.get('volume_page', {}).get('value', '')
                document_case = extracted_data.get('document_case_number', {}).get('value', '')
                execution_date = parse_date(extracted_data.get('execution_date', {}).get('value', ''))
                effective_date = parse_date(extracted_data.get('effective_date', {}).get('value', ''))
                file_date = parse_date(extracted_data.get('recording_date', {}).get('value', ''))
                grantor = extracted_data.get('grantor', {}).get('value', '')
                grantee = extracted_data.get('grantee', {}).get('value', '')
                property_description = extracted_data.get('property_description', {}).get('value', '')

                # Log the parsed dates for debugging
                logging.debug(f"Parsed dates: execution_date={execution_date}, "
                             f"effective_date={effective_date}, file_date={file_date}")

                insert_query = """
                    INSERT INTO public.runsheets(
                        file_id, project_id, user_id, document_case, instrument_type, 
                        volume_page, effective_date, execution_date, file_date, 
                        grantor, grantee, property_description, remarks, 
                        created_at, updated_at, sort_sequence
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                cur.execute(insert_query, (
                    file_id,
                    project_id,
                    None,  # user_id
                    document_case,
                    instrument_type,
                    volume_page,
                    effective_date,
                    execution_date,
                    file_date,
                    grantor,
                    grantee,
                    property_description,
                    None,  # remarks
                    current_time.isoformat(),
                    current_time.isoformat(),
                    None   # sort_sequence
                ))

                conn.commit()
                logging.info(f"Successfully stored runsheet data for file_id: {file_id}")
                return True

    except Exception as e:
        logging.error(f"Error storing runsheet data in database: {e}")
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
    # app.run(debug=True)

    ocr_text = f"""
    459 office of the County Clerk in and for said County, in Book at page_ In Witness Whereof, the undersigned owner and releas or has signed this instrument this 23rd day of December 1939. W. H. Mannes THE STATE OF TEXAS, County of Harris, BEFORE ME, the undersigned, a Notary Public in and for said County and State, on this day personally appeared W. H. Mannes known to me to be the person whose name is subscribed to the foregoing instrument, and acknowledged to me that he executed the same for the purposes and consideration therein expressed. "LS "Given under my hand and seal of office this the 4th day of January, A. D. 1940. H. G. Gwinnup Notary Public in and for Harris County, Texas. Filed for record at 4:15 o'clock P. M. February 5th, A. D. 1940. Recorded at 11:50 o'clock A. M. February 15th, A. D. 1940. D. B. Huton Clk.Co.Ct.M.Co.Tex. BJ John J. Frick Deputy #6874 Partial PARTIAL RELEASE OF OIL AND GAS LEASE Rel O & GL THE STATE OF TEXAS, County of Matagorda, KNOW ALL MEN BY THESE PRESENTS: That W. H. Mannes for W. H. Mannes and in consideration of One Dollar ($1.00) and other valuable considerations, the receipt of to Tom Petrucha et ux which is hereby acknowledged, does hereby release, relinquish and surrender to Tom Petrucha and Julia Petrucha, husband and wife, their heirs or assigns, all right, title and interest in and to a certain oil and gas mining lease made and entered into by and between Tom Petrucha and Julia Petrucha, husband and wife, of Matagorda County, Texas, as lessors, and W. H. Mannes, as lessee dated the 23rd day of June, 1939, insofar as it covers the following described land in the County of Matagorda and State of Texas, to-wit: The East 50 acres of the 165.3 acre tract in the D. McFarland League, and the East 766 acres of the 1316 acre tract in the F. W. Dempsey Survey, as described in said lease. Whereas, under the terms and provisions of said lease the undersigned Lessee did on December 23, 1939, make selection of the West 115.3 acres of the 165.3 acre tract and the West 550 acres of the 1316 acre tract described in said lease and paid the rental of $665.30 into the Bay City Bank & Trust Co. of Bay City, Texas, covering the acreage selected, and said lease as to the 665.3 acres upon which the rental was paid is hereby expressly retained, and as to which the lease shall be and remain in full force and effect. This is a partial release, only, it being the intention to release back to the lessors the balance of the land not retain- ed by rental payment as first above described. said lease being recorded in the office of the County Clerk in and for said County, in Book at page _. In Witness Whereof, the undersigned owner and releasor has signed this instrument this 23rd day of December 1939. W. H. Mannes THE STATE OF TEXAS, County of Harris, BEFORE ME, the undersigned, a Notary Public in and for said County and State, on this day personally appeared W. H. Mannes known to me to be the per- son whose name is subscribed to the foregoing instrument, and acknowledged to me that he executed the same for the purposes and consideration therein expressed. "LS 11 Given under my hand and seal of office this the 4th day of January, A.D. 1940. H. G. Gwinnup Notary Public in and for Harris County, Texas. Filed for record at 4:15 o'clock P. M. February 5th, A. D. 1940. Recorded at 1:30 o'clock P. M. February 15th, A. D. 1940. D. B. shtan Clk.Co.Ct.M.Co.Tex. By John J. Frick Deputy 
    """
    extract_and_process_document(ocr_text)
