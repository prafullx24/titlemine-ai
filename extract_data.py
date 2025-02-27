import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import concurrent.futures
import logging
from flask import Flask, jsonify

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

def fetch_ocr_text(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, None, None, None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT id, file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"
                cur.execute(query, (file_id,))
                response = cur.fetchone()

                if not response:
                    return None, None, None, None, "file_id not found in ocr_data table"

                id_db, file_id_from_db, project_id, ocr_json_1 = response

                try:
                    ocr_data = json.loads(ocr_json_1) if isinstance(ocr_json_1, str) else ocr_json_1
                    return id_db, file_id_from_db, project_id, ocr_data, None
                except json.JSONDecodeError:
                    return id_db, file_id_from_db, project_id, None, "Invalid JSON format"

    except Exception as e:
        logging.error(f"Error fetching OCR text: {e}")
        return None, None, None, None, f"Error: {e}"

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

def store_extracted_data(user_id, id_db, file_id, project_id, extracted_data):
    try:
        conn = get_db_connection()
        if conn is None:
            return "Database connection error"

        with conn:
            with conn.cursor() as cur:
                try:
                    conn.autocommit = False  # Disable autocommit for transactions

                    # Convert date strings to proper formats
                    def convert_date(date_str):
                        if date_str and date_str.lower() not in ["none found", "n/a"]:
                            try:
                                return datetime.strptime(date_str, "%B %d, %Y").date()
                            except ValueError:
                                return None
                        return None

                    # Ensure extracted_data is in dict format
                    if isinstance(extracted_data, str):
                        try:
                            extracted_data = json.loads(extracted_data)
                        except json.JSONDecodeError:
                            return "Invalid JSON data"

                    # Extract necessary fields
                    execution_date = convert_date(extracted_data.get("execution_date"))
                    effective_date = convert_date(extracted_data.get("effective_date"))
                    recording_date = convert_date(extracted_data.get("recording_date"))

                    instrument_type = extracted_data.get("instrument_type", "N/A")
                    volume_page = extracted_data.get("volume_page", "N/A")
                    document_case = extracted_data.get("document_case_number", "N/A")
                    grantor = extracted_data.get("grantor", "N/A")
                    grantee = json.dumps(extracted_data.get("grantee", []))
                    property_description = json.dumps(extracted_data.get("property_description", []))
                    remarks = "N/A"
                    file_date = recording_date

                    # Check if the entry exists
                    check_query = "SELECT id FROM public.runsheets WHERE file_id = %s AND project_id = %s"
                    cur.execute(check_query, (file_id, project_id))
                    existing_entry = cur.fetchone()

                    if existing_entry:
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
                                user_id = COALESCE(%s, user_id),
                                id = COALESCE(%s, id)
                            WHERE file_id = %s AND project_id = %s
                        """
                        cur.execute(update_query, (
                            instrument_type, document_case, volume_page, effective_date,
                            execution_date, file_date, grantor, grantee, property_description, remarks,
                            user_id, id_db, file_id, project_id
                        ))

                        if cur.rowcount == 0:
                            conn.rollback()
                            return "Update failed. No rows affected."

                    else:
                        insert_query = """
                            INSERT INTO public.runsheets (
                                id, file_id, project_id, instrument_type, document_case, volume_page, 
                                effective_date, execution_date, file_date, grantor, grantee, property_description, 
                                remarks, user_id
                            ) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(insert_query, (
                            id_db, file_id, project_id, instrument_type, document_case, volume_page,
                            effective_date, execution_date, file_date, grantor, grantee, property_description,
                            remarks, user_id
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

def fetch_user_id(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT user_id FROM files WHERE files.id = %s"
                try:
                    cur.execute(query, (int(file_id),))
                except ValueError:
                    cur.execute(query, (file_id,))

                response = cur.fetchone()

                if not response:
                    return None, "user_id not found in files table"
                else:
                    return response[0], None
    except Exception as e:
        logging.error(f"Error fetching user_id: {e}")
        return None, str(e)

def process_single_document(file_id):
    id_db, file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(file_id)
    if error:
        logging.error(f"Error fetching OCR text for file {file_id}: {error}")
        return error

    if not ocr_data:
        return f"No OCR data found for file_id {file_id}"

    # Log the type and content of ocr_data for debugging
    # logging.info(f"ocr_data type: {type(ocr_data)}, content: {ocr_data}")

    # Handle both list and dict cases for ocr_data
    if isinstance(ocr_data, list) and ocr_data:
        ocr_text = ocr_data[0].get("text", "")  # Take "text" from first item if list
    elif isinstance(ocr_data, dict):
        ocr_text = ocr_data.get("text", "")  # Original behavior for dict
    else:
        ocr_text = ""  # Default to empty string if unexpected format
        logging.warning(f"Unexpected ocr_data format for file {file_id}")

    extracted_data = extract_and_process_document(ocr_text)
    if "error" in extracted_data:
        logging.error(f"Error processing document {file_id}: {extracted_data}")
        return f"Error processing document {file_id}: {extracted_data.get('error')}"

    user_id, error = fetch_user_id(file_id)
    if error:
        logging.error(f"Error fetching user_id for file {file_id}: {error}")
        return error

    result = store_extracted_data(user_id, id_db, file_id_from_db, project_id, extracted_data)
    return result

def fetch_file_ids_by_project(project_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT file_id FROM ocr_data WHERE project_id = %s"
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



# Flask app initialization
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/api/project/<int:project_id>', methods=['GET'])
def process_project(project_id):
    try:
        # Fetch the file IDs associated with the project_id
        file_ids, error = fetch_file_ids_by_project(project_id)
        if error:
            logging.error(f"Error fetching file IDs for project {project_id}: {error}")
            return jsonify({"error": f"Error fetching file IDs: {error}"}), 500

        if not file_ids:
            logging.info(f"No files to process for project {project_id}")
            return jsonify({"message": f"No files to process for project ID {project_id}"}), 404

        # Process each file_id
        results = []
        for file_id in file_ids:
            logging.info(f"Processing file ID: {file_id}")
            result = process_single_document(file_id)
            results.append({
                "file_id": file_id,
                "result": result
            })
            logging.info(f"Completed processing file ID {file_id}: {result}")
        
        # Return the results in JSON format
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


if __name__ == '__main__':  
    app.run(debug=True, host='0.0.0.0', port=5000)