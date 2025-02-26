import os
import json
import psycopg2
import logging
from dotenv import load_dotenv
import openai
from flask import Flask, request, jsonify

# Load environment variables and configure logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_legal_documents(project_id, batch_size=10):
    """
    Process legal documents for a given project_id in batches with minimal API requests.
    Returns a list of processed results.
    """
    # Database configuration
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

    # Load prompts from file
    with open("prompts.json", "r") as f:
        prompts = json.load(f)

    def get_db_connection():
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            logging.error(f"Error getting DB connection: {e}")
            return None

    def fetch_file_ids():
        try:
            with get_db_connection() as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT file_id FROM ocr_data WHERE project_id = %s", (project_id,))
                        return [row[0] for row in cur.fetchall()]
            return None
        except Exception as e:
            logging.error(f"Error fetching file_ids: {e}")
            return None

    def is_processed(file_id):
        try:
            with get_db_connection() as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM extracted_data WHERE file_id = %s", (file_id,))
                        return cur.fetchone() is not None
            return False
        except Exception as e:
            logging.error(f"Error checking processed file: {e}")
            return False

    def fetch_ocr(file_id):
        try:
            with get_db_connection() as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s", (file_id,))
                        response = cur.fetchone()
                        if response:
                            file_id_db, proj_id, ocr_json = response
                            ocr_data = json.loads(ocr_json) if isinstance(ocr_json, str) else ocr_json
                            return file_id_db, proj_id, ocr_data, None
                        return None, None, None, "file_id not found"
            return None, None, None, "Database connection error"
        except Exception as e:
            logging.error(f"Error fetching OCR text: {e}")
            return None, None, None, str(e)

    def process_document(ocr_text):
        client = openai.OpenAI()
        system_prompt = """
        You are a legal expert extraction algorithm specializing in property law and land transactions.
        Extract the following details from the provided legal land document and provide output in valid JSON format.
        """
        
        user_prompt = f"""
        Extract legal information from the following document:\n\n{ocr_text}.
        1. First, carefully analyze the first few lines to determine the instrument type.
           Instrument Type can be: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way,
           Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption,
           Court Case, Assignment, or Other. If it's an amendment, specify what it amends. If not explicit, use "Other".
        2. Then, extract these parameters based on the instrument type: 
           {json.dumps(prompts.get("default", {}).get("fields", {}), indent=4)}
           Search in the provided text data.
        Return the result as a JSON object with "instrument_type" and all required fields.
        """

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
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
                            "required": ["instrument_type", "volume_page", "document_case_number",
                                       "execution_date", "effective_date", "recording_date",
                                       "grantee", "grantor", "property_description"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
            result = json.loads(response.choices[0].message.content)
            return result
        except Exception as e:
            logging.error(f"Error processing document: {e}")
            return {"error": str(e)}

    # Main processing logic
    file_ids = fetch_file_ids()
    if not file_ids:
        logging.error(f"No files found for project_id {project_id}")
        return "No files to process."

    results = []
    for i in range(0, len(file_ids), batch_size):
        batch = [fid for fid in file_ids[i:i + batch_size] if not is_processed(fid)]
        if not batch:
            logging.info("Skipping batch, all files processed.")
            continue

        for file_id in batch:
            file_id_db, proj_id, ocr_data, error = fetch_ocr(file_id)
            if error:
                logging.error(f"Error fetching OCR for {file_id}: {error}")
                continue

            ocr_text = ocr_data.get("text", "") if isinstance(ocr_data, dict) else ocr_data[0].get("text", "") if isinstance(ocr_data, list) else ""
            if not ocr_text.strip():
                logging.info(f"Skipping {file_id} due to empty OCR text.")
                continue

            extracted_data = process_document(ocr_text)
            if "error" not in extracted_data:
                results.append(extracted_data)
            else:
                logging.error(f"Error processing {file_id}: {extracted_data['error']}")

        logging.info(f"Processed batch: {batch}")

    return results if results else "Processing completed with no new results."

	
app = Flask(__name__)

@app.route('/process_documents/<project_id>', methods=['GET'])
def process_documents(project_id):
    """
    Endpoint to process legal documents. The project_id is passed as part of the URL.
    Batch size is optional and defaults to 10 if not provided.
    """
    # Check if batch_size is provided as a query parameter, default to 10 if not.
    batch_size = request.args.get('batch_size', 10, type=int)
    
    try:
        result = process_legal_documents(project_id, batch_size)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)