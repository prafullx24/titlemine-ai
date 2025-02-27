import os
import json
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv
import openai
from flask import Flask, request, jsonify

# Load environment variables and configure logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection function
def get_db_connection():
    try:
        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is missing.")
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logging.error(f"Error getting DB connection: {e}")
        return None

def process_legal_documents(project_id, batch_size=10):
    """
    Process legal documents for a given project_id in batches with one API request per batch.
    Returns a list of processed results.
    """
    # Load prompts from file
    with open("prompts.json", "r") as f:
        prompts = json.load(f)

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
                        cur.execute("SELECT 1 FROM Extracted_data WHERE file_id = %s", (file_id,))
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
                        cur.execute("SELECT id, file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s", (file_id,))
                        response = cur.fetchone()
                        if response:
                            record_id, file_id_db, proj_id, ocr_json = response
                            ocr_data = json.loads(ocr_json) if isinstance(ocr_json, str) else ocr_json
                            return record_id, file_id_db, proj_id, ocr_data, None
                        return None, None, None, None, "file_id not found"
            return None, None, None, None, "Database connection error"
        except Exception as e:
            logging.error(f"Error fetching OCR text: {e}")
            return None, None, None, None, str(e)

    def process_batch_documents(batch_documents):
        client = openai.OpenAI()
        system_prompt = """
        You are a legal expert extraction algorithm specializing in property law and land transactions.
        Extract details from the provided legal land documents and provide output in valid JSON format.
        """

        # Construct a single prompt for all documents in the batch
        user_prompt = """
        Extract legal information from the following documents. For each document, analyze the first few lines to determine the instrument type and extract the specified fields. Return a single JSON object with a "documents" key containing a list of results, one per document.

        Instrument Types: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment, or Other. If it's an amendment, specify what it amends. If not explicit, use "Other".

        Fields to extract based on instrument type:
        """ + json.dumps(prompts.get("default", {}).get("fields", {}), indent=4) + """

        Documents:
        """
        
        # Add each document's details to the prompt
        for doc in batch_documents:
            file_id, ocr_text, record_id, proj_id = doc
            user_prompt += f"\nDocument (file_id: {file_id}, record_id: {record_id}, project_id: {proj_id}):\n{ocr_text}\n---\n"

        user_prompt += """
        Return the result as a JSON object with a "documents" key containing a list of JSON objects, each with "file_id", "record_id", "project_id", "instrument_type", and the required fields.
        Example:
        {
            "documents": [
                {"file_id": "123", "record_id": "abc", "project_id": "proj1", "instrument_type": "Deed", ...},
                {"file_id": "456", "record_id": "def", "project_id": "proj1", "instrument_type": "Lease", ...}
            ]
        }
        """

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "batch_document_extraction",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "documents": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "file_id": {"type": "string"},
                                            "record_id": {"type": "string"},
                                            "project_id": {"type": "string"},
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
                                        "required": ["file_id", "record_id", "project_id", "instrument_type"],
                                        "additionalProperties": True
                                    }
                                }
                            },
                            "required": ["documents"],
                            "additionalProperties": False
                        },
                        "strict": False
                    }
                }
            )
            result = json.loads(response.choices[0].message.content)
            return result["documents"]
        except Exception as e:
            logging.error(f"Error processing batch documents: {e}")
            return [{"error": str(e)}]

    # Insert multiple documents into runsheets table in a single batch
    def insert_runsheets_batch(data_list):
        if not data_list:
            return True
        try:
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur:
                    sql = """
                        INSERT INTO public.runsheets (
                            id, file_id, project_id, document_case, instrument_type, 
                            volume_page, effective_date, execution_date, file_date, 
                            grantor, grantee, property_description, remarks, 
                            created_at, updated_at, sort_sequence
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """
                    batch_values = [
                        (
                            data.get('record_id'),
                            data.get('file_id'),
                            data.get('project_id'),
                            data.get('document_case_number', ''),
                            data.get('instrument_type', ''),
                            data.get('volume_page', ''),
                            data.get('effective_date', ''),
                            data.get('execution_date', ''),
                            data.get('recording_date', ''),
                            data.get('grantor', ''),
                            data.get('grantee', ''),
                            data.get('property_description', ''),
                            data.get('remarks', ''),
                            datetime.now(),
                            datetime.now(),
                            data.get('sort_sequence', 1)
                        )
                        for data in data_list if "error" not in data
                    ]
                    if batch_values:
                        cur.executemany(sql, batch_values)
                        conn.commit()
                        logging.info(f"Inserted {len(batch_values)} runsheets in a batch")
                conn.close()
                return True
        except Exception as e:
            logging.error(f"Error inserting runsheets batch: {e}")
            return False

    # Main processing logic
    file_ids = fetch_file_ids()
    if not file_ids:
        logging.error(f"No files found for project_id {project_id}")
        return "No files to process."

    all_results = {}
    for i in range(0, len(file_ids), batch_size):
        batch = [fid for fid in file_ids[i:i + batch_size] if not is_processed(fid)]
        if not batch:
            logging.info("Skipping batch, all files processed.")
            continue

        # Collect documents for batch processing
        batch_documents = []
        for file_id in batch:
            record_id, file_id_db, proj_id, ocr_data, error = fetch_ocr(file_id)
            if error:
                logging.error(f"Error fetching OCR for {file_id}: {error}")
                continue

            ocr_text = ocr_data.get("text", "") if isinstance(ocr_data, dict) else ocr_data[0].get("text", "") if isinstance(ocr_data, list) else ""
            if not ocr_text.strip():
                logging.info(f"Skipping {file_id} due to empty OCR text.")
                continue

            batch_documents.append((file_id_db, ocr_text, record_id, proj_id))

        # Process all documents in the batch with a single API call
        if batch_documents:
            extracted_data_list = process_batch_documents(batch_documents)
            for data in extracted_data_list:
                if "error" not in data:
                    all_results[data["file_id"]] = data
                else:
                    logging.error(f"Error processing batch: {data['error']}")

            # Insert all processed data in batch
            insert_runsheets_batch(extracted_data_list)
            logging.info(f"Processed batch: {batch}")

    return all_results if all_results else "Processing completed with no new results."

# Flask API setup
app = Flask(__name__)

@app.route('/process_documents/<project_id>', methods=['GET'])
def process_documents(project_id):
    batch_size = request.args.get('batch_size', 10, type=int)
    try:
        result = process_legal_documents(project_id, batch_size)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)