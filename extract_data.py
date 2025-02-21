import os
import json
import psycopg2
from dotenv import load_dotenv
import openai
from datetime import datetime
import time
import concurrent.futures

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# Function to get a database connection
def get_db_connection():
    try:
        return psycopg2.connect(Config.DATABASE_URL)
    except Exception as e:
        print(f"Error getting DB connection: {e}")
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
        print(f"Error fetching OCR text: {e}")
        return None, str(e)

def extract_instrument_type(ocr_text):
    """
    Extracts the instrument type from the provided OCR text using OpenAI's GPT-3.5-turbo.

    Args:
        ocr_text (str): The OCR-extracted text from the document.

    Returns:
        dict: A dictionary containing the extracted instrument type, or an error dictionary.
              Example: {"instrument_type": "Deed"} or {"error": "Invalid JSON response...", "raw_response": "..."}
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
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt_doc_type}
            ]
        )

        resp = completion.choices[0].message.content.strip("```").lstrip("json\n").strip()

        try:
            json_resp = json.loads(resp)
            return json_resp  # Return JSON object (dict)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": resp}

    except Exception as e:
        print(f"Error communicating with OpenAI: {e}")
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
        user_prompt_doc_type = f"""{prompt_output} according to these parameters, find the corresponding information and return the values in similar json."""
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format."},
                      {"role": "user", "content": user_prompt_doc_type}]
        )
        result = response.choices[0].message.content.strip("```").lstrip("json\n").strip()
        try:
            result_json = json.loads(result)
            return result_json
        except json.JSONDecodeError as e:
            print(f"Error parsing json from LLM: {e}")
            return {"error": "Invalid JSON response from OpenAI", "raw_response": result}

    except Exception as e:
        print(f"Error processing document: {e}")
        return str(e)

def process_single_file(file_id):
    try:
        file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(file_id)
        if error:
            print(f"Error fetching file ID {file_id}: {error}")
            return
        ocr_text = ocr_data.get("text", "")
        extracted_data = extract_and_process_document(ocr_text)
        if extracted_data:
            print(f"File ID: {file_id}")
            print(json.dumps(extracted_data, indent=4))  # Print extracted data
        else:
            print(f"File ID {file_id} failed to extract data")
    except Exception as e:
        print(f"Error processing file ID {file_id}: {e}")

def process_batch(file_ids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # adjust max_workers
        futures = [executor.submit(process_single_file, file_id) for file_id in file_ids]
        concurrent.futures.wait(futures)

def fetch_file_ids_batch(batch_size, offset):
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM ocr_data LIMIT %s OFFSET %s", (batch_size, offset))
                file_ids = [row[0] for row in cur.fetchall()]
                return file_ids
    except Exception as e:
        print(f"Error fetching file IDs: {e}")
        return []

if __name__ == "__main__":
    file_ids = [62, 63, 77,72,67]  
    process_batch(file_ids)
    print("Manual batch processing complete.")