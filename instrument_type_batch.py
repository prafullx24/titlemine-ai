import os
import json
import psycopg2
from flask import Flask, jsonify
from dotenv import load_dotenv
import openai
from datetime import datetime

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
    client = openai.OpenAI()
    # Define system and user prompts
    system_prompt = """
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """
    user_prompt_doc_type ="""Extract legal information from the following document:\n\n{ocr_text}. 
    Instrument Type can be one of following: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. If the type is an amendment, return what kind of instrument it is amending."""

    # Send request to OpenAI
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
    )
    # Print extracted data
    resp = completion.choices[0].message.content
    resp = resp.strip("```").lstrip("json\n").strip()

    try:
        json_resp = json.loads(resp)
        return json_resp  # Return JSON object (dict)
        # print(json_resp)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
        return {"error": "Invalid JSON response from OpenAI", "raw_response": resp}


def get_files_by_project(project_id): 
    """Fetch all file IDs for a given project."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = """
    SELECT id
    FROM public.files 
    WHERE project_id = %s 
    """
    cur.execute(query, (project_id))
    files = cur.fetchall()
    cur.close()
    conn.close()

    return files


        
@app.route("/api/v1/batch_instrument_type/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):

    files = get_files_by_project(project_id)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404
    


    return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)