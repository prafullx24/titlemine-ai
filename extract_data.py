"""
This file is expected to contain a Flask endpoint which takes ocr_data_id as input parameter to extract specific data from OCR data of a file.
We will use OpenAI LLM for this task.
We are expected to combine three OCR texts into one to create a "Consensus Document". This will be added after we add second OCR provider.

Architecture:

Our Data extraction process is divided in five layers:

"Runsheet -> extracted_data -> OCR_data -> Files -> Files in S3"

Here "Runsheets" and "Files in S3" are accesible to the user, everything else is abstracted away.

Files is the files table we use to maintain references to the original uploaded files
OCR_data is the raw text and confidence score of each OCR Block of a file. 
extracted_data is the LLM response for the runsheet prompts. This can contain the multiline response by the LLM.
Runsheet holds the specific information which will be useful for the Title Attorney


The code in this file is expected to provide following:
1. Flask Endpoint that is called from file_ocr.py file, which takes file_id and ocr_data_id as input.
2. Ignore Consensus Document as we only have one OCR in first milestone.
3. Use ChatGPT to prompt and extract data from files which have completed OCR
    - Use pyscopg2 to connect with postgresql database to query OCR data for a specific file 
    - Use OpenAI extract data from this OCR text
    - Use json files in ocr_data table to get confidence scores
4. Store the output from ChatGPT in json format in the extracted_data table.
        {
        "gpt_response": "<Multiline Response from ChatGPT>",
        "specific_data": "<Exact Value>",
        "confidence_score": "99.00"
        }
5. Get specific data from this table and store it in respective runsheet column.

Limitations:
1. Currently, this code is not expected to handle multiple OCR data. 


custom changes:
ALTER TABLE public.extracted_data
ADD CONSTRAINT unique_file_id UNIQUE (file_id);

had to set file_id unique
"""
import os
import json
import psycopg2
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from openai import OpenAI

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
                # print(response)

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

def insert_instrument_type(user_id, project_id, file_id, instrument_type):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = """
                INSERT INTO public.extracted_data (user_id, file_id, project_id, instrument_type)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (file_id) 
                DO UPDATE 
                SET instrument_type = EXCLUDED.instrument_type
                RETURNING id;
                """
                user_id = int(user_id)
                file_id = int(file_id)  # Convert to integer
                project_id = int(project_id)  # Convert to integer
                instrument_type_json = json.dumps(instrument_type)
                try:
                    cur.execute(query, (user_id, file_id, project_id, instrument_type_json))
                    extracted_data_id = cur.fetchone()[0]
                except Exception as e:
                    print("Error inserting into database:", e)
                    return None, "Database insert failed"

                conn.commit()
                return extracted_data_id, None

    except Exception as e:
        print(f"Error Inserting instrument_type: {e}")
        return None, str(e)


def insert_extracted_data(user_id, project_id, file_id, instrument_type):

    # To Do:
        """
        Add all values from Runsheet Propmt Columns and match them with extracted_data column. 
        """
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = """
                INSERT INTO public.extracted_data (user_id, file_id, project_id, instrument_type)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (file_id) 
                DO UPDATE 
                SET instrument_type = EXCLUDED.instrument_type
                RETURNING id;
                """
                user_id = int(user_id)
                file_id = int(file_id)  # Convert to integer
                project_id = int(project_id)  # Convert to integer
                instrument_type_json = json.dumps(instrument_type)
                try:
                    cur.execute(query, (user_id, file_id, project_id, instrument_type_json))
                    extracted_data_id = cur.fetchone()[0]
                except Exception as e:
                    print("Error inserting into database:", e)
                    return None, "Database insert failed"

                conn.commit()
                return extracted_data_id, None

    except Exception as e:
        print(f"Error Inserting instrument_type: {e}")
        return None, str(e)



# API Endpoint to fetch OCR text (file_id passed in URL as part of the route)

def extract_instrument_type(ocr_text):
    client = OpenAI()
    # Define system and user prompts
    system_prompt = f"""
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """
    user_prompt_doc_type = f"""Extract instrument type from the following document:\n\n{ocr_text}. 
    Instrument Type can be one of following: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. If the type is an amendment, return what kind of instrument it is amending. 
    Supporting evidence should be from the text provided as document in this prompt. 
    Expected output json should contain following fields: instrument type and supporting evidence from the input text data. In output json, keep two fields, one string "instrument_type" and second array of strings "supportingEvidence"."""

    # Send request to OpenAI
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
    )
    # Print extracted data
    return completion.choices[0].message.content

def extract_volume_page_document_case_number(ocr_text):
    client = OpenAI()
    # Define system and user prompts
    system_prompt = f"""
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """

    user_prompt_doc_type = f"""Extract Volume number or Page Number and Document or Case Number from the following document:\n\n{ocr_text}. 
    Volume number or Page Number: Reference the recording information found in the document and/or.the name of the file to determine the volume number and page number of where the document was filed with the county. Often the name of the file includes the recording information written simply as a 1 to 4 digit numbers with a dash or space between them. Return as "Volume number/Page Number".
    Document or Case Number: Reference information found in the margins, top of the document, or in the file name, to determine the case, file, or recording number. Return as "#" followed by the number. Not all documents have a number like this, so if none is found, return "none found".
    """

    # Send request to OpenAI
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
    )
    print(user_prompt_doc_type)
    print(completion.choices[0].message)
    # Print extracted data
    return completion.choices[0].message.content


"""
Execution Date
Effective Date
Recording Date
Grantor(s) - ***Note that Grator(s) and Grantee(s) need to indicate when things are owned jointly. Use {name1, name2, name3} for things owned jointly, and the list of names without brackets when owned individually.
Grantee(s) - ***Same note as Grantor(s)
Property Description
"""
def extract_volume_page_document_case_number(ocr_text):
    client = OpenAI()
    # Define system and user prompts
    system_prompt = f"""
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """

    user_prompt_doc_type = f"""Extract Volume number or Page Number and Document or Case Number from the following document:\n\n{ocr_text}. 
    Volume number or Page Number: Reference the recording information found in the document and/or.the name of the file to determine the volume number and page number of where the document was filed with the county. Often the name of the file includes the recording information written simply as a 1 to 4 digit numbers with a dash or space between them. Return as "Volume number/Page Number".
    Document or Case Number: Reference information found in the margins, top of the document, or in the file name, to determine the case, file, or recording number. Return as "#" followed by the number. Not all documents have a number like this, so if none is found, return "none found".
    """

    # Send request to OpenAI
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
    )
    print(user_prompt_doc_type)
    print(completion.choices[0].message)
    # Print extracted data
    return completion.choices[0].message.content


def find_confidence(ocr_data, instrument_data):

    # Incomplete Function DO NOT CALL
    best_match = None
    highest_confidence = 0

    for evidence in instrument_data["SupportingEvidence"]:
        for block in ocr_data["confidence_scores"]:
            similarity = SequenceMatcher(None, evidence, block["text"]).ratio()
            
            if similarity > 0.8:  # Threshold for similarity match
                if block["confidence"] > highest_confidence:
                    highest_confidence = block["confidence"]
                    best_match = block["text"]

    return {"BestMatch": best_match, "Confidence": highest_confidence}





# Flask App Factory
def create_app():
    app = Flask(__name__)
    @app.route("/api/v1/extract_data/<user_id>/<file_id>", methods=["GET"])
    def extract_data(user_id, file_id):
        try:
            file_id, project_id, ocr_data, error = fetch_ocr_text(file_id)

            if error:
                return jsonify({"error": error}), 404

            else:
                ocr_text = ocr_data.get("text", "")
                # print(ocr_text)

                extracted_instrument_type = extract_instrument_type(ocr_text)
                cleaned_extracted_instrument_type = extracted_instrument_type.strip("```").lstrip("json\n").strip()
                
                
                extracted_data_id = insert_instrument_type(user_id, project_id, file_id, cleaned_response)




            return jsonify({"message": "Data extraction successful", "extracted_data_id" : extracted_data_id})

        except Exception as e:
            print(f"Error retrieving OCR text: {e}")
            return jsonify({"error": str(e)}), 500
    return app

# Run the Flask application
if __name__ == "__main__":
    # app = create_app()
    # app.run(debug=True)
    file_id, project_id, ocr_data, error = fetch_ocr_text(63)
    # print(ocr_data)
    ocr_text = ocr_data.get("text", "")
    print(ocr_text)
    print(extract_volume_page_document_case_number(ocr_text))

