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
"""import os
import json
import psycopg2
from flask import Flask, jsonify
from dotenv import load_dotenv
import openai

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
    
def prompts_by_instrument_type(instrument_type):
    prompts = {
        "Deed": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "volume_page": "Reference the recording information found in the document and/or the file name to determine the volume number and page number of where the document was filed with the county. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Reference information found in the margins, top of the document, or in the file name to determine the case, file, or recording number. Return as '#Document Number'. If none is found, return 'none found'.",
            "execution_date": "What is the latest date on which a grantor or grantee signed the document?",
            "recording_date": "What is the date of the transfer of ownership?",
            "grantor": "Who is selling or transferring the property or rights? Provide full names.",
            "grantee": "Who is receiving the property or rights? Provide full names.",
            "property_description": "Provide a detailed description of the property being transferred, including any identifiers.",
            "reservations": "Are there any property rights explicitly excluded from the transaction by the Grantor(s)? If yes, list them.",
            "conditions": "Are there any conditions that must be met after the effective date to finalize the sale or prevent reversion? If yes, specify them."
        }
        },
        "Lease": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date the grantor or grantee signed the document?",
            "recording_date": "What is the official date the lease was recorded with the county?",
            "grantor": "Identify the landlord or entity leasing the property.",
            "grantee": "Identify the tenant or entity receiving the lease rights.",
            "property_description": "Provide a detailed description of the leased property.",
            "reservations": "Are there any rights or parts of the property explicitly excluded from the lease? If yes, specify.",
            "conditions": "Are there any conditions or obligations that must be met by the tenant or landlord? If yes, specify."
        }
        },
        "Release": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date the grantor or grantee signed the document?",
            "recording_date": "What is the official date the release document was recorded with the county?",
            "grantor": "Identify the entity or person releasing rights or claims.",
            "grantee": "Identify the entity or person receiving the release.",
            "property_description": "Provide a detailed description of the property or rights being released.",
            "reservations": "Are there any conditions under which the release does not apply? If yes, specify.",
            "conditions": "Are there any conditions attached to this release? If yes, specify."
        }
        }
        
























        }
    fields = prompts.get(instrument_type, {}).get("fields", {})
    return json.dumps(fields, indent=4)


def extract_instrument_type(ocr_text):
    client = openai.OpenAI()
    # Define system and user prompts
    system_prompt = """
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """
    user_prompt_doc_type =f"""Extract legal information from the following document:\n\n{ocr_text}. 
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
        print(json_resp)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
        return {"error": "Invalid JSON response from OpenAI", "raw_response": resp}
        


def extract_and_process_document(ocr_text):
    try:
        client = openai.OpenAI()  # New OpenAI client instance

        system_prompt = f"""
        You are a legal expert extraction algorithm specializing in property law and land transactions.
        Extract the following details from the provided legal land document and provide output in valid JSON format.
        Extract instrument type from the following document:\n\n{ocr_text}
        """
        
        instrument_type_data = extract_instrument_type(ocr_text)
        print(instrument_type_data)
      
        instrument_type = instrument_type_data.get("instrument_type", "")
        prompt_output = prompts_by_instrument_type(instrument_type)
        # print(prompt_output)
        user_prompt_doc_type = f"""{prompt_output} according to these parameters, find the corresponding information and return the values in similar json."""
        
        if not instrument_type:
            raise ValueError("Instrument type could not be extracted.")
    
        # Send request to OpenAI for document processing
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt_doc_type}]
        )

        result = response.choices[0].message.content.strip("```").lstrip("json\n").strip()
        output_file_path = os.path.join('output', f'{instrument_type}_extracted_data.json')
        with open(output_file_path, 'w') as output_file:
            json.dump(result, output_file, indent=4)

        return result

    except Exception as e:
        print(f"Error processing document: {e}")
        return str(e)

# Run the Flask application
if __name__ == "__main__":
    # app = create_app()
    # app.run(debug=True)
    file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(62)
    ocr_text = ocr_data.get("text", "")
    result = extract_and_process_document(ocr_text)

 
    # result=extract_instrument_type(ocr_text)
    print(result)

