"""
This file is expected to contain all the code related to File OCR handling.
As per the development plan, we are expecting to add multiple OCR Providers to increase our coverage of document processing.
During the first milestone phase, we are specifically working with Google's Document AI.

The code in this file is expected to provide the following:
1. A Flask endpoint that takes file_id as input.
2. This file is downloaded from an S3 URL and split if the page count is more than 15.
3. These 15-page files are sent to OCR, and OCR data is received from Document AI.
4. If a file is split, the OCR data is merged and stored in the database table OCR_data (columns: ocr_text_1 and ocr_json_1).
5. Along with the OCR Text data, we also extract the JSON with OCR confidence from Document AI. This is stored in the ocr_json_1 column.
6. When OCR is complete, call the internal endpoint "extract_data" to start the data extraction process.

Limitations:
1. Currently working with only one OCR provider.
2. Document AI has a file size limit of 20 MB for single-file processing.

To-Do:
1. Add a second OCR Provider: Amazon Textract or Anthropic.
2. Switch to Batch Processing Mode on Document AI to disable the limits.

API Endpoint:
https://host:port/api/v1/ocr_file/:file_id

Response:
Processing:
{
  "status": "processing",
  "file_id": "123456"
}

Completed:
{
  "status": "completed",
  "file_id": "123456",
  "ocr_data_id": "123123"
}

Failed:
{
  "status": "failed",
  "file_id": "123456"
}

Libraries:
Flask
psycopg2
dotenv
requests
google.cloud
json
os
"""

"""
This file is expected to contain a Flask endpoint that takes ocr_data_id as an input parameter to extract specific data from the OCR data of a file.
We will use OpenAI LLM for this task.
We are expected to combine multiple OCR texts into one to create a "Consensus Document" once additional OCR providers are integrated.

Architecture:

Our Data extraction process is divided into five layers:

"Runsheet -> extracted_data -> OCR_data -> Files -> Files in S3"

Here, "Runsheets" and "Files in S3" are accessible to the user, while everything else is abstracted away.

Files refer to the files table used to maintain references to the original uploaded files.
OCR_data contains the raw text and confidence score of each OCR Block of a file.
extracted_data holds the LLM response for the runsheet prompts, containing a multiline response by the LLM.
Runsheet stores specific information useful for the Title Attorney.

The code in this file is expected to provide the following:
1. A Flask Endpoint called from the OCR processing file, which takes file_id and ocr_data_id as input.
2. Ignore the Consensus Document as we only have one OCR in the first milestone.
3. Use ChatGPT to prompt and extract data from files that have completed OCR processing.
    - Use psycopg2 to connect with a PostgreSQL database to query OCR data for a specific file.
    - Use OpenAI API to extract data from this OCR text.
    - Use JSON files in the OCR_data table to get confidence scores.
4. Store the output from ChatGPT in JSON format in the extracted_data table.
    {
        "gpt_response": "<Multiline Response from ChatGPT>",
        "specific_data": "<Exact Value>",
        "confidence_score": "99.00"
    }
5. Retrieve specific data from this table and store it in the respective runsheet column.

Limitations:
1. Currently, this code is not expected to handle multiple OCR providers.

"""




# ----------------------------------------- *** Execution *** ----------------------------------
# 1) api 
# 2) Fetch file details from the database based on file_id --> fetch url from db 
# 3) split file and Define paths for downloaded pdf files and OCR files
# 4) Check if OCR data already exists --> if yes then load existing OCR JSON
# 5) Download File (only if not already present) --> download file from s3 url
# 6) Perform OCR on File --> extract text from the downloaded files
# 7) Save OCR Output as JSON --> save the extracted text in json format
# 8) Save extracted OCR data and update ocr_text_1 if empty --> save the extracted text in the database
# ----------------------------------------- *** Execution *** ----------------------------------



import os
import psycopg2
import requests
import json
from flask import Flask, jsonify
from google.cloud import documentai_v1 as documentai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set Google Application Credentials
credentials_path = os.getenv("CREDENTIALS_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize Flask App
app = Flask(__name__)


# Database Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# Google Document AI Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")


# Folder to store downloaded and OCR files
DOWNLOAD_FOLDER = "download_file"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


# The fetch_and_process_ocr_data function fetches file details and OCR data from the database, 
# then updates or inserts the OCR data in a single database call.
def fetch_and_process_ocr_data(file_id, extracted_data=None):
    """Fetch file details, check OCR data, update or insert in a single database call"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Fetch file details and check if OCR data exists in a single query using LEFT JOIN
        query = """
            SELECT f.user_id, f.project_id, f.file_name, f.s3_url, o.ocr_json_1, o.ocr_text_1
            FROM public.files f
            LEFT JOIN public.ocr_data o ON f.id = o.file_id
            WHERE f.id = %s
        """
        cur.execute(query, (file_id,))
        result = cur.fetchone()
        
        if not result:
            return None, jsonify({"error": "File not found"}), 404
        
        user_id, project_id, file_name, s3_url, ocr_json_1, ocr_text_1 = result
        
        if not s3_url:
            return None, jsonify({"error": "S3 URL not found"}), 404
        
        if extracted_data is not None:
            extracted_text = extracted_data.get("text", "").replace("\n", " ")
            # Use INSERT ... ON CONFLICT to handle both insert and update
            upsert_query = """
                INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (file_id) DO UPDATE SET
                    ocr_json_1 = EXCLUDED.ocr_json_1,
                    ocr_text_1 = CASE
                        WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
                        ELSE public.ocr_data.ocr_text_1
                    END
            """
            cur.execute(upsert_query, (file_id, project_id, json.dumps(extracted_data), extracted_text))
        
        conn.commit()
        return {
            "file_id": file_id,
            "user_id": user_id,
            "project_id": project_id,
            "file_name": file_name,
            "s3_url": s3_url,
            "message": "OCR data processed successfully"
        }, None, 200
        
    except Exception as e:
        conn.rollback()
        return None, jsonify({"error": str(e)}), 500
    
    finally:
        cur.close()
        conn.close()




def download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 URL and save it locally"""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name) # Path to save the downloaded file
    if os.path.exists(file_path):
        return file_path
    response = requests.get(s3_url, stream=True) # Send a GET request to the S3 URL to download the file 
    if response.status_code == 200:
        with open(file_path, "wb") as file: # Open the file in write binary mode 
            for chunk in response.iter_content(1024): # Write the file in chunks of 1024 bytes 
                file.write(chunk) # Write the chunk to the file 
        return file_path # Return the file path if download is successful
    return None



def extract_text_with_confidence(file_path):
    """Extracts text and confidence scores from a document using Google Document AI"""
    
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
    client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
    with open(file_path, "rb") as file:
        content = file.read() # Read the file content in binary mode
    raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
    name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
    request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
    response = client.process_document(request=request) # Process the document using the client and request object
    document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
    extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
    extracted_data = {
        "text": extracted_text,
        "confidence_scores": []
    }
    # Extract confidence scores for each text segment
    for page in response.document.pages:
        for block in page.blocks:
            for segment in block.layout.text_anchor.text_segments:
                segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
                confidence = block.layout.confidence # Extract the confidence score from the block layout 
                extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
                    "text": segment_text,
                    "confidence": confidence
                })
    return extracted_data




@app.route("/api/v1/ocr_file/<int:file_id>", methods=["GET"])
def process_ocr(file_id):
    """API Endpoint to download, perform OCR, save extracted text json file, upload that file in db and retrive plain text from that file and then upload that text in db  """

    # Step 1: Fetch File Info and Save OCR Data
    file_data, error_response, status_code = fetch_and_process_ocr_data(file_id)
    if error_response:
        return error_response, status_code
        
    user_id = file_data["user_id"]
    project_id = file_data["project_id"]
    file_name = file_data["file_name"]
    s3_url = file_data["s3_url"]


    # Step 2: Extract File Extension
    file_extension = os.path.splitext(file_name)[1]
        
    # Define paths for downloaded and OCR files
    pdf_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}")
    ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_ocr_{user_id}_{project_id}_{file_id}.json")


    # Step 3: Check if OCR data already exists
    if os.path.exists(pdf_file_path) and os.path.exists(ocr_file_path):
        print(f"OCR JSON already exists: {ocr_file_path}, skipping OCR processing.")
            
        # Load existing OCR JSON
        with open(ocr_file_path, "r", encoding="utf-8") as json_file:
            extracted_data = json.load(json_file)
            
    else:
        # Step 4: Download File (only if not already present)
        pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension)
        if not pdf_file_path:
            return jsonify({"error": "Failed to download file"}), 500
        else:
            print(f"File downloaded successfully: {pdf_file_path}")


        # Step 5: Perform OCR on File
        extracted_data = extract_text_with_confidence(pdf_file_path)
        print("OCR completed successfully.")


        # Step 6: Save OCR Output as JSON
        with open(ocr_file_path, "w", encoding="utf-8") as json_file:
            json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
            print(f"OCR JSON saved successfully: {ocr_file_path}")


        # Step 7: Save and Update OCR Data
        _, error_response, status_code = fetch_and_process_ocr_data(file_id, extracted_data)
        if error_response:
            return error_response, status_code

    print("OCR data saved successfully in the database.")
    return jsonify({"message": "Inserted successfully in DB in text column "}), 200
    



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)