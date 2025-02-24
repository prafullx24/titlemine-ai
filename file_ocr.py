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



The .env file is expected to contain the following environment variables:

GOOGLE_APPLICATION_CREDENTIALS=
CREDENTIALS_PATH=

DB_NAME=
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=

PROJECT_ID = 
LOCATION = 
PROCESSOR_ID = 

"""



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


# The get_file_info function fetches file details (user_id, project_id, file_name, s3_url) from the database based on the provided file_id.
def get_file_info(file_id):
    """Fetch file details from the database based on file_id"""
    conn = psycopg2.connect(**DB_CONFIG) 
    cur = conn.cursor() # cursor object to execute queries
    query = "SELECT user_id, project_id, file_name, s3_url, ocr_status FROM public.files WHERE id = %s"
    cur.execute(query, (file_id,)) # Execute the query with the file_id parameter 
    file_data = cur.fetchone() # Fetch the first row from the result set 
    cur.close()
    conn.close()
    return file_data



# The download_file_from_s3 function downloads a file from an S3 URL and saves it locally.
def download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 URL and save it locally"""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name) # Path to save the downloaded file
    if os.path.exists(file_path):
        return file_path
    response = requests.get(s3_url, stream=True) # Send a GET request to the S3 URL to download the file 
    if response.status_code == 200:
        with open(file_path, "wb") as file: # Open the file in write binary mode 
            for chunk in response.iter_content(1024): 
                file.write(chunk) # Write the chunk to the file 
        return file_path 
    return None




# The extract_text_with_confidence function processes a document using Google Document AI to extract text and confidence scores for each text segment.
def extract_text_with_confidence(file_path):
    """Extracts text and confidence scores from a document using Google Document AI"""
    
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
    client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
    with open(file_path, "rb") as file:
        content = file.read() 
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




# The `save_and_update_ocr_data` function saves extracted OCR data to the database and updates `ocr_text_1` or inserts a new record if it doesn't exist.
def save_and_update_ocr_data(file_id, project_id, extracted_data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Check if file_id exists in the table
        cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
        result = cur.fetchone() # Fetch the first row from the result set 
        
        if result:
            # File entry exists in ocr_data, update the existing record
            try:
                extracted_text = extracted_data.get("text", "").replace("\n", " ")                
            except (json.JSONDecodeError, TypeError):
                query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s"
                cur.execute(query, (json.dumps(extracted_data), file_id))
                return jsonify({"error": "Invalid JSON format in OCR data"}), 500

            query = "UPDATE public.ocr_data SET ocr_text_1 = %s, ocr_json_1 = %s WHERE file_id = %s"
            cur.execute(query, (extracted_text, json.dumps(extracted_data), file_id)) 
        else:
            # Insert new record
            query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
            cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
        
        # Update ocr_status in public.files table
        update_status_query = "UPDATE public.files SET ocr_status = 'Extracting Data' WHERE id = %s"
        cur.execute(update_status_query, (file_id))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()




@app.route("/api/v1/ocr_file/<int:file_id>", methods=["GET"])
def process_ocr(file_id):
    """API Endpoint to download, perform OCR, save extracted text json file, upload that file in db and retrive plain text from that file and then upload that text in db  """

    # Step 1: Fetch File Info from DB
    file_data = get_file_info(file_id)
    if not file_data:
        return jsonify({"error": "File not found"}), 404
        
    user_id, project_id, file_name, s3_url, ocr_status = file_data
    if not s3_url:
        return jsonify({"error": "S3 URL not available"}), 404


    # Step 2: Check OCR status
    if ocr_status in ["processing", "complete"]:
        return jsonify({"message": f"OCR JSON already {ocr_status}."}), 200


    # Step 3: Extract File Extension
    file_extension = os.path.splitext(file_name)[1]
        
    # Define paths for downloaded and OCR files
    pdf_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}")
    ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_ocr_{user_id}_{project_id}_{file_id}.json")


    # Step 4: Check if OCR data already exists
    if os.path.exists(pdf_file_path) and os.path.exists(ocr_file_path):
        print(f"OCR JSON already exists: {ocr_file_path}, skipping OCR processing.")
        return jsonify({"message": "OCR JSON already exists."}), 200
    
        # Load existing OCR JSON
        # with open(ocr_file_path, "r", encoding="utf-8") as json_file:
        #     extracted_data = json.load(json_file)

    else:
        # Step 5: Download File (only if not already present)
        pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension)
        if not pdf_file_path:
            return jsonify({"error": "Failed to download file"}), 500
        else:
            print(f"File downloaded successfully: {pdf_file_path}")


        # Step 6: Perform OCR on File
        extracted_data = extract_text_with_confidence(pdf_file_path)
        print("OCR completed successfully.")


        # Step 7: Save OCR Output as JSON
        with open(ocr_file_path, "w", encoding="utf-8") as json_file:
            json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
            print(f"OCR JSON saved successfully: {ocr_file_path}")


        # Step 8: Save and Update OCR Data 
        save_and_update_ocr_data(file_id, project_id, extracted_data)
        # print("OCR data saved successfully in the database.")
        return jsonify({"message": "Inserted successfully in DB in text column "}), 200



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
