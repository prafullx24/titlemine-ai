"""
This file is expected to contain all the code related to File OCR handling. 
As per development plan we are expecting to add three OCR Providers to increase our coverage of document.
During the first milestone phase we are specifically working with Google's Document AI.

The code in this file is expected to provide following:
1. A Flask endpoint that takes file_id as input.
2. This file is downloaded and split if page count is more than 15. 
3. These 15 page files are sent to OCR and OCR data is recieved from Document AI.
4. If a file is split the OCR data is merged and stored in the database table OCR_data. column ocr_text_1 and ocr_json_1
5. Along with the OCR Text data, we also extract the json with OCR confidence from Document AI. this is stored in ocr_json_1 column.
6. When OCR is complete, call internal endpoint "extract_data" to start data extraction process.

Limitations:
1. Currently working with only one OCR
2. Document AI has file size limit of 20 MB for single file processing.

To-Do:
1. Add Second OCR Provider: Amazon Textract or Anthropic
2. Switch to Batch Processing Mode on Document AI to disable the limits.

API Endpoint:
https://host:port/api/v1/ocr_file/:file_id

response:
Processing:
{
  "status": "processing",
  "file_id": "123456"
}

Completed:
{
  "status": "completed",
  "file_id": "123456",
  "ocr_data_id" : "123123"
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

# ----------------------------------------- *** ----------------------------------
# Code -
# 1) it takes id of file from the database and download the file locally from the s3 url 
# 2) it uses google document AI to extract the text from the downloaded file
# 3) it saves the extracted file as json locally 
# 4) it saves the extracted file in the database in ocr_json_1 column of ocr_data table
# 5) it updates the ocr_text_1 column of ocr_data table with the extracted text 
# ----------------------------------------- *** ----------------------------------



# ---------------------------------------- Actual Code ----------------------------------------
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



def get_file_info(file_id):
    """Fetch file details from the database based on file_id"""
    conn = psycopg2.connect(**DB_CONFIG) 
    cur = conn.cursor() # cursor object to execute queries
    query = "SELECT user_id, project_id, file_name, s3_url FROM public.files WHERE id = %s"
    cur.execute(query, (file_id,)) # Execute the query with the file_id parameter 
    file_data = cur.fetchone() # Fetch the first row from the result set 
    cur.close()
    conn.close()
    return file_data



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




def save_and_update_ocr_data(file_id, project_id, extracted_data):
    """Save extracted OCR data and update ocr_text_1 if empty"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Check if file_id exists in the table
        cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
        result = cur.fetchone() # Fetch the first row from the result set 
        
        if result:
            # File exists, update the existing record
            query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s" # Update the ocr_json_1 column for the file_id
            cur.execute(query, (json.dumps(extracted_data), file_id)) # Execute the query with the extracted data and file_id
            
            ocr_json_1, ocr_text_1 = result # Unpack the result into ocr_json_1 and ocr_text_1
            
            # Update ocr_text_1 only if it is empty
            if not ocr_text_1 or ocr_text_1.strip() == "":
                try:
                    extracted_text = extracted_data.get("text", "").replace("\n", " ")
                    update_query = "UPDATE public.ocr_data SET ocr_text_1 = %s WHERE file_id = %s"
                    cur.execute(update_query, (extracted_text, file_id))
                except (json.JSONDecodeError, TypeError):
                    return jsonify({"error": "Invalid JSON format in OCR data"}), 500
        else:
            # Insert new record
            query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
            cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
        
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
    
    user_id, project_id, file_name, s3_url = file_data
    if not s3_url:
        return jsonify({"error": "S3 URL not available"}), 404


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


    # step 7: 
    save_and_update_ocr_data(file_id, project_id, extracted_data)
    return jsonify({"message": "Inserted successfully in DB in text column "}), 200
    print("OCR data saved successfully in the database.")



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)


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
