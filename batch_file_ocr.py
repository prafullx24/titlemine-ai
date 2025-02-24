"""
This file is expected to contain all the code related to Batch File OCR handling.
As per the development plan, we are expecting to add multiple OCR Providers to increase our coverage of document processing.
During the first milestone phase, we are specifically working with Google's Document AI.

The code in this file is expected to provide the following:

A Flask endpoint that takes project_id as input.
Fetch all files under the given project_id where OCR processing is incomplete.
Download all these files from their respective S3 URLs concurrently.
Perform OCR processing on these files using Google’s Document AI.
Store the extracted OCR text in the database table OCR_data (columns: ocr_text_1 and ocr_json_1).
Along with OCR Text data, extract the JSON with OCR confidence from Document AI and store it in ocr_json_1.
When OCR processing is complete, update the ocr_status of all processed files to "Completed".
Limitations:

Currently working with only one OCR provider.
Document AI has a file size limit of 20 MB for single-file processing.
Processing is limited by API rate limits of Google Document AI.
To-Do:

Add a second OCR Provider: Amazon Textract or Anthropic.
Switch to Batch Processing Mode on Document AI to disable the file size limit.
API Endpoint:
https://host:port/api/v1/batch_ocr/:project_id

Response:

Processing:
{
  "status": "processing",
  "project_id": "123"
}

Completed:
{
  "status": "completed",
  "project_id": "123"
}


Failed:
{
  "status": "failed",
  "project_id": "123"
}


Libraries Used:

Flask
psycopg2
dotenv
requests
google.cloud
json
os
concurrent.futures (for parallel processing)


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
import psycopg2.extras
import requests
import json
from flask import Flask, jsonify, request
from google.cloud import documentai_v1 as documentai
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor


# Load environment variables
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



# This function saves the project ID and file IDs to a JSON file in the specified download folder.
def save_project_files_to_json(project_id, user_id, files):
    """Save project ID and file IDs to a JSON file."""
    data = {
        "project_id": project_id,
        "file_ids": [file[0] for file in files]
    }
    file_path = os.path.join(DOWNLOAD_FOLDER, f"project_{user_id}_{project_id}_files.json")
    with open(file_path, "w") as json_file:
        json.dump(data, json_file)



# Get files which not completed ocr by project ID from the database and save them to a JSON file
def get_files_by_project(project_id): 
    """Fetch all file IDs for a given project."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    #query = "SELECT id, user_id, %s as project_id, file_name, s3_url, ocr_status FROM public.files WHERE project_id = %s AND ocr_status != 'completed'"
    query = """
    SELECT id, user_id, %s as project_id, file_name, s3_url, ocr_status 
    FROM public.files 
    WHERE project_id = %s 
    AND (ocr_status IS NULL OR ocr_status = 'processing' OR ocr_status != 'completed')
    """

    cur.execute(query, (project_id,project_id))
    files = cur.fetchall()
    print("Get files which not completed ocr by project ID")
    cur.close()
    conn.close()

    # Save project ID and file IDs to a JSON file
    if files:
        user_id = files[0][1]
        save_project_files_to_json(project_id, user_id, files)
        print("Saved project ID and file IDs to a JSON file")

    return files




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
            for chunk in response.iter_content(1024): # Iterate over the response content in chunks of 1024 bytes
                file.write(chunk) # Write the chunk to the file 
        return file_path 
    # print(f"Failed to download file from S3: {s3_url}")
    return None




# The code defines a function to download files concurrently from S3 URLs using a thread pool, handling errors and printing the download status for each file.
def download_files_concurrently(files):
    downloaded_files = [] # List to store the downloaded file paths

    def download_file(file):
        try:
            id, user_id, project_id, file_name, s3_url, ocr_status = file
            file_extension = os.path.splitext(file_name)[1] # Get the file extension 
            pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, id, file_extension)
            if pdf_file_path:
                # print(f"File downloaded successfully: {pdf_file_path}")
                downloaded_files.append(pdf_file_path) # Append the downloaded file path to the list
            else:
                print(f"Failed to download file: {file_name}")
        except Exception as e:
            print(f"Error downloading file {file}: {e}")

    if not files:
        print("No files to download.")
        return []

    # with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent downloads
    with ThreadPoolExecutor() as executor:    # No limit on concurrent downloads
        executor.map(download_file, files)

    print("All files downloaded")
    # print(downloaded_files)
    return downloaded_files



# This function saves the OCR extracted data as a JSON file in a specified download folder, using the user ID, project ID, and file ID to name the file.
def save_ocr_output_as_json(user_id, project_id, file_id, extracted_data):
    """Save OCR output as JSON."""
    ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_json_{user_id}_{project_id}_{file_id}.json")
    with open(ocr_file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
        # print(f"OCR JSON saved successfully: {ocr_file_path}")




#  This function iterates through a list of extracted OCR data and saves each entry as a JSON file using the save_ocr_output_as_json function.
def save_ocr_outputs_as_json(extracted_data_list):
    """Save multiple OCR outputs as JSON."""
    for data in extracted_data_list:
        user_id = data['user_id']
        project_id = data['project_id']
        file_id = data['file_id']
        extracted_data = data['extracted_data']
        save_ocr_output_as_json(user_id, project_id, file_id, extracted_data)
    # print("All OCR JSON files saved successfully")




#This function processes a document using Google Document AI to extract text and confidence scores for each text segment, returning the extracted data in a structured format.
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

    #print(f"Text extracted with confidence scores: {extracted_data}")
    return extracted_data





# This function processes multiple documents using Google Document AI to extract text and confidence scores, saves the extracted data as JSON files, and returns the aggregated results.
def extract_text_with_confidence_batch(downloaded_files):
    """Extracts text and confidence scores from multiple documents using Google Document AI."""
    
    all_extracted_data = [] # List to store all extracted data from multiple documents

    def process_file(file_path):
        extracted_data = extract_text_with_confidence(file_path) # Extract text and confidence scores from the document

        # Extract user_id, project_id, and file_id from the file name
        file_name_parts = os.path.basename(file_path).split('_')
        user_id = file_name_parts[2]
        project_id = file_name_parts[3]
        file_id = file_name_parts[4].split('.')[0]
        
        return {
            'user_id': user_id,
            'project_id': project_id,
            'file_id': file_id,
            'extracted_data': extracted_data
        }

    # with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent threads
    with ThreadPoolExecutor() as executor:  # No limit on concurrent threads
        results = list(executor.map(process_file, downloaded_files))

    all_extracted_data.extend(results)

    # Save all OCR outputs as JSON
    save_ocr_outputs_as_json(all_extracted_data)
    print("Done with Extracts text and confidence scores ")
    return all_extracted_data





# This function inserts or updates OCR data for multiple files in the database and updates their OCR status to 'Completed'.
def save_and_update_ocr_data_batch(project_id, all_extracted_data, db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    try:
        new_records = [
            (data['file_id'], project_id, json.dumps(data['extracted_data']), data['extracted_data'].get('text', '').replace("\n", " ")) #Converts the extracted_data dictionary to a JSON-formatted string.
            for data in all_extracted_data
        ]
        
        insert_query = """
        INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
        VALUES %s
        ON CONFLICT (file_id, project_id) 
        DO UPDATE SET 
            ocr_json_1 = EXCLUDED.ocr_json_1,
            ocr_text_1 = EXCLUDED.ocr_text_1
        """
        
        psycopg2.extras.execute_values(cur, insert_query, new_records)
        
        file_ids = [data['file_id'] for data in all_extracted_data]
        update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = ANY(%s::int[])" # The %s::int[] placeholder is used to safely insert the list of file IDs into the query.
        cur.execute(update_status_query, (file_ids,))
        
        conn.commit()
        # print("Bulk insert and update executed successfully")
    except Exception as e:
        conn.rollback()
        print("Error in save_and_update_ocr_data_batch:", e)
    finally:
        cur.close()
        conn.close()




@app.route("/api/v1/batch_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    """Batch process OCR for all files under a project."""

    # step 1 - get files by project and store file ids in a json file which ocr_status is not completed
    files = get_files_by_project(project_id)
    # id, user_id, project_id, file_name, s3_url, ocr_status = files
    # print(files)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404


    # step 2 - download files from s3
    downloaded_files = download_files_concurrently(files)


    # # Step 3: Perform OCR on all downloaded File
    all_extracted_data = extract_text_with_confidence_batch(downloaded_files)
    # return jsonify({"message": "Batch file download processing completed."}), 200


    # Step 4: Save and Update OCR Data 
    save_and_update_ocr_data_batch(project_id, all_extracted_data, DB_CONFIG)
    print("OCR data saved successfully in the database.")
    return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)