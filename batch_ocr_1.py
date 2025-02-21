import os
import psycopg2
import requests
import json
from flask import Flask, jsonify, request
from google.cloud import documentai_v1 as documentai
from dotenv import load_dotenv

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

def get_files_by_project(project_id):
    """Fetch all file IDs for a given project."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    query = "SELECT id, user_id, file_name, s3_url, ocr_status FROM public.files WHERE project_id = %s"
    cur.execute(query, (project_id,))
    files = cur.fetchall()
    cur.close()
    conn.close()
    return files

# def download_file(s3_url, user_id, project_id, file_id, file_extension):
#     """Download a file from S3 and save it locally."""
#     file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
#     file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
#     if os.path.exists(file_path):
#         return file_path
#     response = requests.get(s3_url, stream=True)
#     if response.status_code == 200:
#         with open(file_path, "wb") as file:
#             for chunk in response.iter_content(1024):
#                 file.write(chunk)
#         return file_path
#     return None



def download_file(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 and save it locally."""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
    if os.path.exists(file_path):
        return file_path
    try:
        response = requests.get(s3_url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        if response.status_code == 200:
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            return file_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {s3_url}: {e}")
    return None

def extract_text_with_confidence(file_path):
    """Extract text using Google Document AI."""
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
    client = documentai.DocumentProcessorServiceClient()
    with open(file_path, "rb") as file:
        content = file.read()
    raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
    name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    response = client.process_document(request=request)
    document_dict = documentai.Document.to_dict(response.document)
    extracted_text = document_dict.get("text", "")
    
    return {
        "text": extracted_text,
        "confidence_scores": [{
            "text": document_dict["text"][segment.start_index:segment.end_index],
            "confidence": block.layout.confidence
        } for page in response.document.pages for block in page.blocks for segment in block.layout.text_anchor.text_segments]
    }

# def save_ocr_data(file_id, extracted_data):
#     """Save extracted OCR data in the database and update status."""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
#     extracted_text = extracted_data.get("text", "").replace("\n", " ")
#     cur.execute("""
#         INSERT INTO public.ocr_data (file_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE 
#         SET ocr_json_1 = EXCLUDED.ocr_json_1, ocr_text_1 = EXCLUDED.ocr_text_1
#     """, (file_id, json.dumps(extracted_data), extracted_text))
#     cur.execute("UPDATE public.files SET ocr_status = 'completed' WHERE id = %s", (file_id,))
#     conn.commit()
#     cur.close()
#     conn.close()




# def save_ocr_data(file_id, extracted_data):
#     """Save extracted OCR data in the database and update status."""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
#     extracted_text = extracted_data.get("text", "").replace("\n", " ")
#     cur.execute("""
#     INSERT INTO public.ocr_data (file_id, ocr_json_1, ocr_text_1)
#     VALUES (%s, %s, %s)
#     ON CONFLICT ON CONSTRAINT unique_file_id 
#     DO UPDATE SET ocr_json_1 = EXCLUDED.ocr_json_1, ocr_text_1 = EXCLUDED.ocr_text_1
#     """, (file_id, json.dumps(extracted_data), extracted_text))
#     cur.execute("UPDATE public.files SET ocr_status = 'completed' WHERE id = %s", (file_id,))
#     conn.commit()
#     cur.close()
#     conn.close()







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












@app.route("/api/v1/batch_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    """Batch process OCR for all files under a project."""
    files = get_files_by_project(project_id)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404
    
    for file_id, user_id, file_name, s3_url, ocr_status in files:
        if ocr_status == "completed":
            continue
        
        file_extension = os.path.splitext(file_name)[1]
        file_path = download_file(s3_url, user_id, project_id, file_id, file_extension)
        if not file_path:
            continue
        
        extracted_data = extract_text_with_confidence(file_path)
        save_and_update_ocr_data(file_id,project_id, extracted_data)
    
    return jsonify({"message": "Batch OCR processing completed."}), 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
