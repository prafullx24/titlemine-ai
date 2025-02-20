


# mi 
# mi 
# mi 
# mi 
# mi 
# mi 
# mi 
# mi 
# mi 





#### combine function - 
# save_and_update_ocr_data(file_id, project_id, extracted_data):  and  get_file_info(file_id):
### into a single function - 
# def fetch_and_save_ocr_data(file_id, extracted_data=None):

# means - 
# The get_file_info function fetches file details (user_id, project_id, file_name, s3_url) from the database based on the provided file_id.
# The `save_and_update_ocr_data` function saves extracted OCR data to the database and updates `ocr_text_1` if it is empty, or inserts a new record if it doesn't exist.

# The fetch_and_save_ocr_data function fetches file details from the database and saves extracted OCR data, updating ocr_text_1 if it exists or inserting a new record if it doesn't.


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


# The extract_text_with_confidence function processes a document using Google Document AI to extract text and confidence scores for each text segment.
# This function does not generate a JSON file; it returns a dictionary containing the extracted text and confidence scores.
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




# The `save_and_update_ocr_data` function saves extracted OCR data to the database and updates `ocr_text_1` if it is empty, or inserts a new record if it doesn't exist.

# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # Check if file_id exists in the table
#         cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
#         result = cur.fetchone() # Fetch the first row from the result set 
        
#         if result:
#             # File exists, update the existing record
#             query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s" # Update the ocr_json_1 column for the file_id
#             cur.execute(query, (json.dumps(extracted_data), file_id)) # Execute the query with the extracted data and file_id
            
#             ocr_json_1, ocr_text_1 = result # Unpack the result into ocr_json_1 and ocr_text_1
            
#             # Update ocr_text_1 only if it is empty
#             if not ocr_text_1 or ocr_text_1.strip() == "":
#                 try:
#                     extracted_text = extracted_data.get("text", "").replace("\n", " ")
#                     update_query = "UPDATE public.ocr_data SET ocr_text_1 = %s WHERE file_id = %s"
#                     cur.execute(update_query, (extracted_text, file_id))
#                 except (json.JSONDecodeError, TypeError):
#                     return jsonify({"error": "Invalid JSON format in OCR data"}), 500
#         else:
#             # Insert new record
#             query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
#             cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
        
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()






# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # UPSERT query to insert or update the record
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         ocr_json_1 = EXCLUDED.ocr_json_1,
#         ocr_text_1 = CASE
#             WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
#             ELSE public.ocr_data.ocr_text_1
#         END;
#         """

#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         print("extracted_text completed successfully.")
#         err = cur.execute(query, (file_id, project_id, json.dumps(extracted_data), extracted_text))
#         print(f"cur.execute completed successfully.")
#         print(f"err: {err}")
#         # Update ocrstatus in public.files table
#         # update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         # cur.execute(update_status_query, (file_id))
                
#         # # Check current ocr_status
#         # check_status_query = "SELECT ocr_status FROM public.files WHERE id = %s"
#         # cur.execute(check_status_query, (file_id,))
#         # current_status = cur.fetchone()[0]

#         # if current_status is None:
#         #     print(f"ocr_status is NULL for file_id {file_id}")
#         # # else:
#         # #     # Update ocrstatus in public.files table
#         # #     update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         # #     cur.execute(update_status_query, (file_id))
#         # #     print(f"Updated ocr_status for file_id {file_id}")


#         conn.commit()
#         #return print({"message": "OCR data saved successfully"}), 200
#         return jsonify({"message": "OCR data saved successfully"}), 200

#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()




# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # UPSERT query to insert or update the record
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         ocr_json_1 = EXCLUDED.ocr_json_1,
#         ocr_text_1 = CASE
#             WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
#             ELSE public.ocr_data.ocr_text_1
#         END;
#         """

#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         cur.execute(query, (file_id, project_id, json.dumps(extracted_data), extracted_text))
        
#         # Update ocr_status in public.files table
#         update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         cur.execute(update_status_query, (file_id,))
        
#         conn.commit()
#         print({"message": "OCR data saved successfully through save_and_update_ocr_data"}), 200
#         return None

#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()








# mi
# mi 
# mi 
# mi 
# mi 

# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update if necessary"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         # print("extracted_text completed successfully.")
#         # print(extracted_text)
#         extracted_json = json.dumps(extracted_data)
#         # print("extracted_json completed successfully.")
#         # print(extracted_json)
        
#         # UPSERT logic to check if update is needed
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#             project_id = EXCLUDED.project_id,
#             ocr_json_1 = CASE
#                 WHEN public.ocr_data.ocr_json_1 IS NULL OR public.ocr_data.ocr_json_1 = '' THEN EXCLUDED.ocr_json_1
#                 ELSE public.ocr_data.ocr_json_1
#             END,
#             ocr_text_1 = CASE
#                 WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
#                 ELSE public.ocr_data.ocr_text_1
#             END;
#         """
        
#         # print(f"Executing query: {query}")
#         # print(f"With values: {file_id}, {project_id}, {extracted_json}, {extracted_text}")
        
#         cur.execute(query, (file_id, project_id, extracted_json, extracted_text))
#         print("Query executed successfully.")

#         # Update ocr_status in public.files table
#         update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         cur.execute(update_status_query, (file_id,))
        
#         conn.commit()
#         print("message-------OCR data saved successfully through save_and_update_ocr_data")
#         return jsonify({"message": "OCR data saved successfully"}), 200
    
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
    
#     finally:
#         cur.close()
#         conn.close()





# mi 
# mi
# mi
# mi








# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # Check if file_id exists in the table
#         cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
#         result = cur.fetchone() # Fetch the first row from the result set 
        
#         if result:
#             # File exists, update the existing record
#             query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s" # Update the ocr_json_1 column for the file_id
#             cur.execute(query, (json.dumps(extracted_data), file_id)) # Execute the query with the extracted data and file_id
            
#             ocr_json_1, ocr_text_1 = result # Unpack the result into ocr_json_1 and ocr_text_1
            
#             # Update ocr_text_1 only if it is empty
#             if not ocr_text_1 or ocr_text_1.strip() == "":
#                 try:
#                     extracted_text = extracted_data.get("text", "").replace("\n", " ")
#                     update_query = "UPDATE public.ocr_data SET ocr_text_1 = %s WHERE file_id = %s"
#                     cur.execute(update_query, (extracted_text, file_id))
#                 except (json.JSONDecodeError, TypeError):
#                     return jsonify({"error": "Invalid JSON format in OCR data"}), 500
#         else:
#             # Insert new record
#             query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
#             cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
        
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()




# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data directly into ocr_json_1 and ocr_text_1 columns"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # Directly insert data into ocr_json_1 and ocr_text_1 columns
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         ocr_json_1 = EXCLUDED.ocr_json_1,
#         ocr_text_1 = EXCLUDED.ocr_text_1;
#         """
        
#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         cur.execute(query, (file_id, project_id, json.dumps(extracted_data), extracted_text))
        
#         conn.commit()
#         return jsonify({"message": "OCR data saved successfully"}), 200
    
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
    
#     finally:
#         cur.close()
#         conn.close()













# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # UPSERT query to insert or update the record
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         ocr_json_1 = EXCLUDED.ocr_json_1,
#         ocr_text_1 = CASE
#             WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
#             ELSE public.ocr_data.ocr_text_1
#         END;
#         """

#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         cur.execute(query, (file_id, project_id, json.dumps(extracted_data), extracted_text))
        
#         # Update ocr_status in public.files table
#         update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         cur.execute(update_status_query, (file_id,))
        
#         conn.commit()
#         return jsonify({"message": "OCR data saved successfully"}), 200

#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()








# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     """Save extracted OCR data and update ocr_text_1 if empty"""
#     conn = None
#     cur = None
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
        
#         extracted_text = extracted_data.get("text", "").replace("\n", " ")
#         ocr_json = json.dumps(extracted_data)
        
#         # Ensure file_id and project_id are valid
#         if not file_id or not project_id:
#             return jsonify({"error": "file_id and project_id cannot be null"}), 400
        
#         query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (file_id) DO UPDATE SET
#         project_id = EXCLUDED.project_id,
#         ocr_json_1 = EXCLUDED.ocr_json_1,
#         ocr_text_1 = CASE
#             WHEN public.ocr_data.ocr_text_1 IS NULL OR public.ocr_data.ocr_text_1 = '' THEN EXCLUDED.ocr_text_1
#             ELSE public.ocr_data.ocr_text_1
#         END
#         RETURNING file_id;
#         """
        
#         cur.execute(query, (file_id, project_id, ocr_json, extracted_text))
        
#         # Check if the row was affected
#         if cur.rowcount == 0:
#             return jsonify({"error": "Failed to insert/update OCR data"}), 500

#         # Update ocr_status in public.files table
#         update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
#         cur.execute(update_status_query, (file_id,))
        
#         conn.commit()
#         return jsonify({"message": "OCR data saved successfully"}), 200
    
#     except psycopg2.DatabaseError as e:
#         if conn:
#             conn.rollback()
#         return jsonify({"error": f"Database error: {str(e)}"}), 500
    
#     except Exception as e:
#         return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    
#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()


















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
            
            # Update ocr_text_1 only if it is empty or null
            if not ocr_text_1 or ocr_text_1.strip() == "":
                try:
                    extracted_text = extracted_data.get("text", "").replace("\n", " ")
                    update_query = "UPDATE public.ocr_data SET ocr_text_1 = %s WHERE file_id = %s"
                    cur.execute(update_query, (extracted_text, file_id))
                    print("updated ocr_text_1 successfully.")
                except (json.JSONDecodeError, TypeError):
                    return jsonify({"error": "Invalid JSON format in OCR data"}), 500
        else:
            # Insert new record
            query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
            cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
            print("insert ocr_text_1 successfully.")
        
        # Update ocr_status in public.files table
        update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = %s"
        cur.execute(update_status_query, (file_id,))
        print("updated ocr_status successfully.")

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
    # print(f"file_data: {file_data}")
    # return jsonify({"message": f"file_data: {file_data}"}), 200
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
    
        # # Load existing OCR JSON
        # with open(ocr_file_path, "r", encoding="utf-8") as json_file:
        #     extracted_data = json.load(json_file)

    else:
        # Step 5: Download File (only if not already present)
        pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension)
        if not pdf_file_path:
            return jsonify({"error": "Failed to download file"}), 500
        else:
            print(f"File downloaded successfully: {pdf_file_path}")
            # return jsonify({"message": f"File downloaded successfully: {pdf_file_path}"}), 200


        # Step 6: Perform OCR on File
        extracted_data = extract_text_with_confidence(pdf_file_path)
        print("OCR completed successfully.")
        # return jsonify({"message": "OCR completed successfully."}), 200


        # Step 7: Save OCR Output as JSON
        with open(ocr_file_path, "w", encoding="utf-8") as json_file:
            json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
            print(f"OCR JSON saved successfully: {ocr_file_path}")
            # return jsonify({"message": f"OCR JSON saved successfully: {ocr_file_path}"}), 200

            #print(file_id, project_id, extracted_data)

        # Step 8: Save and Update OCR Data 
        save_and_update_ocr_data(file_id, project_id, extracted_data)
        # print("OCR data saved successfully in the database.")
        return jsonify({"message": "Inserted successfully in DB in text column "}), 200



if __name__ == "__main__":
    # Define a sample pdf_file_path for testing
    # pdf_file_path = "download_file/download_pdf_1_18_37.pdf"
    
    app.run(debug=True, host="0.0.0.0", port=5000)
    # extracted_data = extract_text_with_confidence(pdf_file_path)
    # save_and_update_ocr_data(37, 18, extracted_data)

# ---------------------------------------- Actual Code ----------------------------------------









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
