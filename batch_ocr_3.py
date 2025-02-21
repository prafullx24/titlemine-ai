import os
import psycopg2
import psycopg2.extras  # Import the extras module
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







# Save project ID and file IDs to a JSON file
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
    query = "SELECT id, user_id, %s as project_id, file_name, s3_url, ocr_status FROM public.files WHERE project_id = %s AND ocr_status != 'completed'"
    cur.execute(query, (project_id,project_id))
    files = cur.fetchall()
    cur.close()
    conn.close()

    # Save project ID and file IDs to a JSON file
    if files:
        user_id = files[0][1]
        save_project_files_to_json(project_id, user_id, files)

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
            for chunk in response.iter_content(1024): 
                file.write(chunk) # Write the chunk to the file 
        return file_path 
    return None





# The code defines a function to download files concurrently from S3 URLs using a thread pool, handling errors and printing the download status for each file.
def download_files_concurrently(files):
    downloaded_files = []

    def download_file(file):
        try:
            id, user_id, project_id, file_name, s3_url, ocr_status = file
            file_extension = os.path.splitext(file_name)[1] # Get the file extension 
            pdf_file_path = download_file_from_s3(s3_url, user_id, project_id, id, file_extension)
            if pdf_file_path:
                #print(f"File downloaded successfully: {pdf_file_path}")
                downloaded_files.append(pdf_file_path)
            else:
                print(f"Failed to download file: {file_name}")
        except Exception as e:
            print(f"Error downloading file {file}: {e}")

    if not files:
        print("No files to download.")
        return []

    with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent downloads
        executor.map(download_file, files)

    # print("All files downloaded")
    # print(downloaded_files)
    return downloaded_files














# # The extract_text_with_confidence function processes a document using Google Document AI to extract text and confidence scores for each text segment.
# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
#     client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
#     with open(file_path, "rb") as file:
#         content = file.read() 
#     raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
#     name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
#     request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
#     response = client.process_document(request=request) # Process the document using the client and request object
#     document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
#     extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
#     extracted_data = {
#         "text": extracted_text,
#         "confidence_scores": []
#     }
#     # Extract confidence scores for each text segment
#     for page in response.document.pages:
#         for block in page.blocks:
#             for segment in block.layout.text_anchor.text_segments:
#                 segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
#                 confidence = block.layout.confidence # Extract the confidence score from the block layout 
#                 extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
#                     "text": segment_text,
#                     "confidence": confidence
#                 })
#     return extracted_data
















# def extract_text_with_confidence_batch(downloaded_files):
#     """Extracts text and confidence scores from multiple documents using Google Document AI batch processing."""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
#     client = documentai.DocumentProcessorServiceClient()  # Create a Document Processor Service Client object
#     name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"  # Define the processor name
    
#     raw_documents = []
#     for file_path in downloaded_files:
#         with open(file_path, "rb") as file:
#             content = file.read()
#         raw_documents.append(documentai.RawDocument(content=content, mime_type="application/pdf"))
    
#     input_config = documentai.BatchDocumentsInputConfig(
#         documents=[documentai.RawDocument(content=doc.content, mime_type=doc.mime_type) for doc in raw_documents]
#     )

#     request = documentai.BatchProcessRequest(
#         name=name,
#         input_documents=input_config
#         )
    
    
#     operation = client.batch_process_documents(request)
#     response = operation.result()
    
#     all_extracted_data = []
#     for document in response.documents:
#         document_dict = documentai.Document.to_dict(document)  # Convert the document response to a dictionary
#         extracted_text = document_dict.get("text", "")
#         extracted_data = {
#             "text": extracted_text,
#             "confidence_scores": []
#         }
        
#         for page in document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index]
#                     confidence = block.layout.confidence
#                     extracted_data["confidence_scores"].append({
#                         "text": segment_text,
#                         "confidence": confidence
#                     })
        
#         all_extracted_data.append(extracted_data)
    
#     return all_extracted_data







def save_ocr_output_as_json(user_id, project_id, file_id, extracted_data):
    """Save OCR output as JSON."""
    ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_json_{user_id}_{project_id}_{file_id}.json")
    with open(ocr_file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
        #print(f"OCR JSON saved successfully: {ocr_file_path}")







def save_ocr_outputs_as_json(extracted_data_list):
    """Save multiple OCR outputs as JSON."""
    for data in extracted_data_list:
        user_id = data['user_id']
        project_id = data['project_id']
        file_id = data['file_id']
        extracted_data = data['extracted_data']
        save_ocr_output_as_json(user_id, project_id, file_id, extracted_data)






# def extract_text_with_confidence_batch(downloaded_files):
#     """Extracts text and confidence scores from multiple documents using Google Document AI."""
    
#     all_extracted_data = []
    
#     for file_path in downloaded_files:
#         extracted_data = extract_text_with_confidence(file_path)
#         #all_extracted_data.append(extracted_data)
        
#         # Extract user_id, project_id, and file_id from the file name
#         file_name_parts = os.path.basename(file_path).split('_')
#         user_id = file_name_parts[2]
#         project_id = file_name_parts[3]
#         file_id = file_name_parts[4].split('.')[0]
        
#         # # Save OCR Output as JSON
#         # ocr_file_path = os.path.join(DOWNLOAD_FOLDER, f"download_json_{user_id}_{project_id}_{file_id}.json")
#         # with open(ocr_file_path, "w", encoding="utf-8") as json_file:
#         #     json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
#         #     print(f"OCR JSON saved successfully: {ocr_file_path}")
    
#         # Save OCR Output as JSON
#         # save_ocr_output_as_json(user_id, project_id, file_id, extracted_data)
    

#         all_extracted_data.append({
#             'user_id': user_id,
#             'project_id': project_id,
#             'file_id': file_id,
#             'extracted_data': extracted_data
#         })
    
#     # Save all OCR outputs as JSON
#     save_ocr_outputs_as_json(all_extracted_data)
#     return all_extracted_data











def extract_text_with_confidence_batch(downloaded_files):
    """Extracts text and confidence scores from multiple documents using Google Document AI."""
    
    all_extracted_data = []

    def process_file(file_path):
        extracted_data = extract_text_with_confidence(file_path)
        
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

    with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent threads
        results = list(executor.map(process_file, downloaded_files))

    all_extracted_data.extend(results)
    
    # Save all OCR outputs as JSON
    save_ocr_outputs_as_json(all_extracted_data)
    return all_extracted_data












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


















# # The `save_and_update_ocr_data` function saves extracted OCR data to the database and updates `ocr_text_1` or inserts a new record if it doesn't exist.
# def save_and_update_ocr_data(file_id, project_id, extracted_data):
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         # Check if file_id exists in the table
#         cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
#         result = cur.fetchone() # Fetch the first row from the result set 
        
#         if result:
#             # File entry exists in ocr_data, update the existing record
#             try:
#                 extracted_text = extracted_data.get("text", "").replace("\n", " ")                
#             except (json.JSONDecodeError, TypeError):
#                 query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s"
#                 cur.execute(query, (json.dumps(extracted_data), file_id))
#                 return jsonify({"error": "Invalid JSON format in OCR data"}), 500

#             query = "UPDATE public.ocr_data SET ocr_text_1 = %s, ocr_json_1 = %s WHERE file_id = %s"
#             cur.execute(query, (extracted_text, json.dumps(extracted_data), file_id)) 
#         else:
#             # Insert new record
#             query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
#             cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
        
#         # Update ocr_status in public.files table
#         update_status_query = "UPDATE public.files SET ocr_status = 'Extracting Data' WHERE id = %s"
#         cur.execute(update_status_query, (file_id))

#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()















# def save_and_update_ocr_data_batch(project_id, all_extracted_data):
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         for data in all_extracted_data:
#             file_id = data['file_id']
#             extracted_data = data['extracted_data']
            
#             # Check if file_id exists in the table
#             cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id,))
#             result = cur.fetchone() # Fetch the first row from the result set 
            
#             if result:
#                 # File entry exists in ocr_data, update the existing record
#                 try:
#                     extracted_text = extracted_data.get("text", "").replace("\n", " ")                
#                 except (json.JSONDecodeError, TypeError):
#                     query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s"
#                     cur.execute(query, (json.dumps(extracted_data), file_id))
#                     return jsonify({"error": "Invalid JSON format in OCR data"}), 500

#                 query = "UPDATE public.ocr_data SET ocr_text_1 = %s, ocr_json_1 = %s WHERE file_id = %s"
#                 cur.execute(query, (extracted_text, json.dumps(extracted_data), file_id)) 
#             else:
#                 # Insert new record
#                 query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES (%s, %s, %s)"
#                 cur.execute(query, (file_id, project_id, json.dumps(extracted_data)))
            
#             # Update ocr_status in public.files table
#             update_status_query = "UPDATE public.files SET ocr_status = 'Completed' WHERE id = %s"
#             cur.execute(update_status_query, (file_id))

#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()






# def save_and_update_ocr_data_batch(project_id, all_extracted_data):
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         new_records = []
#         for data in all_extracted_data:
#             file_id = data['file_id']
#             extracted_data = data['extracted_data']

#             print("Extracted data Query execute !" )

#             # Check if file_id exists in the table
#             cur.execute("SELECT ocr_json_1, ocr_text_1 FROM public.ocr_data WHERE file_id = %s", (file_id))
#             result = cur.fetchone() # Fetch the first row from the result set 

#             print("Select Query ecetuted !")

#             if result:
#                 # File entry exists in ocr_data, update the existing record
#                 try:
#                     extracted_text = extracted_data.get("text", "").replace("\n", " ")                

#                     print("try block executed !")

#                 except (json.JSONDecodeError, TypeError):
#                     query = "UPDATE public.ocr_data SET ocr_json_1 = %s WHERE file_id = %s"
#                     cur.execute(query, (json.dumps(extracted_data), file_id))
#                     conn.rollback()

#                     print("except block executed !")

#                     return jsonify({"error": "Invalid JSON format in OCR data"}), 500

#                 query = "UPDATE public.ocr_data SET ocr_text_1 = %s, ocr_json_1 = %s WHERE file_id = %s"
#                 cur.execute(query, (extracted_text, json.dumps(extracted_data), file_id)) 

#                 print("Update Query executed !")
#             else:
#                 # Collect new records for bulk insert
#                 new_records.append((file_id, project_id, json.dumps(extracted_data)))

#                 print("else block executed !")


#             # Update ocr_status in public.files table
#             update_status_query = "UPDATE public.files SET ocr_status = 'Completed' WHERE id = %s"
#             cur.execute(update_status_query, (file_id))

#             print("Update status Query executed for ocr status!")

#         # Perform bulk insert for new records
#         if new_records:
#             insert_query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES %s"
#             psycopg2.extras.execute_values(cur, insert_query, new_records)

#             print("Bulk insert query executed")

#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         print("Error in save_and_update_ocr_data_batch")
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()
















# def save_and_update_ocr_data_batch(project_id, all_extracted_data):
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()
    
#     try:
#         new_records = []
#         update_status_queries = []

#         for data in all_extracted_data:
#             file_id = data['file_id']
#             extracted_data = data['extracted_data']
#             new_records.append((file_id, project_id, json.dumps(extracted_data)))
#             update_status_queries.append((file_id,))

#         # Perform bulk insert for new records
#         insert_query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES %s"
#         psycopg2.extras.execute_values(cur, insert_query, new_records)

#         # Perform bulk update for ocr_status
#         update_status_query = "UPDATE public.files SET ocr_status = 'Completed' WHERE id = %s"
#         psycopg2.extras.execute_batch(cur, update_status_query, update_status_queries)

#         conn.commit()
#         print("Bulk insert and update queries executed successfully")
#     except Exception as e:
#         conn.rollback()
#         print("Error in save_and_update_ocr_data_batch:", str(e))
#         return jsonify({"error": str(e)}), 500
#     finally:
#         cur.close()
#         conn.close()













# def save_and_update_ocr_data_batch(project_id, all_extracted_data, db_config):
#     conn = psycopg2.connect(**db_config)
#     cur = conn.cursor()
    
#     try:
#         new_records = [(data['file_id'], project_id, json.dumps(data['extracted_data'])) for data in all_extracted_data]
        
#         insert_query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES %s"
#         psycopg2.extras.execute_values(cur, insert_query, new_records)
        
#         file_ids = [data['file_id'] for data in all_extracted_data]
#         update_status_query = "UPDATE public.files SET ocr_status = 'Completed' WHERE id = ANY(%s)"
#         cur.execute(update_status_query, (file_ids,))
        
#         conn.commit()
#         print("Bulk insert and update executed successfully")
#     except Exception as e:
#         conn.rollback()
#         print("Error in save_and_update_ocr_data_batch", e)
#     finally:
#         cur.close()
#         conn.close()






def save_and_update_ocr_data_batch(project_id, all_extracted_data, db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    try:
        new_records = [(data['file_id'], project_id, json.dumps(data['extracted_data'])) for data in all_extracted_data]
        
        insert_query = "INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1) VALUES %s"
        psycopg2.extras.execute_values(cur, insert_query, new_records)
        
        file_ids = [data['file_id'] for data in all_extracted_data]
        update_status_query = "UPDATE public.files SET ocr_status = 'Completed' WHERE id = ANY(%s::int[])"
        cur.execute(update_status_query, (file_ids,))
        
        conn.commit()
        print("Bulk insert and update executed successfully")
    except Exception as e:
        conn.rollback()
        print("Error in save_and_update_ocr_data_batch", e)
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
    print("OCR completed successfully for all files.")
    
    # return jsonify({"message": "Batch file download processing completed."}), 200

    # Step 4: Save and Update OCR Data 
    # for data in all_extracted_data:
    #     file_id = data['file_id']
    #     extracted_data = data['extracted_data']
    #     save_and_update_ocr_data(file_id, project_id, extracted_data)
    #     print("OCR data saved successfully in the database.")


    # Step 4: Save and Update OCR Data 
    save_and_update_ocr_data_batch(project_id, all_extracted_data, DB_CONFIG)
    print("OCR data saved successfully in the database.")

    return jsonify({"message": "Inserted successfully in DB in text column "}), 200





if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
