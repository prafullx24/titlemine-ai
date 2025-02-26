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
import PyPDF2
from PyPDF2 import PdfReader, PdfWriter
from PyPDF2.errors import PdfReadError
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



# # This function saves the project ID and file IDs to a JSON file in the specified download folder.
# def save_project_files_to_json(project_id, user_id, files):
#     """Save project ID and file IDs to a JSON file."""
#     data = {
#         "project_id": project_id,
#         "file_ids": [file[0] for file in files]
#     }
#     file_path = os.path.join(DOWNLOAD_FOLDER, f"project_{user_id}_{project_id}_files.json")
#     with open(file_path, "w") as json_file:
#         json.dump(data, json_file)






# This function saves the project ID and file IDs to a variable and prints the value.
def save_project_files_to_variable(project_id, user_id, files):
    """Save project ID and file IDs to a variable and print the value."""
    data = {
        "project_id": project_id,
        "file_ids": [file[0] for file in files]
    }
    print(data)
    return data







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
        save_project_files_to_variable(project_id, user_id, files)
        print("file saved to variable")

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
    file_sizes = []  # List to store file names and their sizes


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

    # for file_path in downloaded_files:
    #     file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB
    #     print(f"Downloaded file: {file_path}, Size: {file_size:.2f} MB")


    # print(downloaded_files)
    # return downloaded_files

    for file_path in downloaded_files:
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB
        file_size_formatted = f"{file_size:.2f}"  # Format file size to 2 decimal places
        file_sizes.append({"file_name": os.path.basename(file_path), "file_size": file_size_formatted})
        # print(f"Downloaded file: {file_path}, Size: {file_size:.2f} MB")

    # Print file sizes
    print("File sizes:", file_sizes)

    print("All files file size printed successfully")
    return downloaded_files, file_sizes

















# def check_file_sizes_and_count_pages(downloaded_files, file_sizes):
#     """Check file sizes and count pages if size > 20 MB."""
#     def count_pages(file_path):
#         with open(file_path, 'rb') as file:
#             reader = PyPDF2.PdfReader(file)
#             return len(reader.pages)
        

#     def split_pdf(file_path, start_page, end_page, output_path):
#         with open(file_path, 'rb') as file:
#             reader = PyPDF2.PdfReader(file)
#             writer = PyPDF2.PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             with open(output_path, 'wb') as output_file:
#                 writer.write(output_file)

#     split_pdf_paths = []

#     for file_info in file_sizes:
#         if float(file_info['file_size']) > 20:
#             file_path = next((f for f in downloaded_files if os.path.basename(f) == file_info['file_name']), None)
#             if file_path:
#                 num_pages = count_pages(file_path)
#                 file_size = float(file_info['file_size'])
#                 page_size = file_size / num_pages
#                 print(f"File: {file_info['file_name']} has {num_pages} pages and size: {file_info['file_size']} MB.")
#                 print(f"Approximate size of a single page: {page_size:.2f} MB.")


#                 # Calculate the number of parts and print the split details
#                 parts = []
#                 start_page = 0
#                 # part_num = 1
#                 while start_page < num_pages:
#                     end_page = start_page + min(int(20 / page_size), 20)
#                     if end_page > num_pages:
#                         end_page = num_pages

#                     parts.append((start_page + 1, end_page))
#                     start_page = end_page

#                     print(f"Split main PDF and create {len(parts)} PDFs with sizes:")
#                     for i, (start, end) in enumerate(parts):
#                         part_size = (end - start + 1) * page_size
#                         print(f"Part {i + 1}: {part_size:.2f} MB, pages {start} to {end}")

#                     # Split the PDF into smaller PDFs
#                     start_page = 0
#                     part_num = 1
#                     while start_page < num_pages:
#                         end_page = start_page + min(int(20 / page_size), 20)
#                         if end_page > num_pages:
#                             end_page = num_pages
#                         output_path = f"{os.path.splitext(file_path)[0]}_part{part_num}.pdf"
#                         split_pdf(file_path, start_page, end_page, output_path)
#                         print(f"Created {output_path} with pages {start_page + 1} to {end_page}")
#                         start_page = end_page
#                         part_num += 1
    
#     return split_pdf_paths













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




# This function processes a document using Google Document AI to extract text and confidence scores for each text segment, returning the extracted data in a structured format.
# This updated function first checks the number of pages in the PDF. 
# If the number of pages is greater than 15, it splits the PDF into chunks of 15 pages and processes each chunk separately. 
# The extracted data from each chunk is then combined and returned.

# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    


#     def split_pdf(file_path, start_page, end_page):
#         with open(file_path, 'rb') as file:
#             reader = PyPDF2.PdfReader(file)
#             writer = PyPDF2.PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
#         with open(file_path, "rb") as file:
#             content = file.read() 
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
#         response = client.process_document(request=request) # Process the document using the client and request object
#         document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
#         extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
#         extracted_data = {
#             "text": extracted_text,
#             "confidence_scores": []
#         }
#         # Extract confidence scores for each text segment
#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
#                     confidence = block.layout.confidence # Extract the confidence score from the block layout 
#                     extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         #print(f"Text extracted with confidence scores: {extracted_data}")
#         return extracted_data
    
#     with open(file_path, 'rb') as file:
#         reader = PyPDF2.PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if num_pages > 15:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page)


#             # Check the size of the split file
#             split_file_size = os.path.getsize(split_file_path) / (1024 * 1024)  # Convert size to MB
#             if split_file_size > 20:
#                 print(f"File {split_file_path} is greater than 20 MB: {split_file_size:.2f} MB")
#             else:
#                 print(f"File {split_file_path} is less than 20 MB: {split_file_size:.2f} MB")



#             extracted_data.append(process_document(split_file_path))
#         return extracted_data
#     else:
#         return process_document(file_path)
















# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
#         with open(file_path, "rb") as file:
#             content = file.read() 
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
#         response = client.process_document(request=request) # Process the document using the client and request object
#         document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
#         extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
#         extracted_data = {
#             "text": extracted_text,
#             "confidence_scores": []
#         }
#         # Extract confidence scores for each text segment
#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
#                     confidence = block.layout.confidence # Extract the confidence score from the block layout 
#                     extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data
    




#     def split_and_process(file_path, max_size_mb=20):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#         # Check if the file size is greater than max_size_mb
#         if total_size_mb <= max_size_mb:
#             return [process_document(file_path)]

#         try:
#             with open(file_path, 'rb') as file:
#                 reader = PdfReader(file)
#                 total_pages = len(reader.pages)
#                 base_name = os.path.basename(file_path).replace('.pdf', '')
#                 # total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#                 size_per_page_mb = total_size_mb / total_pages

#                 start_page = 0
#                 part_num = 1
#                 extracted_data = []

#                 while start_page < total_pages:
#                     end_page = start_page
#                     current_size_mb = 0
#                     while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb:
#                         current_size_mb += size_per_page_mb
#                         end_page += 1

#                     split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#                     split_pdf(file_path, start_page, end_page)
#                     split_file_size = os.path.getsize(split_file_path) / (1024 * 1024)

#                     if split_file_size > max_size_mb:
#                         extracted_data.extend(split_and_process(split_file_path, max_size_mb))
#                     else:
#                         extracted_data.append(process_document(split_file_path))

#                     start_page = end_page

#             return extracted_data
        
#         except Exception as e:
#             print(f"Error in split_and_process: {e}")
#             return []


#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if num_pages >= 16:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page)
#             extracted_data.extend(split_and_process(split_file_path))
#         return extracted_data
    
#     if total_size_mb > 20:
#         return split_and_process(file_path)
#     else:
#         return process_document(file_path)












# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
#         with open(file_path, "rb") as file:
#             content = file.read() 
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
#         response = client.process_document(request=request) # Process the document using the client and request object
#         document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
#         extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
#         extracted_data = {
#             "text": extracted_text,
#             "confidence_scores": []
#         }
#         # Extract confidence scores for each text segment
#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
#                     confidence = block.layout.confidence # Extract the confidence score from the block layout 
#                     extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, max_size_mb=20):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#         # Check if the file size is greater than max_size_mb
#         if total_size_mb <= max_size_mb:
#             return [process_document(file_path)]

#         try:
#             with open(file_path, 'rb') as file:
#                 reader = PdfReader(file)
#                 total_pages = len(reader.pages)
#                 base_name = os.path.basename(file_path).replace('.pdf', '')
#                 size_per_page_mb = total_size_mb / total_pages

#                 start_page = 0
#                 part_num = 1
#                 extracted_data = []

#                 while start_page < total_pages:
#                     end_page = start_page
#                     current_size_mb = 0
#                     while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb:
#                         current_size_mb += size_per_page_mb
#                         end_page += 1

#                     split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#                     split_pdf(file_path, start_page, end_page)
#                     split_file_size = os.path.getsize(split_file_path) / (1024 * 1024)

#                     if split_file_size > max_size_mb:
#                         extracted_data.extend(split_and_process(split_file_path, max_size_mb))
#                     else:
#                         extracted_data.append(process_document(split_file_path))

#                     start_page = end_page

#             return extracted_data
        
#         except Exception as e:
#             print(f"Error in split_and_process: {e}")
#             return []

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if num_pages > 15 or total_size_mb > 20:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page)
#             extracted_data.extend(split_and_process(split_file_path))
#         return extracted_data
#     else:
#         return process_document(file_path)







# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page, part_num):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}_part{part_num}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient() # Create a Document Processor Service Client object for Document AI which is used to process documents 
#         with open(file_path, "rb") as file:
#             content = file.read() 
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf") # Create a Raw Document object with the file content and mime type
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}" # Define the processor name 
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document) # Create a Process Request object with the processor name and raw document
#         response = client.process_document(request=request) # Process the document using the client and request object
#         document_dict = documentai.Document.to_dict(response.document) # Convert the document response to a dictionary
#         extracted_text = document_dict.get("text", "") # Extract the text from the document dictionary
#         extracted_data = {
#             "text": extracted_text,
#             "confidence_scores": []
#         }
#         # Extract confidence scores for each text segment
#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index] # Extract the text segment from the document dictionary 
#                     confidence = block.layout.confidence # Extract the confidence score from the block layout 
#                     extracted_data["confidence_scores"].append({ # Append the confidence score to the extracted data 
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, max_size_mb=20):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#         # Check if the file size is greater than max_size_mb
#         if total_size_mb <= max_size_mb:
#             return [process_document(file_path)]

#         try:
#             with open(file_path, 'rb') as file:
#                 reader = PdfReader(file)
#                 total_pages = len(reader.pages)
#                 base_name = os.path.basename(file_path).replace('.pdf', '')
#                 size_per_page_mb = total_size_mb / total_pages

#                 start_page = 0
#                 part_num = 1
#                 extracted_data = []

#                 while start_page < total_pages:
#                     end_page = start_page
#                     current_size_mb = 0
#                     while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb:
#                         current_size_mb += size_per_page_mb
#                         end_page += 1

#                     split_file_path = split_pdf(file_path, start_page, end_page, part_num)
#                     split_file_size = os.path.getsize(split_file_path) / (1024 * 1024)

#                     if split_file_size > max_size_mb:
#                         extracted_data.extend(split_and_process(split_file_path, max_size_mb))
#                     else:
#                         extracted_data.append(process_document(split_file_path))

#                     start_page = end_page
#                     part_num += 1

#             return extracted_data
        
#         except Exception as e:
#             print(f"Error in split_and_process: {e}")
#             return []

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)

#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if num_pages > 15 or total_size_mb > 20:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page, 1)
#             extracted_data.extend(split_and_process(split_file_path))
#         return extracted_data
#     else:
#         return process_document(file_path)











# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page, part_num, parent_range):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_{parent_range}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient()
#         with open(file_path, "rb") as file:
#             content = file.read()
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document)
#         response = client.process_document(request=request)
#         document_dict = documentai.Document.to_dict(response.document)
#         extracted_text = document_dict.get("text", "")
#         extracted_data = {"text": extracted_text, "confidence_scores": []}

#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index]
#                     confidence = block.layout.confidence
#                     extracted_data["confidence_scores"].append({
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, parent_range, max_size_mb=20, max_pages=15):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             total_pages = len(reader.pages)
#             size_per_page_mb = total_size_mb / total_pages

#         if total_size_mb <= max_size_mb and total_pages <= max_pages:
#             return [process_document(file_path)]

#         start_page = 0
#         part_num = 1
#         extracted_data = []

#         while start_page < total_pages:
#             end_page = start_page
#             current_size_mb = 0
#             while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
#                 current_size_mb += size_per_page_mb
#                 end_page += 1

#             split_file_path = split_pdf(file_path, start_page, end_page, part_num, parent_range)
#             extracted_data.append(process_document(split_file_path))

#             start_page = end_page
#             part_num += 1

#         return extracted_data

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if total_size_mb > 20 or num_pages > 15:
#         extracted_data = []
#         parent_range = f"pages_1_to_{num_pages}"
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page, 1, parent_range)
#             extracted_data.extend(split_and_process(split_file_path, f"pages_{start_page+1}_to_{end_page}"))
#         return extracted_data
#     else:
#         return process_document(file_path)











# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page, part_num):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient()
#         with open(file_path, "rb") as file:
#             content = file.read()
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document)
#         response = client.process_document(request=request)
#         document_dict = documentai.Document.to_dict(response.document)
#         extracted_text = document_dict.get("text", "")
#         extracted_data = {"text": extracted_text, "confidence_scores": []}

#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index]
#                     confidence = block.layout.confidence
#                     extracted_data["confidence_scores"].append({
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, max_size_mb=20, max_pages=15):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             total_pages = len(reader.pages)
#             size_per_page_mb = total_size_mb / total_pages

#         if total_size_mb <= max_size_mb and total_pages <= max_pages:
#             return [process_document(file_path)]

#         extracted_data = []
#         start_page = 0
#         while start_page < total_pages:
#             end_page = start_page
#             current_size_mb = 0
#             while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
#                 current_size_mb += size_per_page_mb
#                 end_page += 1

#             split_file_path = split_pdf(file_path, start_page, end_page, start_page + 1)
#             extracted_data.extend(split_and_process(split_file_path))

#             start_page = end_page

#         return extracted_data

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)
    
#     if total_size_mb > 20 or num_pages > 15:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page, start_page + 1)
#             extracted_data.extend(split_and_process(split_file_path))
#         return extracted_data
#     else:
#         return process_document(file_path)





















# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page, part_num, parent_range):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_{parent_range}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient()
#         with open(file_path, "rb") as file:
#             content = file.read()
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document)

#         # Debugging statement to log request details
#         print(f"Processing document: {file_path}, Size: {len(content)} bytes")

#         response = client.process_document(request=request)

#         # Debugging statement to log response details
#         # print(f"Document processed: {file_path}, Response: {response}")
#         print(f"Document processed: Done")

#         document_dict = documentai.Document.to_dict(response.document)
#         extracted_text = document_dict.get("text", "")
#         extracted_data = {"text": extracted_text, "confidence_scores": []}

#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index]
#                     confidence = block.layout.confidence
#                     extracted_data["confidence_scores"].append({
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, parent_range, max_size_mb=20, max_pages=15):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             total_pages = len(reader.pages)
#             size_per_page_mb = total_size_mb / total_pages

#         if total_size_mb <= max_size_mb and total_pages <= max_pages:
#             return [process_document(file_path)]

#         start_page = 0
#         part_num = 1
#         extracted_data = []

#         while start_page < total_pages:
#             end_page = start_page
#             current_size_mb = 0
#             while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
#                 current_size_mb += size_per_page_mb
#                 end_page += 1

#             split_file_path = split_pdf(file_path, start_page, end_page, part_num, parent_range)
#             extracted_data.append(process_document(split_file_path))

#             start_page = end_page
#             part_num += 1

#         return extracted_data

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#     if total_size_mb > 20:
#         return split_and_process(file_path)

#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)

#     if num_pages > 15:
#         extracted_data = []
#         parent_range = f"pages_1_to_{num_pages}"
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page, 1, parent_range)
#             extracted_data.extend(split_and_process(split_file_path, f"pages_{start_page+1}_to_{end_page}"))
#         return extracted_data

#     else:
#         return process_document(file_path)
    









# def extract_text_with_confidence(file_path):
#     """Extracts text and confidence scores from a document using Google Document AI"""
    
#     if not os.path.exists(credentials_path):
#         raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

#     def split_pdf(file_path, start_page, end_page):
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             writer = PdfWriter()
#             for page_num in range(start_page, end_page):
#                 writer.add_page(reader.pages[page_num])
#             split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
#             with open(split_file_path, 'wb') as output_file:
#                 writer.write(output_file)
#             return split_file_path

#     def process_document(file_path):
#         client = documentai.DocumentProcessorServiceClient()
#         with open(file_path, "rb") as file:
#             content = file.read()
#         raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
#         name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
#         request = documentai.ProcessRequest(name=name, raw_document=raw_document)

#         # Debugging statement to log request details
#         print(f"Processing document: {file_path}, Size: {len(content)} bytes")

#         response = client.process_document(request=request)

#         # Debugging statement to log response details
#         print(f"Document processed: Done")

#         document_dict = documentai.Document.to_dict(response.document)
#         extracted_text = document_dict.get("text", "")
#         extracted_data = {"text": extracted_text, "confidence_scores": []}

#         for page in response.document.pages:
#             for block in page.blocks:
#                 for segment in block.layout.text_anchor.text_segments:
#                     segment_text = document_dict["text"][segment.start_index:segment.end_index]
#                     confidence = block.layout.confidence
#                     extracted_data["confidence_scores"].append({
#                         "text": segment_text,
#                         "confidence": confidence
#                     })

#         return extracted_data

#     def split_and_process(file_path, max_size_mb=20, max_pages=15):
#         total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#         with open(file_path, 'rb') as file:
#             reader = PdfReader(file)
#             total_pages = len(reader.pages)
#             size_per_page_mb = total_size_mb / total_pages

#         if total_size_mb <= max_size_mb and total_pages <= max_pages:
#             return [process_document(file_path)]

#         start_page = 0
#         extracted_data = []

#         while start_page < total_pages:
#             end_page = start_page
#             current_size_mb = 0
#             while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
#                 current_size_mb += size_per_page_mb
#                 end_page += 1

#             split_file_path = split_pdf(file_path, start_page, end_page)
#             extracted_data.append(process_document(split_file_path))

#             start_page = end_page

#         return extracted_data

#     total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#     # if total_size_mb > 20:
#     #     return split_and_process(file_path)

#     with open(file_path, 'rb') as file:
#         reader = PdfReader(file)
#         num_pages = len(reader.pages)

#     if total_size_mb > 20:
#         return split_and_process(file_path)

#     elif num_pages > 15:
#         extracted_data = []
#         for start_page in range(0, num_pages, 15):
#             end_page = min(start_page + 15, num_pages)
#             split_file_path = split_pdf(file_path, start_page, end_page)
#             extracted_data.extend(split_and_process(split_file_path))
#         return extracted_data

#     else:
#         return process_document(file_path)










def extract_text_with_confidence(file_path):
    """Extracts text and confidence scores from a document using Google Document AI"""
    
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    def split_pdf(file_path, start_page, end_page):
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            writer = PdfWriter()
            for page_num in range(start_page, end_page):
                writer.add_page(reader.pages[page_num])
            split_file_path = f"{os.path.splitext(file_path)[0]}_pages_{start_page+1}_to_{end_page}.pdf"
            with open(split_file_path, 'wb') as output_file:
                writer.write(output_file)
            return split_file_path

    def process_document(file_path):
        client = documentai.DocumentProcessorServiceClient()
        with open(file_path, "rb") as file:
            content = file.read()
        raw_document = documentai.RawDocument(content=content, mime_type="application/pdf")
        name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}"
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)

        # Debugging statement to log request details
        print(f"Processing document: {file_path}, Size: {len(content)} bytes")

        response = client.process_document(request=request)

        # Debugging statement to log response details
        print(f"Document processed: Done")

        document_dict = documentai.Document.to_dict(response.document)
        extracted_text = document_dict.get("text", "")
        extracted_data = {"text": extracted_text, "confidence_scores": []}

        for page in response.document.pages:
            for block in page.blocks:
                for segment in block.layout.text_anchor.text_segments:
                    segment_text = document_dict["text"][segment.start_index:segment.end_index]
                    confidence = block.layout.confidence
                    extracted_data["confidence_scores"].append({
                        "text": segment_text,
                        "confidence": confidence
                    })

        return extracted_data

    def split_and_process(file_path, max_size_mb=20, max_pages=15):
        total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            total_pages = len(reader.pages)
            size_per_page_mb = total_size_mb / total_pages

        if total_size_mb <= max_size_mb and total_pages <= max_pages:
            return [process_document(file_path)]

        start_page = 0
        extracted_data = []

        while start_page < total_pages:
            end_page = start_page
            current_size_mb = 0
            while end_page < total_pages and current_size_mb + size_per_page_mb <= max_size_mb and (end_page - start_page) < max_pages:
                current_size_mb += size_per_page_mb
                end_page += 1

            split_file_path = split_pdf(file_path, start_page, end_page)
            extracted_data.extend(split_and_process(split_file_path))

            start_page = end_page

        return extracted_data

    total_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    with open(file_path, 'rb') as file:
        reader = PdfReader(file)
        num_pages = len(reader.pages)

        # Extract file ID from the file path
    file_id = os.path.basename(file_path).split('_')[4].split('.')[0]

    if total_size_mb > 20 and num_pages == 1:
        # print('Splitting for this file is not possible !')
        print(f'Splitting for this file is not possible! File ID: {file_id}')
    elif total_size_mb > 20:
        return split_and_process(file_path)
    elif num_pages > 15:
        extracted_data = []
        for start_page in range(0, num_pages, 15):
            end_page = min(start_page + 15, num_pages)
            split_file_path = split_pdf(file_path, start_page, end_page)
            extracted_data.extend(split_and_process(split_file_path))
        return extracted_data
    else:
        return process_document(file_path)

    











# This function processes multiple documents using Google Document AI to extract text and confidence scores, saves the extracted data as JSON files, and returns the aggregated results.
def extract_text_with_confidence_batch(downloaded_files, file_sizes):
    """Extracts text and confidence scores from multiple documents using Google Document AI."""
    
    all_extracted_data = [] # List to store all extracted data from multiple documents

    def process_file(file_path):
        try:
            print(f"Processing file: {file_path}")
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
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None

    

    # with ThreadPoolExecutor(max_workers=5) as executor:  # Limit to 5 concurrent threads
    with ThreadPoolExecutor() as executor:  # No limit on concurrent threads
        results = list(executor.map(process_file, downloaded_files))

    # Filter out any None results due to errors
    results = [result for result in results if result is not None]

    all_extracted_data.extend(results)

    # Save all OCR outputs as JSON
    save_ocr_outputs_as_json(all_extracted_data)
    print("Done with Extracts text and confidence scores ")
    return all_extracted_data























# # This function inserts or updates OCR data for multiple files in the database and updates their OCR status to 'Completed'.
# def save_and_update_ocr_data_batch(project_id, all_extracted_data, db_config):
#     conn = psycopg2.connect(**db_config)
#     cur = conn.cursor()
    
#     try:
#         new_records = [
#             (data['file_id'], project_id, json.dumps(data['extracted_data']), data['extracted_data'].get('text', '').replace("\n", " ")) #Converts the extracted_data dictionary to a JSON-formatted string.
#             for data in all_extracted_data
#         ]
        
#         insert_query = """
#         INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
#         VALUES %s
#         ON CONFLICT (file_id, project_id) 
#         DO UPDATE SET 
#             ocr_json_1 = EXCLUDED.ocr_json_1,
#             ocr_text_1 = EXCLUDED.ocr_text_1
#         """
        
#         psycopg2.extras.execute_values(cur, insert_query, new_records)
        
#         file_ids = [data['file_id'] for data in all_extracted_data]
#         update_status_query = "UPDATE public.files SET ocr_status = 'completed' WHERE id = ANY(%s::int[])" # The %s::int[] placeholder is used to safely insert the list of file IDs into the query.
#         cur.execute(update_status_query, (file_ids,))
        
#         conn.commit()
#         # print("Bulk insert and update executed successfully")
#     except Exception as e:
#         conn.rollback()
#         print("Error in save_and_update_ocr_data_batch:", e)
#     finally:
#         cur.close()
#         conn.close()









@app.route("/api/v1/batch_ocr/<int:project_id>", methods=["POST"])
def batch_ocr(project_id):
    """Batch process OCR for all files under a project."""

    # step 1 - get files by project and store file ids in a json file which ocr_status is not completed
    files = get_files_by_project(project_id)
    # id, user_id, project_id, file_name, s3_url, ocr_status = files
    # print(files)
    if not files:
        return jsonify({"error": "No files found for this project."}), 404

    # return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200





    # step 2 - download files from s3
    downloaded_files,file_sizes = download_files_concurrently(files)
    # return jsonify({"message": "Inserted/Updated Data successfully in DataBase", "file_sizes": file_sizes}), 200
    


    # # # step 3- check_file_sizes_and_count_pages
    # split_pdf_paths = check_file_sizes_and_count_pages(downloaded_files, file_sizes)
    # return jsonify({"message": "Inserted/Updated Data successfully in DataBase", "split_pdf_paths": split_pdf_paths}), 200

    # Step 3: Perform OCR on all downloaded File
    all_extracted_data = extract_text_with_confidence_batch(downloaded_files, file_sizes)
    return jsonify({"message": "Batch file download processing completed."}), 200


    # # Step 4: Save and Update OCR Data 
    # save_and_update_ocr_data_batch(project_id, all_extracted_data, DB_CONFIG)
    # print("OCR data saved successfully in the database.")
    # return jsonify({"message": "Inserted/Updated Data successfully in DataBase"}), 200



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)