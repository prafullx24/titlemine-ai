"""

This file is expected to contain all the code related to Project Information retrieval.
As per the development plan, this module is responsible for fetching project-related data, including file counts, OCR data, extracted data availability, and runsheet completion status.

The code in this file is expected to provide the following:

- A Flask endpoint that takes `project_id` as input.
- Retrieve the total number of files under the given `project_id` from the database.
- Count the number of files with OCR status as "Completed" and "Not Completed".
- Fetch OCR-related data, including how many times OCR has been performed on a file.
- Check if the extracted data for these files is available in the `extracted_data` table.
- Validate the completeness of the runsheet data for each file in the project.
- Return all this data in a structured JSON format.

Limitations:

- The current implementation relies on PostgreSQL queries and assumes proper indexing for efficiency.
- The completeness check for runsheet data assumes that all necessary columns exist in the database schema.
- Extracted data verification is based on file IDs available in the `extracted_data` table.

To-Do:

- Improve query performance for large datasets.
- Introduce error handling improvements for better debugging.

API Endpoint:
`https://host:port/api/v1/project_info/:project_id`

Response:

Success:
```json
{
  "project_id": "123",
  "total_files": 100,
  "No. of ocr completed_files": 80,
  "No. of ocr not_completed_files": 20,
  "ocr_data": [
    {
      "file_id": "xyz",
      "perform_ocr_for_times": 2,
      "not_perform_ocr_for_times": 1
    }
  ],
  "file_ids": ["file1", "file2", ...],
  "Extraction performed on": ["file1", "file3"],
  "Extraction not performed on": ["file2"],
  "Runsheet": {
    "Runsheet complete_file_ids": ["file1"],
    "Runsheet incomplete_file_ids": ["file2"],
    "Runsheet incomplete_file_ids columns are": {
      "file2": ["grantor", "grantee"]
    }
  }
}
```

Failure:
```json
{
  "error": "Unable to fetch file counts"
}
```

Libraries Used:

- Flask
- psycopg2
- dotenv
- requests
- google.cloud
- json
- os

The `.env` file is expected to contain the following environment variables:

```
DB_NAME=
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=

PROJECT_ID=
LOCATION=
PROCESSOR_ID=
CREDENTIALS_PATH=
```
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



# "Get the total number of files for a given project_id from the Files table.
# count the number of files with OCR status 'completed'.
# count the number of files with OCR status not 'completed'


def get_total_files(project_id):
    """Get the total number of files for a given project_id from the Files table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG) 
        cur = conn.cursor() # cursor object to execute queries

        # Combined query to count total files, completed files, and not completed files
        query = """
        SELECT 
            COUNT(*) AS total_files,
            COUNT(CASE WHEN ocr_status = 'completed' THEN 1 END) AS completed_files,
            COUNT(CASE WHEN ocr_status != 'completed' THEN 1 END) AS not_completed_files
        FROM Files 
        WHERE project_id = %s;
        """
        cur.execute(query, (project_id,))
        result = cur.fetchone()
        total_files, completed_files, not_completed_files = result

        # Query to get all file IDs
        query_file_ids = """
        SELECT id FROM Files WHERE project_id = %s;
        """
        cur.execute(query_file_ids, (project_id,))
        file_ids = [row[0] for row in cur.fetchall()]

        # Close the cursor and connection
        cur.close()
        conn.close()

        # Print all file IDs
        # print("File IDs:", file_ids)

        return total_files, completed_files, not_completed_files, file_ids
    
    except Exception as e:
        print(f"Error fetching total files: {e}")
        return None,None,None,None





# function retrieves OCR data for a given project_id from the ocr_data table, 
# counting the number of non-null and null OCR columns for each file, 
# and returns the results in JSON format.

def ocr_data(project_id):
    """Get OCR data for a given project_id from the ocr_data table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Query to get file_id and count of non-null and null OCR columns
        query = """
        SELECT 
            file_id,
            (CASE WHEN ocr_text_1 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ocr_text_2 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ocr_text_3 IS NOT NULL THEN 1 ELSE 0 END) AS ocr_performed,

            (CASE WHEN ocr_text_1 IS NULL THEN 1 ELSE 0 END +
            CASE WHEN ocr_text_2 IS NULL THEN 1 ELSE 0 END +
            CASE WHEN ocr_text_3 IS NULL THEN 1 ELSE 0 END) AS ocr_not_performed

            FROM ocr_data
        WHERE project_id = %s;
        """
        cur.execute(query, (project_id,))
        rows = cur.fetchall()

        # Close the cursor and connection
        cur.close()
        conn.close()

        # print("Retrieves OCR data successfully")
        # print(ocr_performed)
        # print(ocr_not_performed)

        # Prepare the result in JSON format
        result = [] # store results in a list
        for row in rows:
            file_id, ocr_performed, ocr_not_performed = row
            result.append({
                "file_id": file_id,
                "perform_ocr_for_times": ocr_performed,
                "not_perform_ocr_for_times": ocr_not_performed
            })

        return result

    except Exception as e:
        print(f"Error fetching OCR data: {e}")
        return None






# function checks if the given file IDs are available in the extracted_data table for a specified project_id.
# Returns two lists: available and unavailable file IDs.

def extracted_data(project_id, file_ids):
    """Check if file IDs are available in the extracted_data table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Query to check if file IDs are available in the extracted_data table
        query = """
        SELECT file_id FROM extracted_data WHERE project_id = %s AND file_id = ANY(%s);
        """
        cur.execute(query, (project_id, file_ids))
        available_file_ids = [row[0] for row in cur.fetchall()] # fetch all rows and store in a list of available file IDs 

        # Find file IDs that are not available in the extracted_data table
        unavailable_file_ids = list(set(file_ids) - set(available_file_ids))

        # Close the cursor and connection
        cur.close()
        conn.close()

        # print("Extracted data checked successfully")
        # print(available_file_ids)
        # print(unavailable_file_ids)

        return available_file_ids, unavailable_file_ids

    except Exception as e:
        print(f"Error checking extracted data: {e}")
        return None, None



# function checks the completeness of runsheet data for given file IDs in a specified project.
# categorizing them into complete, incomplete, and missing files, and returns the results.
def runsheet(project_id, file_ids):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        complete_file_ids = [] # store complete file IDs in a list
        incomplete_file_ids = []
        incomplete_file_columns = {} # store incomplete file columns in a dictionary
        missing_file_ids = []

        for file_id in file_ids:
            query = """
            SELECT id, file_id, project_id, user_id, document_case, instrument_type, volume_page, 
                effective_date, execution_date, file_date, grantor, grantee, property_description, 
                remarks, created_at, updated_at, sort_sequence
            FROM public.runsheets
            WHERE file_id = %s;
            """
            
            cur.execute(query, (file_id,))
            rows = cur.fetchall()

            if not rows:
                missing_file_ids.append(file_id)
                continue
            
            # This Code retrieves the column names from the cursor description object.
            # Initializes a flag to track if any null values are found, and 
            # Creates a set to store the names of columns that contain null values.
            columns = [desc[0] for desc in cur.description] # get column names from the cursor description object 
            has_null = False
            null_columns = set()


            # This code iterates over each row in rows, and for each row, it enumerates through the values. 
            # If any value is None, it sets the has_null flag to True and 
            # Adds the corresponding column name (from columns[col_index]) to the null_columns set.
            for row in rows:
                for col_index, value in enumerate(row): # enumerate() function returns an index and value
                    if value is None:
                        has_null = True
                        null_columns.add(columns[col_index])


            # This code checks if the has_null flag is True. If it is, it appends the file_id to the incomplete_file_ids list and 
            # stores the list of columns with null values in the incomplete_file_columns dictionary with the file_id as the key. 
            # If has_null is False, it appends the file_id to the complete_file_ids list.
            if has_null:
                incomplete_file_ids.append(file_id)
                incomplete_file_columns[file_id] = list(null_columns)
            else:
                complete_file_ids.append(file_id)
        
        cur.close()
        conn.close()


        # This code initializes an empty dictionary result and then checks if the lists complete_file_ids, incomplete_file_ids, or missing_file_ids are not empty. 
        # If they are not empty, it adds them to the result dictionary with appropriate keys. For incomplete_file_ids, 
        # It also adds the corresponding columns with null values from the incomplete_file_columns dictionary.
        result = {}
        if complete_file_ids:
            result["Runsheet complete_file_ids"] = complete_file_ids
        if incomplete_file_ids:
            result["Runsheet incomplete_file_ids"] = incomplete_file_ids
            result["Runsheet incomplete_file_ids columns are"] = incomplete_file_columns
        if missing_file_ids:
            result["Runsheet missing_file_ids"] = missing_file_ids
        
        return result
    
    except Exception as e:
        print(f"Error checking runsheet data: {e}")
        return {"error": "Unable to fetch runsheet data"}





@app.route("/api/v1/project_info/<int:project_id>", methods=["GET"])
def project_info(project_id):

    # step 1 : get the total number of files for a given project_id
    total_files, completed_files, not_completed_files, file_ids = get_total_files(project_id)
    if total_files is None or completed_files is None or not_completed_files is None:
        return jsonify({"error": "Unable to fetch file counts"}), 500

    # step 2 : get OCR data information for a given project_id
    ocr_data_result = ocr_data(project_id)
    if ocr_data_result is None:
        return jsonify({"error": "Unable to fetch OCR data"}), 500

    # step 3 : check if file IDs are available in the extracted_data table
    available_file_ids, unavailable_file_ids = extracted_data(project_id, file_ids)
    if available_file_ids is None or unavailable_file_ids is None:
        return jsonify({"error": "Unable to check extracted data"}), 500

    # step 4 : check runsheet completeness for each file ID
    runsheet_result = runsheet(project_id, file_ids)
    if "error" in runsheet_result:
        return jsonify(runsheet_result), 500

    # Combine all results into a single JSON response
    response = {
        "project_id": project_id,
        "total_files": total_files,
        "No. of ocr completed_files": completed_files,
        "No. of ocr not_completed_files": not_completed_files,
        "ocr_data": ocr_data_result,
        "file_ids": file_ids,
        "Extraction performed on": available_file_ids,
        "Extraction not performed on": unavailable_file_ids,
        "Runsheet": runsheet_result
    }

    return jsonify(response)



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)