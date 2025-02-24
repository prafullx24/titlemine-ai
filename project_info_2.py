"""


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


# # Folder to store downloaded and OCR files
# DOWNLOAD_FOLDER = "download_file"
# os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)







# "Get the total number of files for a given project_id from the Files table.
# count the number of files with OCR status 'completed'.
# count the number of files with OCR status not 'completed'



# def get_total_files(project_id):
#     """Get the total number of files for a given project_id from the Files table."""
#     try:
#         conn = psycopg2.connect(**DB_CONFIG) 
#         cur = conn.cursor() # cursor object to execute queries

#         # Execute the query to count the total number of files
#         query_total_files = """
#         SELECT COUNT(*) FROM Files WHERE project_id = %s;
#         """
#         cur.execute(query_total_files, (project_id,))
#         total_files = cur.fetchone()[0]

#         # Execute the query to count the number of files with OCR status 'completed'
#         query_completed_files = """
#         SELECT COUNT(*) FROM Files WHERE project_id = %s AND ocr_status = 'completed';
#         """
#         cur.execute(query_completed_files, (project_id,))
#         completed_files = cur.fetchone()[0]

#         # Execute the query to count the number of files with OCR status not 'completed'
#         query_not_completed_files = """
#         SELECT COUNT(*) FROM Files WHERE project_id = %s AND ocr_status != 'completed';
#         """
#         cur.execute(query_not_completed_files, (project_id,))
#         not_completed_files = cur.fetchone()[0]

#         # Close the cursor and connection
#         cur.close()
#         conn.close()

#         return total_files, completed_files, not_completed_files

#     except Exception as e:
#         print(f"Error fetching total files: {e}")
#         return None







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
        print("File IDs:", file_ids)

        return total_files, completed_files, not_completed_files, file_ids
    
    except Exception as e:
        print(f"Error fetching total files: {e}")
        return None,None,None,None






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

        # Prepare the result in JSON format
        result = []
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









# def extracted_data(project_id, file_ids):
#     """Check if file IDs are available in the extracted_data table."""
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()

#         # Query to check if file IDs are available in the extracted_data table
#         query = """
#         SELECT file_id FROM extracted_data WHERE project_id = %s AND file_id = ANY(%s);
#         """
#         cur.execute(query, (project_id, file_ids))
#         available_file_ids = [row[0] for row in cur.fetchall()]

#         # Find file IDs that are not available in the extracted_data table
#         unavailable_file_ids = set(file_ids) - set(available_file_ids)

#         # Close the cursor and connection
#         cur.close()
#         conn.close()


#         return available_file_ids, unavailable_file_ids

#     except Exception as e:
#         print(f"Error checking extracted data: {e}")
#         return None, None













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
        available_file_ids = [row[0] for row in cur.fetchall()]

        # Find file IDs that are not available in the extracted_data table
        unavailable_file_ids = list(set(file_ids) - set(available_file_ids))

        # Close the cursor and connection
        cur.close()
        conn.close()

        return available_file_ids, unavailable_file_ids

    except Exception as e:
        print(f"Error checking extracted data: {e}")
        return None, None

















# @app.route("/api/v1/project_info/<int:project_id>", methods=["GET"])
# def project_info(project_id):

    # # step 1 : get the total number of files for a given project_id
    # total_files, completed_files, not_completed_files,file_ids = get_total_files(project_id)
    # if total_files is not None and completed_files is not None and not_completed_files is not None:
    #     return jsonify({
    #         "project_id": project_id,
    #         "total_files": total_files,
    #         "completed_files": completed_files,
    #         "not_completed_files": not_completed_files,
    #         "file_ids": file_ids
    #     })
    # else:
    #     return jsonify({"error": "Unable to fetch file counts"}), 500

    # # step 2 : get OCR data for a given project_id
    # ocr_data_result = ocr_data(project_id)
    # if ocr_data_result is not None:
    #     return jsonify(ocr_data_result)
    # else:
    #     return jsonify({"error": "Unable to fetch OCR data"}), 500




    # # step 1 : get the total number of files for a given project_id
    # total_files, completed_files, not_completed_files, file_ids = get_total_files(project_id)
    # if total_files is None or completed_files is None or not_completed_files is None:
    #     return jsonify({"error": "Unable to fetch file counts"}), 500

    # # step 2 : get OCR data for a given project_id
    # ocr_data_result = ocr_data(project_id)
    # if ocr_data_result is None:
    #     return jsonify({"error": "Unable to fetch OCR data"}), 500

    # # # Combine both results into a single JSON response
    # # response = {
    # #     "project_id": project_id,
    # #     "total_files": total_files,
    # #     "No. of ocr completed_files": completed_files,
    # #     "No. of ocr not_completed_files": not_completed_files,
    # #     "ocr_data": ocr_data_result,
    # #     "file_ids": file_ids,
    # # }

    # # return jsonify(response)



    # # step 3 : check if file IDs are available in the extracted_data table
    # available_file_ids, unavailable_file_ids = extracted_data(project_id, file_ids)
    # if available_file_ids is None or unavailable_file_ids is None:
    #     return jsonify({"error": "Unable to check extracted data"}), 500

    # # Combine all results into a single JSON response
    # response = {
    #     "project_id": project_id,
    #     "total_files": total_files,
    #     "No. of ocr completed_files": completed_files,
    #     "No. of ocr not_completed_files": not_completed_files,
    #     "ocr_data": ocr_data_result,
    #     "file_ids": file_ids,
    #     "Extraction performed on": available_file_ids,
    #     "Extraction not performed on": unavailable_file_ids,
    # }

    # return jsonify(response)







# def runsheet(project_id):
#     """Check runsheet completeness for each file ID."""
#     # Step 1: Get the total number of files and file IDs for the given project_id
#     file_ids = get_total_files(project_id)
#     if file_ids is None:
#         print("Unable to fetch file counts")
#         return

#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()

#         # Step 2: Check for empty or null values in the runsheets table for each file ID
#         complete_file_ids = []
#         incomplete_file_ids = {}

#         for file_id in file_ids:
#             query = """
#             SELECT 
#                 id, file_id, project_id, user_id, document_case, instrument_type, volume_page, 
#                 effective_date, execution_date, file_date, grantor, grantee, property_description, 
#                 remarks, created_at, updated_at, sort_sequence
#             FROM public.runsheets
#             WHERE file_id = %s;
#             """
#             cur.execute(query, (file_id,))
#             row = cur.fetchone()

#             if row:
#                 columns = ["id", "file_id", "project_id", "user_id", "document_case", "instrument_type", 
#                            "volume_page", "effective_date", "execution_date", "file_date", "grantor", 
#                            "grantee", "property_description", "remarks", "created_at", "updated_at", 
#                            "sort_sequence"]
#                 empty_columns = [columns[i] for i, value in enumerate(row) if value is None or value == '']

#                 if not empty_columns:
#                     complete_file_ids.append(file_id)
#                 else:
#                     incomplete_file_ids[file_id] = empty_columns

#         # Close the cursor and connection
#         cur.close()
#         conn.close()

#         # Step 3: Print results
#         print("Runsheet complete for file IDs:", complete_file_ids)
#         for file_id, empty_columns in incomplete_file_ids.items():
#             print(f"File ID {file_id} has empty or null values in columns: {empty_columns}")

#     except Exception as e:
#         print(f"Error checking runsheet data: {e}")







# def runsheet(project_id):
#     """Check runsheet completeness for each file ID."""
#     # Step 1: Get the total number of files and file IDs for the given project_id
#     _, _, _, file_ids = get_total_files(project_id)
#     if file_ids is None:
#         return {"error": "Unable to fetch file counts"}

#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()

#         # Step 2: Check for empty or null values in the runsheets table for each file ID
#         complete_file_ids = []
#         incomplete_file_ids = {}

#         for file_id in file_ids:
#             query = """
#             SELECT 
#                 id, file_id, project_id, user_id, document_case, instrument_type, volume_page, 
#                 effective_date, execution_date, file_date, grantor, grantee, property_description, 
#                 remarks, created_at, updated_at, sort_sequence
#             FROM public.runsheets
#             WHERE file_id = %s;
#             """
#             cur.execute(query, (file_id,))
#             row = cur.fetchone()

#             if row:
#                 columns = ["id", "file_id", "project_id", "user_id", "document_case", "instrument_type", 
#                            "volume_page", "effective_date", "execution_date", "file_date", "grantor", 
#                            "grantee", "property_description", "remarks", "created_at", "updated_at", 
#                            "sort_sequence"]
#                 empty_columns = [columns[i] for i, value in enumerate(row) if value is None or value == ''] # check for empty or null values

#                 if not empty_columns:
#                     complete_file_ids.append(file_id)
#                 else:
#                     incomplete_file_ids[file_id] = empty_columns

#         # Close the cursor and connection
#         cur.close()
#         conn.close()

#         # Step 3: Return results as JSON
#         return {
#             "Runsheet complete_file_ids": complete_file_ids,
#             "Runsheet incomplete_file_ids": incomplete_file_ids
#         }

#     except Exception as e:
#         return {"error": f"Error checking runsheet data: {e}"}



# def runsheet(project_id):
#     """Check runsheet completeness for each file ID."""
#     # Step 1: Get the total number of files and file IDs for the given project_id
#     _, _, _, file_ids = get_total_files(project_id)
#     if file_ids is None:
#         return {"error": "Unable to fetch file counts"}
    
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
        
#         complete_file_ids = []
#         incomplete_file_ids = {}
#         complete_count = 0
#         incomplete_count = 0
        
#         for file_id in file_ids:
#             query = """
#             SELECT 
#                 id, file_id, project_id, user_id, document_case, instrument_type, volume_page, 
#                 effective_date, execution_date, file_date, grantor, grantee, property_description, 
#                 remarks, created_at, updated_at, sort_sequence
#             FROM public.runsheets
#             WHERE file_id = %s;
#             """
#             cur.execute(query, (file_id,))
#             row = cur.fetchone()
            
#             if row:
#                 columns = ["id", "file_id", "project_id", "user_id", "document_case", "instrument_type", 
#                           "volume_page", "effective_date", "execution_date", "file_date", "grantor", 
#                           "grantee", "property_description", "remarks", "created_at", "updated_at", 
#                           "sort_sequence"]
#                 empty_columns = [columns[i] for i, value in enumerate(row) if value is None or value == '']
                
#                 if not empty_columns:
#                     complete_file_ids.append(file_id)
#                     complete_count += 1
#                 else:
#                     incomplete_file_ids[file_id] = empty_columns
#                     incomplete_count += 1
        
#         # Close the cursor and connection
#         cur.close()
#         conn.close()
        
#         # Return detailed results
#         return {
#             "summary": {
#                 "total_runsheets": len(file_ids),
#                 "complete_runsheets": complete_count,
#                 "incomplete_runsheets": incomplete_count
#             },
#             "details": {
#                 "complete_file_ids": complete_file_ids,
#                 "incomplete_file_ids": incomplete_file_ids
#             }
#         }
        
#     except Exception as e:
#         return {"error": f"Error checking runsheet data: {e}"}






# @app.route("/api/v1/project_info/<int:project_id>", methods=["GET"])
# def project_info(project_id):
#     # step 1 : get the total number of files for a given project_id
#     total_files, completed_files, not_completed_files, file_ids = get_total_files(project_id)
#     if total_files is None or completed_files is None or not_completed_files is None:
#         return jsonify({"error": "Unable to fetch file counts"}), 500

#     # step 2 : get OCR data for a given project_id
#     ocr_data_result = ocr_data(project_id)
#     if ocr_data_result is None:
#         return jsonify({"error": "Unable to fetch OCR data"}), 500

#     # step 3 : check if file IDs are available in the extracted_data table
#     available_file_ids, unavailable_file_ids = extracted_data(project_id, file_ids)
#     if available_file_ids is None or unavailable_file_ids is None:
#         return jsonify({"error": "Unable to check extracted data"}), 500

#     # Combine all results into a single JSON response
#     response = {
#         "project_id": project_id,
#         "total_files": total_files,
#         "No. of ocr completed_files": completed_files,
#         "No. of ocr not_completed_files": not_completed_files,
#         "ocr_data": ocr_data_result,
#         "file_ids": file_ids,
#         "Extraction performed on": available_file_ids,
#         "Extraction not performed on": unavailable_file_ids,
        
#     }

#     return jsonify(response)
















def runsheet(project_id):
    """Check runsheet completeness for each file ID."""
    # Step 1: Get the total number of files and file IDs for the given project_id
    _, _, _, file_ids = get_total_files(project_id)
    if file_ids is None:
        return {"error": "Unable to fetch file counts"}

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Step 2: Check for empty or null values in the runsheets table for each file ID
        complete_file_ids = []
        incomplete_file_ids = {}
        missing_file_ids = []

        for file_id in file_ids:
            query = """
            SELECT 
                id, file_id, project_id, user_id, document_case, instrument_type, volume_page, 
                effective_date, execution_date, file_date, grantor, grantee, property_description, 
                remarks, created_at, updated_at, sort_sequence
            FROM public.runsheets
            WHERE file_id = %s;
            """
            cur.execute(query, (file_id,))
            row = cur.fetchone()

            if row:
                columns = ["id", "file_id", "project_id", "user_id", "document_case", "instrument_type", 
                           "volume_page", "effective_date", "execution_date", "file_date", "grantor", 
                           "grantee", "property_description", "remarks", "created_at", "updated_at", 
                           "sort_sequence"]
                empty_columns = [columns[i] for i, value in enumerate(row) if value is None or value == ''] # check for empty or null values

                if not empty_columns:
                    complete_file_ids.append(file_id)
                else:
                    incomplete_file_ids[file_id] = empty_columns
            else:
                missing_file_ids.append(file_id)

        # Close the cursor and connection
        cur.close()
        conn.close()

        # Step 3: Return results as JSON
        return {
            "Runsheet complete_file_ids": complete_file_ids,
            "Runsheet incomplete_file_ids": incomplete_file_ids,
            "Runsheet missing_file_ids": missing_file_ids
        }

    except Exception as e:
        return {"error": f"Error checking runsheet data: {e}"}











@app.route("/api/v1/project_info/<int:project_id>", methods=["GET"])
def project_info(project_id):
    # step 1 : get the total number of files for a given project_id
    total_files, completed_files, not_completed_files, file_ids = get_total_files(project_id)
    if total_files is None or completed_files is None or not_completed_files is None:
        return jsonify({"error": "Unable to fetch file counts"}), 500

    # step 2 : get OCR data for a given project_id
    ocr_data_result = ocr_data(project_id)
    if ocr_data_result is None:
        return jsonify({"error": "Unable to fetch OCR data"}), 500

    # step 3 : check if file IDs are available in the extracted_data table
    available_file_ids, unavailable_file_ids = extracted_data(project_id, file_ids)
    if available_file_ids is None or unavailable_file_ids is None:
        return jsonify({"error": "Unable to check extracted data"}), 500

    # step 4 : check runsheet completeness for each file ID
    runsheet_result = runsheet(project_id)
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
        # "runsheet": {
        #     "complete_file_ids": runsheet_result.get("complete_file_ids", []),
        #     "incomplete_file_ids": runsheet_result.get("incomplete_file_ids", {})
        # }
        "runsheet": runsheet_result
    }

    return jsonify(response)







# @app.route("/api/v1/runsheet/<int:project_id>", methods=["GET"])
# def runsheet_api(project_id):
#     """API endpoint to check runsheet completeness for each file ID."""
#     result = runsheet(project_id)
#     if "error" in result:
#         return jsonify(result), 500
#     return jsonify(result)







if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
