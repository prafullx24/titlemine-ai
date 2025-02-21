# list out all file_id according to that project id and create  json file which contail 
# all files_id for that project. import os


import os
import json
import psycopg2
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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

def get_all_file_ids(project_id):
    """Fetches all file IDs for the given project ID from the database."""
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Query to fetch file IDs for the given project ID
        query = """
        SELECT file_id FROM files WHERE project_id = %s;
        """
        cursor.execute(query, (project_id,))
        file_ids = [row[0] for row in cursor.fetchall()]
        
        # Close database connection
        cursor.close()
        conn.close()

        # Save the file IDs to a JSON file
        file_path = f"download_file/project_{project_id}_files.json"
        with open(file_path, "w") as json_file:
            json.dump({"project_id": project_id, "file_ids": file_ids}, json_file, indent=4)

        return file_path, file_ids
    
    except Exception as e:
        return str(e), []

@app.route("/get_file_ids/<int:project_id>", methods=["GET"])
def get_file_ids_api(project_id):
    """API endpoint to get file IDs for a given project ID."""
    file_path, file_ids = get_all_file_ids(project_id)
    
    if isinstance(file_ids, list):
        return jsonify({
            "message": "File IDs retrieved successfully.",
            "file_path": file_path,
            "file_ids": file_ids
        })
    else:
        return jsonify({"error": file_path}), 500

if __name__ == "__main__":
    app.run(debug=True)