# titlemine-ai
This is the project guideline for setting up TitleMine Data Extractor Code. 
The process of Data Extraction is split into three layers of isolation. 

## Installation

To install the required dependencies, run the following command:

```
pip install -r requirements.txt
```

## set GOOGLE_APPLICATION_CREDENTIALS
set GOOGLE_APPLICATION_CREDENTIALS="<Location of the json>"

## Usage

1. Ensure that you have access to the PostgreSQL database and the necessary credentials.
2. Update the database configuration in `file_ocr.py` with your database details.
3. Run the application using:

```
python file_ocr.py
```

4. Access the API endpoint to process OCR by navigating to:

```
http://localhost:5000/api/v1/file_ocr/<file_id>
```

Replace `<file_id>` with the ID of the file you want to process.


5. check = " titlemine-documentai-ocr-898de9277942.json " file available in folder which contain credentials.



# Batch Processing

Project: project_id
Files: file_ids = [...]

dbtrigger: files table: OCR delay
Frontend: call 
            files: 100
            call: api/v1/process_files/:project_id
            Project: 200 files 
                     100 Files processed: OCR -> Extraction -> Runsheet 
                     New 100 array 
                     instrument_type: 100 
                     execution_date ... grantors {10} : 1000
                     Total: 1100

