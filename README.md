# titlemine-ai
This is the project guideline for setting up TitleMine Data Extractor Code. 
The process of Data Extraction is split into three layers of isolation. 

# TODO
1. Combine the DB Config: Currently two config types are present
2. Serial execution of Data extraction with OCR process runs in background parallely

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