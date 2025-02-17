"""
This file is expected to contain a Flask endpoint which takes ocr_data_id as input parameter to extract specific data from OCR data of a file.
We will use OpenAI LLM for this task.
We are expected to combine three OCR texts into one to create a "Consensus Document". This will be added after we add second OCR provider.

Architecture:

Our Data extraction process is divided in five layers:

"Runsheet -> extracted_data -> OCR_data -> Files -> Files in S3"

Here "Runsheets" and "Files in S3" are accesible to the user, everything else is abstracted away.

Files is the files table we use to maintain references to the original uploaded files
OCR_data is the raw text and confidence score of each OCR Block of a file. 
extracted_data is the LLM response for the runsheet prompts. This can contain the multiline response by the LLM.
Runsheet holds the specific information which will be useful for the Title Attorney


The code in this file is expected to provide following:
1. Flask Endpoint that is called from file_ocr.py file, which takes file_id and ocr_data_id as input.
2. Ignore Consensus Document as we only have one OCR in first milestone.
3. Use ChatGPT to prompt and extract data from files which have completed OCR
    - Use pyscopg2 to connect with postgresql database to query OCR data for a specific file 
    - Use OpenAI extract data from this OCR text
    - Use json files in ocr_data table to get confidence scores
4. Store the output from ChatGPT in json format in the extracted_data table.
        {
        "gpt_response": "<Multiline Response from ChatGPT>",
        "specific_data": "<Exact Value>",
        "confidence_score": "99.00"
        }
5. Get specific data from this table and store it in respective runsheet column.

Limitations:
1. Currently, this code is not expected to handle multiple OCR data. 

"""