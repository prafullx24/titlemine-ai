"""
This file is expected to contain all the code related to File OCR handling. 
As per development plan we are expecting to add three OCR Providers to increase our coverage of document.
During the first milestone phase we are specifically working with Google's Document AI.

The code in this file is expected to provide following:
1. A Flask endpoint that takes file_id as input.
2. This file is downloaded and split if page count is more than 15. 
3. These 15 page files are sent to OCR and OCR data is recieved from Document AI.
4. If a file is split the OCR data is merged and stored in the database table OCR_data. column ocr_text_1 and ocr_json_1
5. Along with the OCR Text data, we also extract the json with OCR confidence from Document AI. this is stored in ocr_json_1 column.
6. When OCR is complete, call internal endpoint "extract_data" to start data extraction process.

Limitations:
1. Currently working with only one OCR
2. Document AI has file size limit of 20 MB for single file processing.

To-Do:
1. Add Second OCR Provider: Amazon Textract or Anthropic
2. Switch to Batch Processing Mode on Document AI to disable the limits.

API Endpoint:
https://host:port/api/v1/ocr_file/:file_id

response:
Processing:
{
  "status": "processing",
  "file_id": "123456"
}

Completed:
{
  "status": "completed",
  "file_id": "123456",
  "ocr_data_id" : "123123"
}

Failed:
{
  "status": "failed",
  "file_id": "123456"
}

Libraries:
Flask
psycopg2
dotenv
requests
google.cloud 
json
os
"""
