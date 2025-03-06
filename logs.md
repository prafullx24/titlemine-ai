Logs for 
1) @app.route("/api/v1/combine_ocr/<int:project_id>", methods=["POST"]) -> for project_id=27 on local DB.



2025-03-06 10:33:08,875 - INFO - Files fetched for ocr : [(587, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24d3703.pdf', 'Processing'), (588, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d244f142.pdf', 'Processing'), (589, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d2489052.pdf', 'Processing'), (590, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24943d6.pdf', 'Processing'), (592, 1, 27, 'sample.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24b088c.pdf', 'Processing')]
2025-03-06 10:33:09,082 - INFO - File: (587, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24d3703.pdf', 'Processing')
2025-03-06 10:33:09,083 - INFO - File: (588, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d244f142.pdf', 'Processing')
2025-03-06 10:33:09,083 - INFO - File: (589, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d2489052.pdf', 'Processing')
2025-03-06 10:33:09,084 - INFO - File: (590, 1, 27, 'upload_to_check.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24943d6.pdf', 'Processing')
2025-03-06 10:33:09,084 - INFO - File: (592, 1, 27, 'sample.pdf', 'https://titlemine-app.s3.amazonaws.com/runsheet_documents/2025-02-28_09%3A08%3A52_67c17d24b088c.pdf', 'Processing')
2025-03-06 10:33:16,959 - INFO - Downloaded file from S3: file_id: 592; project_id 27
2025-03-06 10:33:19,920 - INFO - Downloaded file from S3: file_id: 588; project_id 27
2025-03-06 10:33:20,110 - INFO - Downloaded file from S3: file_id: 587; project_id 27
2025-03-06 10:33:21,974 - INFO - Downloaded file from S3: file_id: 589; project_id 27
2025-03-06 10:33:22,039 - INFO - Downloaded file from S3: file_id: 590; project_id 27
2025-03-06 10:33:22,041 - INFO - Downloaded files successfully for Document AI OCR.
2025-03-06 10:33:22,042 - INFO - AWS Text extraction is in progress
2025-03-06 10:33:48,335 - INFO - Text extraction completed successfully. Results saved to output_file\2025-02-28_09_08_52_67c17d244f142.json
2025-03-06 10:33:51,607 - INFO - Text extraction completed successfully. Results saved to output_file\2025-02-28_09_08_52_67c17d24b088c.json
2025-03-06 10:34:01,339 - INFO - Text extraction completed successfully. Results saved to output_file\2025-02-28_09_08_52_67c17d2489052.json
2025-03-06 10:34:01,588 - INFO - Text extraction completed successfully. Results saved to output_file\2025-02-28_09_08_52_67c17d24d3703.json
2025-03-06 10:34:02,337 - INFO - Text extraction completed successfully. Results saved to output_file\2025-02-28_09_08_52_67c17d24943d6.json
2025-03-06 10:34:02,346 - INFO - Processing file: download_file\download_pdf_1_27_592.pdf
2025-03-06 10:34:02,347 - INFO - Processing file: download_file\download_pdf_1_27_588.pdf
2025-03-06 10:34:02,347 - INFO - Processing file: download_file\download_pdf_1_27_587.pdf
2025-03-06 10:34:02,348 - INFO - Processing file: download_file\download_pdf_1_27_589.pdf
2025-03-06 10:34:02,349 - INFO - Processing file: download_file\download_pdf_1_27_590.pdf
2025-03-06 10:34:02,422 - INFO - Processing document: download_file\download_pdf_1_27_589.pdf, Size: 1467141 bytes
2025-03-06 10:34:02,424 - INFO - Processing document: download_file\download_pdf_1_27_588.pdf, Size: 1025520 bytes
2025-03-06 10:34:02,431 - INFO - Processing document: download_file\download_pdf_1_27_590.pdf, Size: 1598586 bytes
2025-03-06 10:34:02,433 - INFO - Processing document: download_file\download_pdf_1_27_592.pdf, Size: 1116481 bytes
2025-03-06 10:34:02,439 - INFO - Processing document: download_file\download_pdf_1_27_587.pdf, Size: 2004914 bytes
2025-03-06 10:34:11,888 - INFO - Document processed: download_file\download_pdf_1_27_592.pdf, Size: 1116481 bytes
2025-03-06 10:34:12,339 - INFO - Document processed: download_file\download_pdf_1_27_588.pdf, Size: 1025520 bytes
2025-03-06 10:34:12,425 - INFO - Document processed: download_file\download_pdf_1_27_587.pdf, Size: 2004914 bytes
2025-03-06 10:34:12,833 - INFO - Document processed: download_file\download_pdf_1_27_589.pdf, Size: 1467141 bytes
2025-03-06 10:34:13,125 - INFO - Document processed: download_file\download_pdf_1_27_590.pdf, Size: 1598586 bytes
2025-03-06 10:34:13,362 - INFO - OCR JSON saved successfully: download_file\download_json_1_27_592.json
2025-03-06 10:34:13,366 - INFO - OCR JSON saved successfully: download_file\download_json_1_27_588.json
2025-03-06 10:34:13,368 - INFO - OCR JSON saved successfully: download_file\download_json_1_27_587.json
2025-03-06 10:34:13,370 - INFO - OCR JSON saved successfully: download_file\download_json_1_27_589.json
2025-03-06 10:34:13,372 - INFO - OCR JSON saved successfully: download_file\download_json_1_27_590.json
2025-03-06 10:34:13,373 - INFO - Done with Extracts text and confidence scores
2025-03-06 10:34:13,374 - INFO - Extracted text with confidence successfully for Document AI OCR and AWS Textract.
2025-03-06 10:34:13,514 - INFO - Open AI and AWS Textract OCR data saved for project_id: 27
2025-03-06 10:34:13,544 - INFO - OCR status updated to 'Extracting' for project_id: 27
2025-03-06 10:34:13,560 - INFO - Execute Document AI and AWS Textract successfully.
2025-03-06 10:34:13,561 - INFO - Starting Extraction: 27
2025-03-06 10:34:13,711 - INFO - Processing file ID: 587
2025-03-06 10:34:16,492 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:34:16,503 - INFO - Total Token used for instrument_type: 6768
2025-03-06 10:34:29,114 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:34:29,117 - INFO - Total Token used for data extraction: 2936
2025-03-06 10:34:29,410 - INFO - Successfully stored/updated data in Extracted Data Table for file_id: 587
2025-03-06 10:34:31,307 - INFO - [store_runsheet_data] Inserted new row with ID: 99
2025-03-06 10:34:31,652 - INFO - [store_runsheet_data] Successfully inserted runsheet data for file_id: 587
2025-03-06 10:34:31,653 - INFO - Completed processing file ID 587
2025-03-06 10:34:31,654 - INFO - Processing file ID: 588
2025-03-06 10:34:34,917 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:34:34,918 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:35:07,356 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:35:07,358 - INFO - Total Token used for instrument_type: 3998
2025-03-06 10:35:08,641 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:35:08,643 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:35:52,730 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:35:52,732 - INFO - Total Token used for data extraction: 2947
2025-03-06 10:35:52,995 - INFO - Successfully stored/updated data in Extracted Data Table for file_id: 588
2025-03-06 10:35:53,295 - WARNING - [store_runsheet_data] Sequence out of sync (seq=99, max_id=99). Resetting sequence.
2025-03-06 10:35:53,300 - INFO - [store_runsheet_data] Sequence reset to 100
2025-03-06 10:35:53,312 - INFO - [store_runsheet_data] Inserted new row with ID: 100
2025-03-06 10:35:53,493 - INFO - [store_runsheet_data] Successfully inserted runsheet data for file_id: 588
2025-03-06 10:35:53,494 - INFO - Completed processing file ID 588
2025-03-06 10:35:53,495 - INFO - Processing file ID: 589
2025-03-06 10:35:58,197 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:35:58,198 - INFO - Total Token used for instrument_type: 6007
2025-03-06 10:35:59,222 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:35:59,224 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:36:44,688 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:36:44,691 - INFO - Total Token used for data extraction: 2925
2025-03-06 10:36:44,947 - INFO - Successfully stored/updated data in Extracted Data Table for file_id: 589
2025-03-06 10:36:45,411 - WARNING - [store_runsheet_data] Sequence out of sync (seq=100, max_id=100). Resetting sequence.
2025-03-06 10:36:45,415 - INFO - [store_runsheet_data] Sequence reset to 101
2025-03-06 10:36:45,471 - INFO - [store_runsheet_data] Inserted new row with ID: 101
2025-03-06 10:36:45,484 - INFO - [store_runsheet_data] Successfully inserted runsheet data for file_id: 589
2025-03-06 10:36:45,485 - INFO - Completed processing file ID 589
2025-03-06 10:36:45,486 - INFO - Processing file ID: 590
2025-03-06 10:36:49,363 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:36:49,365 - INFO - Total Token used for instrument_type: 6245
2025-03-06 10:36:50,495 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:36:50,497 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:37:31,582 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:37:31,585 - INFO - Total Token used for data extraction: 3008
2025-03-06 10:37:31,811 - INFO - Successfully stored/updated data in Extracted Data Table for file_id: 590
2025-03-06 10:37:33,103 - WARNING - [store_runsheet_data] Sequence out of sync (seq=101, max_id=101). Resetting sequence.
2025-03-06 10:37:33,108 - INFO - [store_runsheet_data] Sequence reset to 102
2025-03-06 10:37:33,119 - INFO - [store_runsheet_data] Inserted new row with ID: 102
2025-03-06 10:37:33,328 - INFO - [store_runsheet_data] Successfully inserted runsheet data for file_id: 590
2025-03-06 10:37:33,330 - INFO - Completed processing file ID 590
2025-03-06 10:37:33,331 - INFO - Processing file ID: 592
2025-03-06 10:37:35,275 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:37:35,277 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:38:07,987 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:38:07,989 - INFO - Total Token used for instrument_type: 4124
2025-03-06 10:38:08,685 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 429 Too Many Requests"
2025-03-06 10:38:08,686 - INFO - Retrying request to /chat/completions in 30.000000 seconds
2025-03-06 10:38:48,848 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-06 10:38:48,851 - INFO - Total Token used for data extraction: 2962
2025-03-06 10:38:49,330 - INFO - Successfully stored/updated data in Extracted Data Table for file_id: 592
2025-03-06 10:38:49,758 - WARNING - [store_runsheet_data] Sequence out of sync (seq=102, max_id=102). Resetting sequence.
2025-03-06 10:38:49,979 - INFO - [store_runsheet_data] Sequence reset to 103
2025-03-06 10:38:49,990 - INFO - [store_runsheet_data] Inserted new row with ID: 103
2025-03-06 10:38:50,197 - INFO - [store_runsheet_data] Successfully inserted runsheet data for file_id: 592
2025-03-06 10:38:50,198 - INFO - Completed processing file ID 592
2025-03-06 10:38:50,202 - INFO - 127.0.0.1 - - [06/Mar/2025 10:38:50] "POST /api/v1/combine_ocr/27 HTTP/1.1" 200 -
