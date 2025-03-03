FOR FILE NAME DR S/150 FILE ID -63 PROJECT ID - 18 ex-1

PROMPTS USED

"Deed": {
"system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format. For each extracted field, also include a confidence score (0-10) based on certainty, and the exact text from which the field was derived.",
"fields": {
"instrument_type": {
"value": "Identify the type of legal document (e.g., Deed, Affidavit, Mortgage, etc.).",
"score": "How confident are you about this instrument type? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the instrument type.",
"summary" : "what relevant information do you have in this file about instrument type? summarize in less than 50 words."
},
"volume_page": {
"value": "Reference the recording information found in the document and/or the file name to determine the volume number and page number of where the document was filed with the county. Return as 'Volume Number/Page Number'.",
"score": "How confident are you about this volume page? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the volume and page number.",
"summary" : "what relevant information do you have in this file about volume page? summarize in less than 50 words."
},
"document_case_number": {
"value": "Reference information found in the margins, top of the document, or in the file name to determine the case, file, or recording number. Return as '#Document Number'. Examples: 'Case No. 12345', 'File: ABC-2023-001'. If none is found, return 'none found'.",
"score": "How confident are you about this document case number? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the document case number.",
"summary" : "what relevant information do you have in this file about document case number? summarize in less than 50 words."
},
"execution_date": {
"value": "What is the latest date on which a grantor or grantee signed the document? Return the date in 'Month Day, Year' format. Example: 'January 1, 2023'. If no date is found, return 'N/A'.",
"score": "How confident are you about this execution date ? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the execution date.",
"summary" : "what relevant information do you have in this file about execution date? summarize in less than 50 words."
},
"effective_date": {
"value": "What is the date of the transfer of ownership? Return the date in 'Month Day, Year' format. Example: 'January 1, 2023'. If no date is found, return 'N/A'.",
"score":"How confident are you about this effective date? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the effective date.",
"summary" : "what relevant information do you have in this file about effective date? summarize in less than 50 words."
},
"recording_date": {
"value": "What is the date of the transfer of ownership? Return the date in 'Month Day, Year' format. Example: 'January 1, 2023'. If no date is found, return 'N/A'.",
"score":"How confident are you about this recording date ? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the recording date.",
"summary" : "what relevant information do you have in this file about recording date? summarize in less than 50 words."
},
"grantee": {
"value": "Who is receiving the property or rights? Provide full names.",
"score": "How confident are you about this grantee ? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the grantee.",
"summary" : "what relevant information do you have in this file about grantee? summarize in less than 50 words."
},
"grantor": {
"value": "Who is selling or transferring the property or rights? Provide full names.",
"score": "How confident are you about this grantor ? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the grantor.",
"summary" : "what relevant information do you have in this file about grantor? summarize in less than 50 words."
},
"property_description": {
"value": "What is the description of the property being transferred?",
"score": "How confident are you about this property description ? Rate your confidence in a range 1 to 10. 1 being not able to understand the document and generating data from generic information to 10 being having exact answer to the question specifically mentioned in the document.",
"source": "Provide the exact text from the document that was used to determine the property description.",
"summary" : "what relevant information do you have in this file about property description? summarize in less than 50 words."
},
"reservations": {
"value": "Is there any property or rights which are explicitly excluded from the transaction by the Grantor(s)?",
"score": "Provide a confidence score (0-10) indicating the reliability of the extracted reservations.",
"source": "Provide the exact text from the document that was used to determine the reservations."
},
"conditions": {
"value": "Are there any conditions that must be met after the effective date to finalize or avoid reversion of the sale?",
"score": "Provide a confidence score (0-10) indicating the reliability of the extracted conditions.",
"source": "Provide the exact text from the document that was used to determine the conditions."
}
}

    THE FINAL OUTPUT BASED ON THESE PROMPTS

"instrument_type": {
"value": "Deed",
"score": 10,
"source": "To Joseph Patrucio Deed THE STATE OF TEXAS COUNTY OF MATAGORDA Know all men by these presents, that I, Frank Seardon of the County of Matagorda and State
aforesaid...",
"summary": "The document is a deed where the property is conveyed from Frank Seardon to Joseph Patrucio."
},
"volume_page": {
"value": "Book L/513",
"score": 9,
"source": "for a better description of the land herein conveyed see deed from the Admrs. of H. S. Powell Decd. to myself and Joseph Patrucio, recorded in Matagorda County, book L, p. 513",
"summary": "The property is referenced in Book L, Page 513 for description purposes."
},
"document_case_number": {
"value": "#8370",
"score": 10,
"source": "No. 8370 Frank Seardon To Joseph Patrucio Deed THE STATE OF TEXAS",
"summary": "The document case number is 8370, indicating the specific deed transaction between the parties."
},
"execution_date": {
"value": "April 17, 1880",
"score": 10,
"source": "Witness my hand at Matagorda this 17th day of April, 1880.",
"summary": "The deed was executed on April 17, 1880."
},
"effective_date": {
"value": "April 17, 1880",
"score": 10,
"source": "Witness my hand at Matagorda this 17th day of April, 1880.",
"summary": "The effective date of the transfer of ownership is April 17, 1880."
},
"recording_date": {
"value": "April 19, 1880",
"score": 10,
"source": "Recorded at 11 o'clock April 19th, 1880.",
"summary": "The document was recorded on April 19, 1880."
},
"grantee": {
"value": "Joseph Patrucio",
"score": 10,
"source": "to the said Joseph Patrucio, his heirs and assigns",
"summary": "Joseph Patrucio is the grantee receiving the property."
},
"grantor": {
"value": "Frank Seardon",
"score": 10,
"source": "I, Frank Seardon of the County of Matagorda and State aforesaid...",
"summary": "Frank Seardon is the grantor selling the property."
},
"property_description": {
"value": "Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8",
"score": 10,
"source": "the following described property totwit: Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8",
"summary": "The property consists of 250 acres in subdivision No. 1 out of league Survey No. 8."
},
"reservations": {
"value": "none found",
"score": 1,
"source": "",
"summary": "No reservations were mentioned in the document."
},
"conditions": {
"value": "none found",
"score": 1,
"source": "",
"summary": "No specific conditions post-effective date were detailed in the document."
}
}

Logs for these data

PS C:\Users\DELL\Desktop\titlemine> python app.py
2025-03-03 16:07:13,520 - INFO - Processing document with file_id: 63, OCR text length: 7342
2025-03-03 16:07:16,053 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-03 16:07:16,061 - INFO - Total Token used for instrument_type: 2313
2025-03-03 16:07:25,493 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
2025-03-03 16:07:25,496 - INFO - {
"instrument_type": {
"value": "Deed",
"score": 10,
"source": "In the document, it is stated: 'To Joseph Patrucio Deed' and 'To Frank Seardon Deed'.",
"summary": "This legal document involves property transactions referenced as deeds executed between parties."
},
"volume_page": {
"value": "book L / p. 513",
"score": 8,
"source": "References to 'recorded in Matagorda County, book L, p. 513' were noted in the descriptions.",
"summary": "The volume and page reference where the document is recorded is book L, page 513."
},
"document_case_number": {
"value": "#8370",
"score": 10,
"source": "The document contains a number mention: 'No. 8370'.",
"summary": "The document case number is clearly stated as #8370."
},
"execution_date": {
"value": "April 17, 1880",
"score": 10,
"source": "The date of execution is noted as 'this 17th day of April, 1880'.",
"summary": "The execution date of the document is April 17, 1880."
},
"effective_date": {
"value": "April 17, 1880",
"score": 10,
"source": "The effective transaction date is indicated on the execution date as 'this 17th day of April, 1880'.",
"summary": "The effective date of the property transaction is April 17, 1880."
},
"recording_date": {
"value": "April 19, 1880",
"score": 10,
"source": "The document mentions 'Recorded at 11 o'clock April 19th, 1880'.",
"summary": "The recording date of the document is April 19, 1880."
},
"grantee": {
"value": "Joseph Patrucio",
"score": 10,
"source": "The grantee is identified as 'Joseph Patrucio' in the transaction.",
"summary": "The individual receiving the property is Joseph Patrucio."
},
"grantor": {
"value": "Frank Seardon",
"score": 10,
"source": "The grantor is identified as 'Frank Seardon' in the document.",
"summary": "The individual transferring ownership is Frank Seardon."
},
"property_description": {
"value": "Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8, D. McFarlane Original grantee.",  
 "score": 10,
"source": "The document describes the property as 'Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8'.",
"summary": "The property consists of 250 acres described as subdivision No. 1 from the D. McFarlane survey."
},
"reservations": {
"value": "none found",
"score": 0,
"source": "No specific exclusions or reservations are noted in the document.",
"summary": "There are no reservations explicitly mentioned in the provided text."
},
"conditions": {
"value": "none found",
"score": 0,
"source": "The document does not specify any conditions that relate to the transfer.",
"summary": "No conditions are mentioned that need to be satisfied following the effective date."
}
}
{
"instrument_type": {
"value": "Deed",
"score": 10,
"source": "In the document, it is stated: 'To Joseph Patrucio Deed' and 'To Frank Seardon Deed'.",
"summary": "This legal document involves property transactions referenced as deeds executed between parties."
},
"volume_page": {
"value": "book L / p. 513",
"score": 8,
"source": "References to 'recorded in Matagorda County, book L, p. 513' were noted in the descriptions.",
"summary": "The volume and page reference where the document is recorded is book L, page 513."
},
"document_case_number": {
"value": "#8370",
"score": 10,
"source": "The document contains a number mention: 'No. 8370'.",
"summary": "The document case number is clearly stated as #8370."
},
"execution_date": {
"value": "April 17, 1880",
"score": 10,
"source": "The date of execution is noted as 'this 17th day of April, 1880'.",
"summary": "The execution date of the document is April 17, 1880."
},
"effective_date": {
"value": "April 17, 1880",
"score": 10,
"source": "The effective transaction date is indicated on the execution date as 'this 17th day of April, 1880'.",
"summary": "The effective date of the property transaction is April 17, 1880."
},
"recording_date": {
"value": "April 19, 1880",
"score": 10,
"source": "The document mentions 'Recorded at 11 o'clock April 19th, 1880'.",
"summary": "The recording date of the document is April 19, 1880."
},
"grantee": {
"value": "Joseph Patrucio",
"score": 10,
"source": "The grantee is identified as 'Joseph Patrucio' in the transaction.",
"summary": "The individual receiving the property is Joseph Patrucio."
},
"grantor": {
"value": "Frank Seardon",
"score": 10,
"source": "The grantor is identified as 'Frank Seardon' in the document.",
"summary": "The individual transferring ownership is Frank Seardon."
},
"property_description": {
"value": "Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8, D. McFarlane Original grantee.",  
 "score": 10,
"source": "The document describes the property as 'Two hundred and fifty (250) acres of land it being subdivision No. 1 out of league Survey No. 8'.",
"summary": "The property consists of 250 acres described as subdivision No. 1 from the D. McFarlane survey."
},
"reservations": {
"value": "none found",
"score": 0,
"source": "No specific exclusions or reservations are noted in the document.",
"summary": "There are no reservations explicitly mentioned in the provided text."
},
"conditions": {
"value": "none found",
"score": 0,
"source": "The document does not specify any conditions that relate to the transfer.",
"summary": "No conditions are mentioned that need to be satisfied following the effective date."
}
}
2025-03-03 16:07:25,612 - INFO - Successfully stored data for file_id: 63
Data stored successfully: True
