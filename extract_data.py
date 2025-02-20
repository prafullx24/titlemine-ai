import os
import json
import psycopg2
from flask import Flask, jsonify
from dotenv import load_dotenv
import openai
from datetime import datetime

# Load environment variables
load_dotenv()

# Configuration class
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is missing.")

# Function to get a database connection
def get_db_connection():
    try:
        return psycopg2.connect(Config.DATABASE_URL)
    except Exception as e:
        print(f"Error getting DB connection: {e}")
        return None

# Function to fetch OCR text from the database
def fetch_ocr_text(file_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return None, "Database connection error"

        with conn:
            with conn.cursor() as cur:
                query = "SELECT file_id, project_id, ocr_json_1 FROM ocr_data WHERE file_id = %s"

                try:
                    cur.execute(query, (int(file_id),))  # If file_id is an integer
                except ValueError:
                    cur.execute(query, (file_id,))  # If file_id is a string
                
                response = cur.fetchone()

                if not response:
                    return None, None, None, "file_id not found in ocr_data table"

                file_id_from_db, project_id, ocr_json_1 = response

                try:
                    ocr_data = json.loads(ocr_json_1) if isinstance(ocr_json_1, str) else ocr_json_1
                    return file_id_from_db, project_id, ocr_data, None
                except json.JSONDecodeError:
                    return file_id_from_db, project_id, None, "Invalid JSON format"
    except Exception as e:
        print(f"Error fetching OCR text: {e}")
        return None, str(e)
    

   
def store_extracted_data(file_id, project_id, extracted_data):
    try:
        conn = get_db_connection()
        if conn is None:
            return "Database connection error"
        
        with conn:
            with conn.cursor() as cur:
                # Convert date fields to proper DATE format
                def convert_date(date_str):
                    if date_str and date_str.lower() != "none found" and date_str.lower() != "n/a":
                        try:
                            return datetime.strptime(date_str, "%B %d, %Y").date()
                        except ValueError:
                            return None
                    return None

                # Parse the JSON string if it's a string
                if isinstance(extracted_data, str):
                    try:
                        extracted_data = json.loads(extracted_data)
                    except json.JSONDecodeError:
                        return "Invalid JSON data"

                execution_date = convert_date(extracted_data.get("execution_date"))
                effective_date = convert_date(extracted_data.get("effective_date"))
                recording_date = convert_date(extracted_data.get("recording_date"))

                # Prepare data fields
                instrument_type = extracted_data.get("instrument_type", "N/A")
                volume_page = extracted_data.get("volume_page", "N/A")
                document_case_number = extracted_data.get("document_case_number", "N/A")
                combined_volume_page = f"{volume_page} {document_case_number}"
                grantor = extracted_data.get("grantor", "N/A")
                grantee = json.dumps(extracted_data.get("grantee", []))
                land_description = json.dumps(extracted_data.get("property_description", []))
                
                # Store reservations in remarks column
                remarks = extracted_data.get("reservations", "N/A")
                
                # Store conditions text in remarks
                conditions = extracted_data.get("conditions", "N/A")
                if conditions != "N/A":
                    remarks = f"Reservations: {remarks}; Conditions: {conditions}"
                
                # Set sort_sequence to a default integer value
                sort_sequence = 0

                # First check if record exists
                check_query = "SELECT COUNT(*) FROM public.runsheets WHERE file_id = %s"
                cur.execute(check_query, (file_id,))
                exists = cur.fetchone()[0] > 0

                if exists:
                    # Update existing record
                    update_query = """
                    UPDATE public.runsheets SET 
                        instrument_type = %s,
                        volume_page = %s,
                        effective_date = %s,
                        execution_date = %s,
                        file_date = %s,
                        grantor = %s,
                        grantee = %s,
                        land_description = %s,
                        remarks = %s,
                        sort_sequence = %s,
                        updated_at = NOW()
                    WHERE file_id = %s
                    """
                    cur.execute(update_query, (
                        instrument_type, combined_volume_page, 
                        effective_date, execution_date, recording_date,
                        grantor, grantee, land_description, 
                        remarks, sort_sequence, file_id
                    ))
                else:
                    # Insert new record
                    insert_query = """
                    INSERT INTO public.runsheets (
                        file_id, project_id, instrument_type, volume_page, 
                        effective_date, execution_date, file_date, grantor, grantee, 
                        land_description, remarks, sort_sequence, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """
                    cur.execute(insert_query, (
                        file_id, project_id, instrument_type,
                        combined_volume_page, effective_date, execution_date, recording_date,
                        grantor, grantee, land_description, remarks, sort_sequence
                    ))

        return "Data stored successfully"
    except Exception as e:
        print(f"Error storing extracted data: {e}")
        return str(e)
def prompts_by_instrument_type(instrument_type):
    prompts = {
        "Deed": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Deed",
            "volume_page": "Reference the recording information found in the document and/or the file name to determine the volume number and page number of where the document was filed with the county. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Reference information found in the margins, top of the document, or in the file name to determine the case, file, or recording number. Return as '#Document Number'. If none is found, return 'none found'.",
            "execution_date": "What is the latest date on which a grantor or grantee signed the document?",
            "effective_date": "What is the date of the transfer of ownership?",
            "recording_date": "What is the date of the transfer of ownership?",
            "grantee": "Who is selling or transferring the property or rights? Provide full names.",
            "grantor": "Who is receiving the property or rights? Provide full names.",
            "property_description": "What is the description of the property being transferred?",
            "reservations": "Is there any property or rights which are explicitly excluded from the transaction by the Grantor(s)?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize or avoid reversion of the sale?"
        }
        },
        "Lease": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Lease",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which a lessor or lessee signed the document?",
            "effective_date": "What is the date the lease is effective?",    
            "recording_date": "What is the date on which the lease was recorded or filed with the county?",
            "grantee": "Who is granting rights under.",
            "grantor": "Who is granting rights under this lease?",
            "property_description": "What is the description of the rights being transferred, including a description of the land they are associated with?",
            "reservations": "Are there rights which are explicitly excluded from the transaction by the lessors?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize, maintain, or extend the lease?"
        }
        },
        "Release": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Release",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the release was signed?",
            "effective_date": "What is the date that the release went into effect?",
            "recording_date": "What is the date on which the release was recorded or filed with the county?",
            "grantee": "Who is gaining the rights or property?",
            "grantor": "Who is releasing the rights or property?",
            "property_description": "What is the description of the rights or property being transferred, including a description of the land they are associated with?",
            "reservations": "Are there rights which are explicitly excluded from the release?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize or avoid negating the release?"
        }
        },
        "Waiver": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Waiver",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the waiver was signed?",
            "effective_date": "What is the date that the waiver went into effect?",
            "recording_date": "What is the date on which the waiver was recorded or filed with the county?",
            "grantee": "Who is the counterparty in the waiver?.",
            "grantor": "Who is waiving their rights?",
            "property_description": "What is the description of the rights being waived, including a description of the land they are associated with?",
            "reservations": "Are there rights which are explicitly excluded from the waiver?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize or avoid negating the waiver?"
        }
        },
        "Quitclaim": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
             "instrument_type":"Quitclaim",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the quitclaim was signed?",
            "effective_date": "What is the date that the quitclaim went into effect?",
            "recording_date": "What is the date on which the quitclaim was recorded or filed with the county?",
            "grantor": "Who is releasing the rights or property?",
            "grantee": "Who is gaining the rights or property?",
            "property_description": "Provide a detailed description of the property or rights being released.",
            "reservations": "Are there rights which are explicitly excluded from the quitclaim?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize the quitclaim?"
        }
        },
        "Option": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Option",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the option was signed?",
            "effective_date": "What is the date that the option went into effect?",
            "recording_date": "What is the date on which the option was recorded or filed with the county?",
            "grantor": "Who granted the option?",
            "grantee": "Who received the option rights?",
            "property_description": "What is the description of the rights or property being optioned, including a description of the land they are associated with?",
            "reservations": "Are there rights which are explicitly excluded from the option?",
            "conditions": "How long is the option and what are the conditions that must be met to exercise the option?"
        }
        },
        "Easement or Right of Way": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Easement or Right of Way",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the easement was signed?",
            "effective_date": "What is the date that the easement went into effect?",
            "recording_date": "What is the date on which the easement was recorded or filed with the county?",
            "grantor": "Who granted the easement?",
            "grantee": "Who received the easement?",
            "property_description": "What is the description of the rights of the easement, including a description of the land they cover?",
            "reservations": "Are there rights which are explicitly excluded from the easement?",
            "conditions": "Are there rights which are explicitly excluded from the easement?"
        }
        },
        "Ratification": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Ratification",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the ratification was signed?",
            "effective_date": "What is the date that the ratification went into effect?",
            "recording_date": "What is the date on which the ratification was recorded or filed with the county?",
            "grantor": "Who was the grantor in the agreement being ratified?",
            "grantee": "Who is the grantee in the agreement being ratified?Identify the entity or person receiving the release.",
            "property_description": "What rights or property are being confirmed or transfered in this ratification, including a description of the land involved?",
            "reservations": "Are there rights or property which are explicitly excluded from the ratification?",
            "conditions": "What conditions were met to ratify the original agreement? Are there any remaining conditions/"
        }
        },
        "Affidavit": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Affidavit",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the affidavit was signed?",
            "effective_date": "Return: N/A",
            "recording_date": "What is the date on which the affidavit was recorded or filed with the county?",
            "grantor": "Who was attesting to the information provided?",
            "grantee": "Return: N/A",
            "property_description": "What is being attested to in the affidavit?",
            "reservations": "Return: N/A",
            "conditions": "Return: N/A"
        }
        },
        "Probate": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Probate",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the probate document was signed?",
            "effective_date": "What is the effective date of the probate document?",
            "recording_date": "What is the date on which the probate was recorded or filed with the county?",
            "grantor": "Who died?",
            "grantee": "Who are the heirs or beneficiaries that will receive property?",
            "property_description": "List each heir or beneficiary of probate and what assets they received",
            "reservations": "Return: N/A",
            "conditions": "List each heir or beneficiary of probate and whether there are conditions that must be met for them to receive assets?"
        }
        },
        "Will and Testament": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Will and Testament",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the will was signed?",
            "effective_date": "Return: N/A",
            "recording_date": "What is the date on which the will was recorded or filed with the county?",
            "grantor": "Who died?",
            "grantee": "Who are the heirs or beneficiaries that will receive property?",
            "property_description": "List each heir or beneficiary of the will and what assets they received.",
            "reservations": "Return: N/A",
            "conditions": "List each heir or beneficiary of the will and whether there are conditions that must be met for them to receive assets?"
        }
        },
        "Death Certificate": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Death Certificate",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "Return: N/A",
            "effective_date": "Return: N/A",
            "recording_date": "What is the date of the death certificate?",
            "grantor": "Who died?",
            "grantee": "Return: N/A",
            "property_description": "Return: N/A",
            "reservations": "Return: N/A",
            "conditions": "Return: N/A"
        }
        },
        "Obituary": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Obituary",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "Return: N/A",
            "effective_date": "Return: N/A",
            "recording_date": "Who died?",
            "grantor": "Who died?",
            "grantee": "Who are the surviving family members and what is their connection to the deceased?",
            "property_description": "Return: N/A",
            "reservations": "Return: N/A",
            "conditions": "Return: N/A"
        }
        },
        "Divorce": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Divorce",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which the divorce filing was signed?",
            "effective_date": "What is the effective date of the divorce finalization?",
            "recording_date": "What is the date that the divorce filing was recorded or filed with the county?",
            "grantor": "Who was the spouse filing for divorce?",
            "grantee": "Who was the other spouse?",
            "property_description": "List each spouse and the property they received in the divorce.",
            "reservations": "Return: N/A",
            "conditions": "Are there any conditions that must be met for the distribution of property to take place as described in the divorce filing?"
        }
        },
        "Adoption": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Adoption",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date that the adoption documents were signed?",
            "effective_date": "Return: N/A",
            "recording_date": "What is the date the adoption was recorded or filed with the county?",
            "grantor": "Who was doing the adopting?",
            "grantee": "Who was adopted?",
            "property_description": "Return: N/A",
            "reservations": "Return: N/A",
            "conditions": "Return: N/A"
        }
        },
        "Court Case": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Court Case",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "Return: N/A",
            "effective_date": "What is the date that the ruling of the court case goes into effect?",
            "recording_date": "What is the date of the court case recording or filing with the county?",
            "grantor": "Who is the plaintiff?",
            "grantee": "Who is the defendant?",
            "property_description": "Does the court case ruling result in the transfer of rights or property? If so, what rights or property are transfered?",
            "reservations": "Are there any rights or property explicitly excluded from the ruling?",
            "conditions": "Are there any conditions that must be met in order for the transfer of rights to take place?"
        }
        },
        "Assignment": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
            "instrument_type":"Assignment",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which party signed the assignment?",
            "effective_date": "What is the effective date of the assignment?",
            "recording_date": "What is the date of the assignment recording or filing with the county?",
            "grantor": "Who is the grantor in this assignment?",
            "grantee": "Who is the grantee in this assignment?",
            "property_description": "Return: N/A",
            "reservations": "Return: N/A",
            "conditions": "Return: N/A"
        }
        },
        "Other": {
        "system": "You are a legal expert extraction algorithm specializing in property law and land transactions. Extract the following details from the provided legal land document and provide output in valid JSON format.",
        "fields": {
             "instrument_type":"Other",
            "volume_page": "Identify the volume and page number where the document was recorded. Return as 'Volume Number/Page Number'.",
            "document_case_number": "Determine the case, file, or recording number. If not found, return 'none found'.",
            "execution_date": "What is the latest date on which party signed the document?",
            "effective_date": "What is the date the document is effective?",
            "recording_date": "What is the date on which the document was recorded or filed with the county?",
            "grantor": "Who is giving property or rights?",
            "grantee": "Who is receiving property or rights?",
            "property_description": "What is the description of the property or rights being transferred?",
            "reservations": "Is there any property or rights which are explicitly excluded from the transaction?",
            "conditions": "Are there any conditions that must be met after the effective date to finalize the transfer?"
        }
        }
        }
    fields = prompts.get(instrument_type, {}).get("fields", {})
    return json.dumps(fields, indent=4)


def extract_instrument_type(ocr_text):
    client = openai.OpenAI()
    # Define system and user prompts
    system_prompt = """
    You are a legal expert extraction algorithm specializing in property law and land transactions.
    Extract the following details from the provided legal land document and provide output in valid JSON format.
    """
    user_prompt_doc_type ="""Extract legal information from the following document:\n\n{ocr_text}. 
    Instrument Type can be one of following: Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification, Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. If the type is an amendment, return what kind of instrument it is amending."""

    # Send request to OpenAI
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
    )
    # Print extracted data
    resp = completion.choices[0].message.content
    resp = resp.strip("```").lstrip("json\n").strip()

    try:
        json_resp = json.loads(resp)
        return json_resp  # Return JSON object (dict)
        # print(json_resp)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
        return {"error": "Invalid JSON response from OpenAI", "raw_response": resp}
        


def extract_and_process_document(ocr_text):
    try:
        client = openai.OpenAI()  # New OpenAI client instance

        system_prompt = f"""
        You are a legal expert extraction algorithm specializing in property law and land transactions.
        Extract the following details from the provided legal land document and provide output in valid JSON format.
        Extract instrument type from the following document:\n\n{ocr_text}
        """
        
        instrument_type_data = extract_instrument_type(ocr_text)
        # print(instrument_type_data)
      
        instrument_type = instrument_type_data.get("instrument_type", "")
        prompt_output = prompts_by_instrument_type(instrument_type)
        # print(prompt_output)
        user_prompt_doc_type = f"""{prompt_output} according to these parameters, find the corresponding information and return the values in similar json."""
        
        if not instrument_type:
            raise ValueError("Instrument type could not be extracted.")
    
        # Send request to OpenAI for document processing
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt_doc_type}]
        )

        result = response.choices[0].message.content.strip("```").lstrip("json\n").strip()
        output_file_path = os.path.join('output', f'{instrument_type}_extracted_data.json')
        with open(output_file_path, 'w') as output_file:
            json.dump(result, output_file, indent=4)

        return result

    except Exception as e:
        print(f"Error processing document: {e}")
        return str(e)

# Run the Flask application
if __name__ == "__main__":
    # app = create_app()
    # app.run(debug=True)
    file_id_from_db, project_id, ocr_data, error = fetch_ocr_text(206)
    
    if error:
        print(f"Error: {error}")
    else:
        ocr_text = ocr_data.get("text", "")
        extracted_data = extract_and_process_document(ocr_text)
        if extracted_data:
            try:
                # Parse the JSON if it's a string
                if isinstance(extracted_data, str):
                    extracted_json = json.loads(extracted_data)
                else:
                    extracted_json = extracted_data
                storage_result = store_extracted_data(file_id_from_db, project_id, extracted_json)
                print(f"Storage result: {storage_result}")
            except json.JSONDecodeError as e:
                print(f"Error parsing extracted data: {e}")
            except Exception as e:
                print(f"Error during storage: {e}")
        
        print("Extracted data:", extracted_data)
