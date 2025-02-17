"""
Get Document Type
"""

import json
from openai import OpenAI

# Load OCR result from JSON file
file_path = "./files/DR R-417 1428 Ac..pdf.json"
with open(file_path, "r", encoding="utf-8") as file:
    ocr_data = json.load(file)

# Extract text content
document_text = ocr_data.get("text", "")

# Define OpenAI client
client = OpenAI()

# Define system and user prompts
system_prompt = """
You are a legal expert extraction algorithm specializing in property law and land transactions.
Extract the following details from the provided legal land document and provide output in valid JSON format.
"""

user_prompt_doc_type = f"Extract legal information from the following document:\n\n{document_text} What type of legal instrument or document is in this file (deed, oil and gas lease, quitclaim, death certificate, option to lease, will and testament, etc.)? If the type is an amendment, return what kind of instrument it is amending. Deed, Lease, Release, Waiver, Quitclaim, Option, Easement or Right of Way, Ratification,	Affidavit, Probate, Will and Testament, Death Certificate, Obituary, Divorce, Adoption, Court Case, Assignment or Other. expected output json should contain following fields: Document type and supporting evidence from the input text data."

# Send request to OpenAI
completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_doc_type}
    ]
)

# Print extracted data
print(completion.choices[0].message.content)


"""
Output 1:
{
  "Instrument Type": "Deed",
  "Recording Information": {
    "Filed": {
      "Date": "June 1st, 1879",
      "Time": "10 AM"
    },
    "Recorded": {
      "Date": "June 3rd, 1879",
      "Time": "2 PM"
    }
  },
  "Recording Date": "June 3rd, 1879",
  "Execution Date": "May 22nd, 1879",
  "Effective Date": "May 29th, 1879",
  "Grantor(s)": [
    {
      "Name": "Antonette Roberts",
      "Former Name": "Antonette Power",
      "County": "Washington",
      "State": "Texas"
    }
  ],
  "Grantee(s)": [
    {
      "Name": "William C Braman",
      "County": "Matagorda",
      "State": "Texas"
    }
  ],
  "Property Description": {
    "Location": "Part of F W Dempsey League",
    "County": "Matagorda",
    "Acres": 1428,
    "Boundaries": {
      "Westerly": "Colorado River",
      "Easterly": "S Ingram league",
      "Northerly": "McFarland and Jaques league",
      "Sutherly": "500 acres sold by Charles Power to Tupper Barton & Taylor"
    }
  },
  "Reservations": [],
  "Conditions": [
    {
      "Covenant": "Grantor is lawfully seized with a fee simple title to the property",
      "Warrant": "Will defend the title and possession against heirs of Charles Power and all other claims."
    }
  ]
}
"""