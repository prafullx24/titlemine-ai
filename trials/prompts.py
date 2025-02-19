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

Instrument Type: Extract what type of legal instrument or document is in this file (deed, lease, quitclaim, will, etc.).
If the type is an amendment, return what kind of instrument it is amending.

Based on the Instrument Type, extract:
- Recording Information
- Recording Date
- Execution Date
- Effective Date
- Grantor(s)
- Grantee(s)
- Property Description
- Reservations
- Conditions

Provide structured JSON output.
"""

user_prompt = f"Extract legal information from the following document:\n\n{document_text}"

# Send request to OpenAI
completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
)

# Print extracted data
print(completion.choices[0].message.content)


"""
Expected Output:
"Instrument_Type": "Deed",
  "Recording_Information": "Filed at 11 Oclock A M May 29th. 1879; Recorded at 4 Oclock P M May 29th. 1879.",
  "Recording_Date": "1879-05-29",
  "Execution_Date": "1879-05-27",
  "Effective_Date": null,
  "Grantor": "Antonette Roberts, formerly Antonette Power",
  "Grantee": "William C Braman",
  "Property_Description": "Fourteen hundred & Twenty eight acres of land situated in said Matagorda County being a part of F W Dempsey league on the east side of the Colorado River, bounded as follows: on the Westerly end by said River, on the easterly end by the S Ingram league, on the Northerly side by the McFarland and Jaques league, and on the Southerly side by 500 acres of land part of same league sold by Charles Power decd. to Tupper Barton & Taylor.",
  "Reservations": null,
  "Conditions": "I the said Antonette E Roberts do hereby covenant with the said Braman his heirs & assigns that I am lawfully seized with a fee simple title to the said land 1428 acres heretofore described & conveyed & that I have perfect right to sell and convey and dispose the same as I have here done. And I do hereby warrant and will forever defend the title and possession of said tract of land unto him the said William Cheever Braman his heirs or assigns forever, against the heirs of the said Charles Power and against all other persons legally claiming or to claim the same or any part thereof."

"""