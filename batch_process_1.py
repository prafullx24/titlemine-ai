import os
from flask import Flask, request, jsonify
from google.cloud import documentai_v1 as documentai

from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

@app.route('/process-documents', methods=['POST'])
def process_documents():

    # Google Document AI Configuration
    project_id = os.getenv("PROJECT_ID")
    location = os.getenv("LOCATION")
    processor_id = os.getenv("PROCESSOR_ID")

    
    gcs_input_uri = request.json.get('gcs_input_uri')
    gcs_output_uri = request.json.get('gcs_output_uri')
    
    # Initialize Document AI client
    client = documentai.DocumentProcessorServiceClient()
    
    # Format the processor name
    processor_name = client.processor_path(project_id, location, processor_id)
    
    # Configure the batch process request
    input_config = documentai.BatchProcessRequest.BatchInputConfig(
        gcs_source=gcs_input_uri,
        mime_type="application/pdf"  # Adjust based on your document types
    )
    
    output_config = documentai.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=gcs_output_uri
    )
    
    # Create the batch request
    request = documentai.BatchProcessRequest(
        name=processor_name,
        input_configs=[input_config],
        output_config=output_config
    )
    
    # Make the batch processing request
    operation = client.batch_process_documents(request=request)
    
    # Return the operation name which can be used to check status later
    return jsonify({
        "operation_id": operation.name,
        "status": "Batch processing started"
    })

@app.route('/get-operation-status', methods=['GET'])
def get_operation_status():
    operation_name = request.args.get('operation_name')
    
    # Initialize Document AI client
    client = documentai.DocumentProcessorServiceClient()
    
    # Get operation
    operation = client.get_operation(name=operation_name)
    
    if operation.done:
        return jsonify({
            "status": "completed",
            "metadata": dict(operation.metadata)
        })
    else:
        return jsonify({
            "status": "in_progress",
            "metadata": dict(operation.metadata)
        })

if __name__ == '__main__':
    # Set environment variable for authentication
    credentials_path = os.getenv("CREDENTIALS_PATH")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    app.run(debug=True)