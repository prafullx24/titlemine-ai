from flask import Flask, request, jsonify
import subprocess  # For running the batch script

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_data():
    try:
        data = request.get_json()  # Get data from the request (e.g., a list of items)
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Save data to a file (or use a database/queue) for the batch script to read
        with open("data_to_process.json", "w") as f:
            import json
            json.dump(data, f) 


        # Start the batch processing script in the background
        subprocess.Popen(["python", "batch_processor.py"]) # Detach process
        # subprocess.run(["python", "batch_processor.py"], check=True) # For synchronous execution

        return jsonify({"message": "Batch processing started"}), 202  # 202 Accepted

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)