# The download_file_from_s3 function downloads a file from an S3 URL and saves it locally.
import logging
import os
import requests
import config


def download_file_from_s3(s3_url, user_id, project_id, file_id, file_extension):
    """Download a file from S3 URL and save it locally"""
    file_name = f"download_pdf_{user_id}_{project_id}_{file_id}{file_extension}"
    file_path = os.path.join(config.DOWNLOAD_FOLDER, file_name)
    if os.path.exists(file_path):
        return file_path
    response = requests.get(s3_url, stream=True)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        logging.info(f"Downloaded file from S3: file_id: {file_id}; project_id {project_id}")
        return file_path
    logging.error(f"Failed to download file from S3: {s3_url}")
    return None