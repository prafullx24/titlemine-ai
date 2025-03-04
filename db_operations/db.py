import psycopg2.extras

import config

def get_db_connection():
    """Establish and return a database connection."""
    connection = psycopg2.connect(**config.DB_CONFIG)
    return connection

def select_file_by_projectid(connection, projectid):
    select_query = """
        SELECT id, user_id, project_id, file_name, s3_url, ocr_status 
        FROM public.files 
        WHERE project_id = %s 
        AND ocr_status = 'Processing'
    """

    with connection.cursor() as cursor:
        cursor.execute(select_query, (projectid,))
        files = cursor.fetchall()

    return files



def select_file_by_fileid(connection, fileid):
    select_query = """
        SELECT id, user_id, project_id, file_name, s3_url, ocr_status 
        FROM public.files 
        WHERE id = %s 
        AND ocr_status = 'Processing'
    """

    with connection.cursor() as cursor:
        cursor.execute(select_query, (fileid,))
        files = cursor.fetchall()

    return files


def insert_or_update_ocr_data(connection, new_records):
    insert_query = """
    INSERT INTO public.ocr_data (file_id, project_id, ocr_json_1, ocr_text_1)
    VALUES %s
    ON CONFLICT (file_id, project_id)  
    DO UPDATE SET 
        ocr_json_1 = EXCLUDED.ocr_json_1,
        ocr_text_1 = EXCLUDED.ocr_text_1
    """
    
    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, insert_query, new_records)
        # connection.commit()



def update_file_status(connection, file_ids, status='Extracting'):
    update_status_query = "UPDATE public.files SET ocr_status = %s WHERE id = ANY(%s::int[])"
    with connection.cursor() as cursor:
        cursor.execute(update_status_query, (status, file_ids))


