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