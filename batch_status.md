curl https://api.openai.com/v1/batches/batch_67bc41b08f2481908efbcaa9ffb2d4fc \
  -H "Authorization: Bearer " \
  -H "Content-Type: application/json"

  FileObject(id='file-6t929NC1h2u6ofZxNam6Y6', bytes=516, created_at=1740390551, filename='test.jsonl', object='file', purpose='batch', status='processed', status_details=None, expires_at=None)


  curl https://api.openai.com/v1/files/file-M8axXUE4X2B7Xu68opfhRn/content \
  -H "Authorization: Bearer " > batch_output.jsonl