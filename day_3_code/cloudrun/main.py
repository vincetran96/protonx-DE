from typing import List
from google.cloud import storage

BUCKET_NAME="mmo_event_processing_vietzergtran"
PREFIX="bronze-zone/event/"

def list_file_in_bucket(bucket_name :str,prefix: str) -> List[storage.Blob]:
    list_file = [] 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=prefix):
        list_file.append(blob)
    return list_file

if __name__ == "__main__":
    print(">>> Hello World, this is version 2")
    print(list_file_in_bucket(BUCKET_NAME,PREFIX))
