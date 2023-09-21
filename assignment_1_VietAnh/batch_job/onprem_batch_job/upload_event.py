'''Upload multiple files to gloud: https://saturncloud.io/blog/how-to-upload-multiple-files-to-google-cloud-storage-bucket-as-a-transaction/
'''
from glob import glob
import argparse
import os
import tqdm
from decouple import Config,RepositoryEnv

from google.cloud import storage


def encode_destination_path(local_file_path:str, destination_prefix:str) -> str:
    """
        Chuyển đỏi đường dẫn local thành đường dẫn gcs 
        
        Args:
        local_file_path (str): Path của file 
        destination_prefix (str): Prefix trên gs

        Returns:
            str: Path trên gs 

        Ví dụ: 
            local_file_path = "./data/2023-08-12/local_file_name.json"
            destination_prefix = "bronze-zone/event"
        
        Kết quả mong muốn result = "bronze-zone/event/2023/08/12/local_file_name.json"

    """
    destination = ""
    #TODO: Begin
    import re

    date_pattern = r"\d{4}(?:-\d{2}){2}"
    date_subdir = re.search(date_pattern, local_file_path).group().replace("-", "/")
    file_subdir = re.sub(date_pattern, date_subdir, local_file_path)
    file_subdir = file_subdir[file_subdir.find(date_subdir):len(file_subdir)]
    destination = f"{destination_prefix}/{file_subdir}"
    #TODO: End
    return destination


def upload_file_to_storage(
    input_path:str, bucket_name:str, destination_prefix:str, project_id: str
) ->None :
    """ 
        Upload tất cả file trong folder data có đường dẫn dưới dạng 
        data/year-month-day/file.json
        lên google cloud storage dưới dạng

        bucket_name/prefix/year/month/day/file.json

        Args:
            input_path (str): đường dẫn đến folder data
            bucket_name (str): bucket trên google storage
            destination_prefix (str): prefix của google storage

        Ví dụ: 
            ├── batch_job
            │   └── onprem_batch_job
            │        └── upload_event.py
            └── data
                ├── 2023-08-09
                │   ├── file1.json
                └── 2023-08-10
                    └── file2.json

            
            Folder data có 2 file: 
                ./data/2023-08-09/file1.json
                ./data/2023-08-10/file2.json
            
            
            Upload lên folder: 
                gs://mmo_adventure_event_processing/bronze-zone/event

            Chúng ta đang ở thư mục onprem_batch_job 
                python upload_event.py 

            Input: 
                input_path = ../../data/
                destination_prefix = bronze-zone/event
            
            Sau khi upload
                    gs://mmo_adventure_event_processing/bronze-zone/event/2023/08/09/file1.json
                    gs://mmo_adventure_event_processing/bronze-zone/event/2023/08/10/file2.json
    """
    client = storage.Client(project=project_id)
    #TODO: Begin
    from pathlib import Path

    bucket = client.get_bucket(bucket_name)
    blobs = []

    try:
        for glob in Path(input_path).rglob("*.json"):
            destination_path = encode_destination_path(str(glob), destination_prefix)
            blob = bucket.blob(destination_path)
            blobs.append(blob)
            blob.upload_from_filename(str(glob))
    except Exception as exc:
        for blob in blobs:
            blob.delete()
        raise exc
    #TODO: End



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Upload file lên storage",
        description="""
            Upload file có đường dẫn dưới dạng 
            data/year-month-day/file.json
            lên google cloud storage dưới dạng

            bucket_name/prefix/year/month/day/file.json
        """,
    )
    
    parser.add_argument(
        "--input-path", 
        dest="input_path", 
        help="Input path name", 
        required=True
    )
    args = parser.parse_args()

    DOTENV_FILE = ".env"
    env_config = Config(RepositoryEnv(DOTENV_FILE))

    BUCKET_NAME = env_config.get("BUCKET_NAME")
    DESTINATION_PREFIX = env_config.get("EVENT_BRONZE_ZONE_PREFIX")
    PROJECT_ID = env_config.get("PROJECT_ID")
    
    upload_file_to_storage(
        args.input_path,
        BUCKET_NAME,
        DESTINATION_PREFIX,
        PROJECT_ID
    )
