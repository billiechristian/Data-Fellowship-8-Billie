from google.cloud import storage
import os

BUCKET = 'data-fellowship-8-billie'
URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet'
LOCAL_FILE_NAME = 'yellow_tripdata_2022-06.parquet'
GCS_PATH = 'practice/' + LOCAL_FILE_NAME

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'iykra-df8-creds.json'
# os.environ["GCLOUD_PROJECT"]='iykra-df8'

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


if __name__ == '__main__':

    os.system(f"wget {URL} -O {LOCAL_FILE_NAME}")
    upload_to_gcs(BUCKET, GCS_PATH, LOCAL_FILE_NAME)
