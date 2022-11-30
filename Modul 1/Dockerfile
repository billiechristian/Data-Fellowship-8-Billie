FROM python:3.9

RUN pip install google-cloud-storage

WORKDIR /app
COPY upload-to-gcs.py upload-to-gcs.py
COPY iykra-df8-creds.json iykra-df8-creds.json

ENTRYPOINT [ "python", "upload-to-gcs.py" ]
