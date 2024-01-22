FROM python:3.11

RUN apt-get update -y
WORKDIR /usr/src/app
# Install package dependencies as recommended 
RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --requirement /tmp/requirements.txt

COPY ingest_data.py ingest_data.py 

# Run the ingestion script 
ENTRYPOINT [ "python", "ingest_data.py" ]
#CMD [ "python", "ingest_data.py" ]
