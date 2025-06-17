FROM python:3.11-slim

WORKDIR /app

COPY ingestaocsv.py ./
COPY requirements.txt ./
COPY .env ./

RUN pip install -r requirements.txt

CMD [ "python", "ingestaocsv.py" ]