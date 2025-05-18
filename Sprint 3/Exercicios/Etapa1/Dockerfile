FROM python:3.11

WORKDIR /app

COPY carguru.py .

RUN pip install --upgrade pip --no-cache-dir

CMD [ "python","carguru.py" ]

EXPOSE 8000