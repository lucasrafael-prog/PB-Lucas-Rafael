# Etapa 3
FROM python:3.11

WORKDIR /app
# Cópia dos arquivos
COPY concert_tours_by_women.csv . 
COPY etl.py .

RUN pip install pandas

CMD ["python", "etl.py"]