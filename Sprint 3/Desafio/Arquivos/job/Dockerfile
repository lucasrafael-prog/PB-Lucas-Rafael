# Etapa 4
FROM python:3.11

WORKDIR /app

COPY job.py .
# Antes da implantação do docker-compose, utilizo o comando:
# Copio o csv da raiz de arquivos
# COPY etl/csv_limpo.csv ./etl/csv_limpo.csv
RUN pip install pandas matplotlib

CMD ["python" , "job.py"]