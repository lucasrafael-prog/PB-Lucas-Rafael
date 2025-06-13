import requests
import pandas as pd
from IPython.display import display
from dotenv import load_dotenv
import os

# Carregando arquivo .env
load_dotenv()

# Chamando API direto do arquivo
api_key = os.getenv("TMDB_API_KEY")

# URL da API com as series de TV criadas no século XXI mais bem avaliadas 
url = f"https://api.themoviedb.org/3/discover/tv?api_key={api_key}&language=pt-BR&sort_by=vote_average.desc&vote_count.gte=100&first_air_date.gte=2000-01-01"

# Requisição da URL
response = requests.get(url)
data = response.json()

# Processamento dos resultados
series = []
for show in data['results']:
    df = {'Titulo': show['name'],
        'Data de Estreia': show['first_air_date'],
        'Visão geral': show['overview'],
        'Popularidade': show['popularity'],
        'Média de votos:': show['vote_average'],
        'Total de votos:': show['vote_count']}
    series.append(df)
df = pd.DataFrame(series)
display(df)