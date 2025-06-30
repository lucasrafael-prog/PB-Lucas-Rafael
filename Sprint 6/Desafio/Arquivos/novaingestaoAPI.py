# ETAPA 2 DO DESAFIO
# Importação das Bibliotecas
import json
import boto3
import os
import requests
import gc
from datetime import datetime
from time import sleep

# Nome do Bucket
bucket_name = 'datalake-lucasrafael'

# Conexão com o Bucket
s3 = boto3.client('s3')

# Chave da API do TMDB
api_key = os.getenv("TMDB_API_KEY")

# Sessão global para requests
session = requests.Session() # Ajuste que evita acúmulo de conexões
session.headers.update({'Accept': 'application/json'}) # Declarando o cabeçalho da sessão para aceitar o formato JSON
session.params = {'api_key': api_key, 'language': 'en-US'} # Parâmetros de requisiçõe(Chave e Idioma da API)

# Data e caminho para arquivos
hoje = datetime.now() # Data para o caminho
caminho_data = f"{hoje.year}/{hoje.month:02}/{hoje.day:02}" # Caminho com a data organizada

# Configurações
generos_desejados = [80, 10752] # Id do Gênero Guerra e Crime
wc_movies_data = [] # Lista para os dados de filmes de guerra e crime
cont_arquivo = 1 # Contagem do n° de arquivos
cont_filmes_total = 0 # Contagem do total de filmes
batch_tamanho = 100 # Tamanho estipulado para o arquivo JSON
max_filmes_total = 1500 # Máximo de filmes a serem buscados
max_pages = max_filmes_total // 20 # Máximo de páginas para busca (20 filmes por página)
ids_ja_add = set() # Conjunto para evitar filmes duplicados

def lambda_handler(event, context): 
    # Inicio da função Lambda
    global wc_movies_data, cont_arquivo, cont_filmes_total # Variáveis globais 

    # Estrutura para busca dos dados com os gêneros, ordenado por popularidade, separando por páginas
    for page in range(1, max_pages + 1):
        print(f"Buscando página {page}...") # Indicador da busca de páginas
        discover_url = f"https://api.themoviedb.org/3/discover/movie" # Url para buscar filmes
        params = { 
            'with_genres': "80|10752", # Gêneros Guerra e Crime
            'sort_by': 'popularity.desc',
            'page': page
        }
        try:
            response = session.get(discover_url, params=params) # Requisição para buscar filmes
            response.raise_for_status() # Verifica se a requisição foi bem sucedida
            movies = response.json().get("results", []) # Obtém os resultados da requisição
        except Exception as e:
            print(f"Erro ao buscar discover (página {page}): {e}") # Exceção para problemas na busca
            continue
        
        
        for f in movies: # Para cada item na lista de filmes
            movie_id = f.get("id") # Requisita o  ID do filme
            if not movie_id or movie_id in ids_ja_add: # Verifica se o filme já foi adicionado
                continue
            ids_ja_add.add(movie_id) # Adiciona o ID do filme no conjunto

            try:
                # Detalhes do filme
                details_url = f"https://api.themoviedb.org/3/movie/{movie_id}" # URL de Busca
                details_resp = session.get(details_url, params={'api_key': api_key, 'language': 'en-US'}) # Requisição da sessão de detalhes do filme
                details_resp.raise_for_status() # Verifica se a requisição foi bem sucedida
                movie_details = details_resp.json() # Obtem os detalhes da requisição no JSON
                sleep(0.1) # Intervalo para evitar sobrecarga na API
                
                # Verifica se o filme pertence aos gêneros Crime e Guerra
                if not any(g["id"] in generos_desejados for g in movie_details.get("genres", [])):
                    continue

                # Busca as Palavras-chave do filme
                keywords_url = f"https://api.themoviedb.org/3/movie/{movie_id}/keywords" # Url de busca
                keywords_resp = session.get(keywords_url, params={'api_key': api_key}) # Requisição da sessão de palavras-chave do filme
                keywords_resp.raise_for_status() # Verifica se a requisição foi bem sucedida
                keywords_data = keywords_resp.json() # Obtem as palavras-chave da requisição no JSON
                keywords_list = [k['name'] for k in keywords_data.get("keywords", [])] # Lista para as palavras-chave

                # Montando estrutura do JSON
                wc_movies_data.append({ # Inclusão das informações do filme
                    "movie_id": movie_id,
                    "title": movie_details.get("title"),
                    "overview": movie_details.get("overview"),
                    "release_date": movie_details.get("release_date"),
                    "popularity": movie_details.get("popularity"),
                    "vote_average": movie_details.get("vote_average"),
                    "vote_count": movie_details.get("vote_count"),
                    "collection": movie_details.get("belongs_to_collection", {}).get("name") if movie_details.get("belongs_to_collection") else None,
                    "genres": [g["name"] for g in movie_details.get("genres", []) if g["id"] in generos_desejados],
                    "keywords": keywords_list
                })

                cont_filmes_total += 1 # Incremento do total de filmes

                # Verifica se o tamanho do batch foi atingido para salvar o JSON no S3
                if len(wc_movies_data) == batch_tamanho: 
                    salvar_no_s3() # Chama a função para salvar os dados no S3
                    wc_movies_data.clear() # Limpa a lista após o envio
                    gc.collect() # Coleta lixo para liberar memória
                    sleep(0.5) # Intervalo para sobrecarga na API

            except Exception as e:
                print(f"Erro ao processar filme ID {movie_id}: {e}") # Exceção para erros de leitura
                continue
    
    # Verifica se ainda há dados para salvar no S3 após o loop
    if wc_movies_data: 
        salvar_no_s3()

    print(f"Coleta finalizada com {cont_arquivo - 1} arquivos enviados.") # Indicador de fim da coleta de dados
    
    return { # Retorno da função lambda 
        'statusCode': 200,
        'body': f'{cont_filmes_total} filmes processados e salvos no S3.'
    }

# Função para salvar os dados no S3
def salvar_no_s3():
    global wc_movies_data, cont_arquivo # Variáveis globais
    nome_arquivo = f'Raw/TMDB/JSON/Movies/{caminho_data}/filmes_{cont_arquivo:03d}.json' # Nome do arquivo com o caminho completo

    s3.put_object( # Envio do arquivo para o S3
        Bucket=bucket_name, 
        Key=nome_arquivo,
        Body=json.dumps(wc_movies_data, ensure_ascii=False).encode('utf-8')
    )

    print(f"Arquivo enviado para: s3://{bucket_name}/{nome_arquivo}") # Indicador de envio do arquivo
    cont_arquivo += 1 # Incrementa a contagem de arquivos

# Finalizado o desafio