# ETAPA 2 DO DESAFIO
# Importação das Bibliotecas
import json
import boto3
import os
from datetime import datetime
from time import sleep
from tmdbv3api import TMDb, Movie, Collection, Genre, Discover, Keyword

# Nome do Bucket
bucket_name = 'datalake-lucasrafael'

# Conexão com o Bucket
s3 = boto3.client('s3')

# Buscando Chave da API
tmdb = TMDb()
tmdb.api_key = os.getenv("TMDB_API_KEY")
tmdb.debug = True

# Funções do TMDB
movie = Movie()
collection = Collection()
genre = Genre()
keyword_api = Keyword()
discover = Discover()

# Função Lambda
def lambda_handler(event, context):
    # Instância do evento para testes na AWS
    tipo = event.get("tipo")

    # Estrutura condicional para teste
    if tipo == "franquias":
        processar_franquias()
    elif tipo == "guerra":
        processar_filmes_guerra()
    elif tipo == "crime":
        processar_filmes_crime()
    else:
        return {"status": "erro", "mensagem": "Tipo inválido. Use 'franquias', 'guerra' ou 'crime'."}

    return {"status": "ok", "mensagem": f"Processamento '{tipo}' concluído com sucesso"}

# Função de divisão das franquias
def processar_franquias():
    hoje = datetime.now() # Data para o caminho
    caminho_data = f"{hoje.year}/{hoje.month:02}/{hoje.day:02}" # Caminho com a data Organizada

    # Variáveis 
    crime_genre_id = 80 # Id do Gênero Crime
    war_genre_id = 10752 # Id do Gênero Guerra
    cont_filmes_total = 0 # Contagem do total de Filmes
    batch_tamanho = 100 # Tamanho estipulado para o arquivo JSON
    max_pages = 20 # Máximo de páginas para serem buscadas
    request_count = 0 # Contagem de requisições para controle

    # Identificação dos gêneros
    genres = {
        "Crime": crime_genre_id,
        "Guerra": war_genre_id
    }

    # Estrutura de repetição para a função
    for name_genre, genre_id in genres.items(): # Buscando informações de gênero
        print(f"Buscando filmes do gênero: {name_genre}")
        franqs_data = {} # Dicionário para separação das franquias
        cont_arquivo = 1 # Contagem para nomes de arquivos
        cont_filmes = 0 # Contagem de filmes para as franquias

        # Estrutura para busca dos dados com o gênero, ordenado por popularidade, separando por páginas
        for page in range(1, max_pages + 1): 
            movies = discover.discover_movies({
                'with_genres': genre_id,
                'sort_by': 'popularity.desc',
                'page': page
            })
            sleep(0.1) # Intervalo entre requisições evitando quebra do limite

            if not movies: # Quebrar o ciclo caso não haja filmes
                break

            for f in movies: # Para cada item na lista
                try:
                    movie_details = movie.details(f.id) # Requisitando detalhes dos filmes
                    request_count += 1 # Incremento da contagem de requisições
                    if movie_details.belongs_to_collection: # Estrutura para verificar os filmes que pertencem a franquias
                        cont_filmes += 1 # Incremento da contagem de filmes
                        cont_filmes_total += 1 # Incremento do total de filmes
                        coll_id = movie_details.belongs_to_collection['id']  # Variavel para busca do id
                        sleep(0.1) # Intervalo
                        coll_data = collection.details(coll_id) # Variavel para buscar os detalhes de franquia
                        coll_name = coll_data.name # Variavel para nome da franquia
                        request_count += 1 # Incremento da contagem de requisições

                        if coll_name not in franqs_data: # Identificando se filme está na franquia 
                            franqs_data[coll_name] = [] 

                        franqs_data[coll_name].append({ # Inclusão das informações do filme
                            "movie_id": movie_details.id,
                            "title": movie_details.title,
                            "popularity": movie_details.popularity,
                            "vote_average": movie_details.vote_average,
                            "vote_count": movie_details.vote_count
                        })
                        request_count += 1 # Incremento da contagem de requisições
                        sleep(0.1) # Intervalo

                        if request_count >= 30: # Implantando intervalo e esvaziando a contagem de requisições ao chegar no limite
                            sleep(2.0) # Intervalo
                            request_count = 0

                except Exception as e:
                    print(f"Erro ao processar filme ID {f.id}: {e}") # Exceção para leitura

        franqs_keys = list(franqs_data.keys()) # Transformando as franquias em lista
        for i in range(0, len(franqs_keys), batch_tamanho): # Estrutura para criação de arquivo com limite de registros
            batch = {key: franqs_data[key] for key in franqs_keys[i:i + batch_tamanho]} # Limite

            nome_arquivo = f'Raw/TMDB/JSON/Franquias_{name_genre}/{caminho_data}/franquias_{name_genre.lower()}_{cont_arquivo:03d}.json'

            s3.put_object( # Código de upload para o Bucket
                Bucket=bucket_name,
                Key=nome_arquivo,
                Body=json.dumps(batch, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )

            cont_arquivo += 1 # Incremento contagem de nome de arquivo
            for arquivo in range(cont_arquivo):
                print(f"Arquivo enviado para: s3://{bucket_name}/{nome_arquivo}")
            

        print(f"Filmes com coleção no gênero {name_genre}: {cont_filmes}")
    print(f"Total de filmes com coleção encontrados: {cont_filmes_total}")

# Função de divisão dos filmes de guerra
def processar_filmes_guerra():
    hoje = datetime.now() # Data para o caminho
    caminho_data = f"{hoje.year}/{hoje.month:02}/{hoje.day:02}" # Caminho com a data Organizada

    war_genre_id = 10752 # Id do Gênero Guerra 
    war_movies_data = [] # Lista para os dados de filmes de guerra
    cont_filmes_total = 0 # Contagem do total de filmes
    batch_tamanho = 100 # Tamanho estipulado para o arquivo JSON
    cont_arquivo = 1 # Contagem do n° de arquivos
    request_count = 0  # Contagem de requisições
    max_pages = 15 # Máximo de páginas para busca

# Estrutura para busca dos dados com o gênero, ordenado por popularidade, separando por páginas
    for page in range(1, max_pages + 1): 
        movies = discover.discover_movies({
            'with_genres': war_genre_id,
            'sort_by': 'popularity.desc',
            'page': page
        })


        for f in movies: # Para cada item na lista
            try:
                movie_details = movie.details(f.id) # Requisitando detalhes dos filmes
                sleep(0.1) # Intervalo
                keywords_obj = movie.keywords(f.id) # Requisitando as palavras chaves referente aos filmes
                keywords_list = [keyword.name for keyword in keywords_obj.keywords] if hasattr(keywords_obj, 'keywords') else [] #Lista para as palavras chave

                war_movies_data.append({ # Inclusão das informações do filme
                    "movie_id": movie_details.id,
                    "title": movie_details.title,
                    "overview": movie_details.overview,
                    "release_date": movie_details.release_date,
                    "popularity": movie_details.popularity,
                    "vote_average": movie_details.vote_average,
                    "vote_count": movie_details.vote_count,
                    "keywords": keywords_list
                })

                request_count += 1 # Incremento da contagem de requisições
                if request_count >= 30: # Implantando intervalo e esvaziando a contagem de requisições ao chegar no limite
                    sleep(2.0) # Intervalo
                    request_count = 0

                if len(war_movies_data) == batch_tamanho:  # Estrutura para criação de arquivo com limite de registros
                    nome_arquivo = f'Raw/TMDB/JSON/War_Movies/{caminho_data}/filmes_guerra_{cont_arquivo:03d}.json'
                    s3.put_object( # Código de upload para o Bucket
                        Bucket=bucket_name,
                        Key=nome_arquivo,
                        Body=json.dumps(war_movies_data, ensure_ascii=False).encode('utf-8')
                    )
                    war_movies_data = [] # Esvaziando lista para novo arquivo
                    cont_arquivo += 1 # Incremento contagem de nome de arquivo
                    
                cont_filmes_total += 1 # Incremento do total de filmes

            except Exception as e:
                print(f"Erro ao processar filme ID {f.id}: {e}") # Exceção para leitura

    if war_movies_data: # Criação do arquivo caso sobrem dados com menos que 100 registros
        nome_arquivo = f'Raw/TMDB/JSON/War_Movies/{caminho_data}/filmes_guerra_{cont_arquivo:03d}.json'
        s3.put_object( # Código de upload para o Bucket
            Bucket=bucket_name,
            Key=nome_arquivo,
            Body=json.dumps(war_movies_data, ensure_ascii=False).encode('utf-8')
        )
    print(f"Total de filmes encontrados: {cont_filmes_total}")

def processar_filmes_crime():
    hoje = datetime.now() # Data para o caminho
    caminho_data = f"{hoje.year}/{hoje.month:02}/{hoje.day:02}" # Caminho com a data Organizada

    crime_genre_id = 80 # Id do Gênero Crime
    crime_movies_data = [] # Lista para os dados de filmes de guerra
    cont_filmes_total = 0 # Contagem do total de filmes
    batch_tamanho = 100 # Tamanho estipulado para o arquivo JSON
    max_pages = 15 # Máximo de páginas para busca
    cont_arquivo = 1 # Contagem do n° de arquivos para o nome
    request_count = 0 # Contagem de requisições

    # Estrutura para busca dos dados com o gênero, ordenado por popularidade, separando por páginas
    for page in range(1, max_pages + 1):
        movies = discover.discover_movies({
            'with_genres': crime_genre_id,
            'sort_by': 'popularity.desc',
            'page': page
        })

        for f in movies: # Para cada item na lista
            try:
                movie_details = movie.details(f.id) # Requisitando detalhes dos filmes
                sleep(0.4) # Intervalo
                keywords_obj = movie.keywords(f.id) # Requisitando as palavras chaves referente aos filmes
                sleep(0.4) # Intervalo
                keywords_list = [keyword.name for keyword in keywords_obj.keywords] if hasattr(keywords_obj, 'keywords') else [] #Lista para as palavras chave

                crime_movies_data.append({ # Inclusão das informações do filme
                    "movie_id": movie_details.id,
                    "title": movie_details.title,
                    "overview": movie_details.overview,
                    "release_date": movie_details.release_date,
                    "popularity": movie_details.popularity,
                    "vote_average": movie_details.vote_average,
                    "vote_count": movie_details.vote_count,
                    "keywords": keywords_list
                })

                request_count += 1 # Incremento da contagem de requisições
                if request_count >= 30: # Implantando intervalo e esvaziando a contagem de requisições ao chegar no limite
                    sleep(2.5) # Intervalo
                    request_count = 0

                if len(crime_movies_data) == batch_tamanho: # Estrutura para criação de arquivo com limite de registros
                    nome_arquivo = f'Raw/TMDB/JSON/Crime_Movies/{caminho_data}/filmes_crime_{cont_arquivo:03d}.json'
                    s3.put_object(  # Código de upload para o Bucket
                        Bucket=bucket_name,
                        Key=nome_arquivo,
                        Body=json.dumps(crime_movies_data, ensure_ascii=False).encode('utf-8')
                    )
                    crime_movies_data = [] # Esvaziando lista para novo arquivo
                    cont_arquivo += 1  # Incremento contagem de nome de arquivo

                cont_filmes_total += 1 # Incremento do total de filmes

            except Exception as e:
                print(f"Erro ao processar filme ID {f.id}: {e}") # Exceção para leitura

    if crime_movies_data: # Criação do arquivo caso sobrem dados com menos que 100 registros
        nome_arquivo = f'Raw/TMDB/JSON/Crime_Movies/{caminho_data}/filmes_crime_{cont_arquivo:03d}.json'
        s3.put_object( # Código de upload para o Bucket
            Bucket=bucket_name,
            Key=nome_arquivo,
            Body=json.dumps(crime_movies_data, ensure_ascii=False).encode('utf-8')
        )
    print(f"Total de filmes encontrados: {cont_filmes_total}")

# Finalizado o desafio