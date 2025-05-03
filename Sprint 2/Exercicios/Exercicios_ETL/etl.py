# Primeira parte - Extração
def extrair_dados(arquivo_csv):
    with open(arquivo_csv, 'r', encoding='utf-8', newline='') as arquivo:
        linhas = arquivo.readlines()
        cabecalho = linhas[0].strip().split(',')
        # dados = [linhas[i].strip().split(',') for i in range(1, len(linhas))]
        # Para resolver o problema da vírgula no nome do ator, vamos dividir corretamente os dados
        dados = []
        for linha in linhas[1:]:
            linha = linha.strip()
        # Se o nome do ator tiver vírgula e estiver entre aspas duplas, vamos separar e dividir corretamente os dados.
            if linha.startswith('"'):
                fecha_aspas = linha.find('"', 1)
                nome = linha[1:fecha_aspas]
                restante = linha[fecha_aspas+2:]
                campos = [nome] + restante.split(',')
            else:
                campos = linha.split(',')

            dados.append([campo.strip() for campo in campos])
            
    return cabecalho, dados


cabecalho, dados = extrair_dados('Exercicios_ETL/actors.csv')
atores = [linha[0] for linha in dados]
renda_bruta_filmes_ator = [float(linha[1]) for linha in dados]
num_filmes = [linha[2] for linha in dados]
bilheteria_por_filme = [float(linha[3]) for linha in dados]
filme_principal = [linha[4] for linha in dados]
renda_total_filme = [float(linha[5]) for linha in dados]

# Segunda parte - Transformação

# Etapa 1 - Apresente o ator/atriz com maior número de filmes e a respectiva quantidade.
# A quantidade de filmes encontra-se na coluna Number of movies do arquivo.
def etapa_1(atores, num_filmes):
    maior_numero_filmes = max(num_filmes)
    resultado = f'O ator que mais possui filmes no dataset é {atores[num_filmes.index(maior_numero_filmes)]} com {maior_numero_filmes} filmes.'
    return resultado
    # Etapa 2 - Apresente a média de receita de bilheteria bruta dos principais filmes, considerando todos os atores.
    # Estamos falando aqui da média da coluna Gross.

def etapa_2(renda_total_filme):
    media_renda_total_filme = round(sum(renda_total_filme) / len(renda_total_filme), 2)
    resultado = f'A média de receita de bilheteria bruta dos principais filmes é de {media_renda_total_filme} milhões de dólares.'
    return resultado


# Etapa 3 - Apresente o ator/atriz com a maior média de receita de bilheteria bruta por filme do conjunto de dados.
# Considere a coluna Average per Movie para fins de cálculo.

def etapa_3(bilheteria_por_filme):
    maior_media_bilheteria_filme = max(bilheteria_por_filme)
    resultado = f'O ator com a maior média de receita de bilheteria bruta por filme é {atores[bilheteria_por_filme.index(maior_media_bilheteria_filme)]} \
com a média de {maior_media_bilheteria_filme} milhões de dólares.'
    return resultado



# Etapa 4 - A coluna #1 Movie contém o filme de maior bilheteria em que o ator atuou.
# Realize a contagem de aparições destes filmes no dataset, listando-os ordenados pela quantidade de vezes em que estão presentes.
# Considere a ordem decrescente e, em segundo nível, o nome do filme.
# Ao escrever no arquivo, considere o padrão de saída (sequencia) - O filme (nome filme) aparece (quantidade) vez(es) no dataset, adicionando um resultado a cada linha.
def etapa_4(filme_principal):
    contagem_filmes = {}

    for filme in filme_principal:
        if filme in contagem_filmes: 
            contagem_filmes[filme] += 1 
        else:
            contagem_filmes[filme] = 1
    

    lista_contagem = list(contagem_filmes.items())
    lista_contagem.sort(key=lambda item: (-item[1], item[0]))

    #resultado = [f'O filme {filme} aparece {qtd} vez(es) no dataset.\n' for filme, qtd in lista_contagem]
    resultado = '\n'.join([f'O filme {filme} aparece {qtd} vez(es) no dataset.' for filme, qtd in lista_contagem])
    return resultado



# Etapa 5 - Apresente a lista dos atores ordenada pela receita bruta de bilheteria de seus filmes (coluna Total Gross), em ordem decrescente.
# Ao escrever no arquivo, considere o padrão de saída (nome do ator) - (receita total bruta), adicionando um resultado a cada linha.
def etapa_5(atores, renda_bruta_filmes_ator):
    lista_atores = [(atores[i], renda_bruta_filmes_ator[i]) for i in range(len(atores))]
    lista_atores.sort(key=lambda x: x[1], reverse=True)

    #resultado = [f'{ator} - {renda} milhões de dólares.\n' for ator, renda in lista_atores]
    resultado = '\n'.join([f'{ator} - {renda} milhões de dólares.' for ator, renda in lista_atores])
    return resultado


# Terceira parte - Load(Carregamento)
def salvar_dados(nome_arquivo, texto):
    with open(nome_arquivo, 'w', encoding='utf-8') as arquivo:
        arquivo.write(texto)



resultado1 = etapa_1(atores, num_filmes)
salvar_dados('Exercicios_ETL\etapa-1.txt', resultado1)

resultado2 = etapa_2(renda_total_filme)
salvar_dados('Exercicios_ETL\etapa-2.txt', resultado2)

resultado3 = etapa_3(bilheteria_por_filme)
salvar_dados('Exercicios_ETL\etapa-3.txt', resultado3)

resultado4 = etapa_4(filme_principal)
salvar_dados('Exercicios_ETL\etapa-4.txt', resultado4)

resultado5 = etapa_5(atores,renda_bruta_filmes_ator)
salvar_dados('Exercicios_ETL\etapa-5.txt', resultado5)
