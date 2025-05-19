import pandas as pd
import matplotlib.pyplot as plt

#Etapa 2 - Processamento de dados

dados = pd.read_csv("volume/csv_limpo.csv")


# Resolução das Questões:

# Q1 - Qual é a artista que mais aparece nessa lista e possui a maior média de seu faturamento bruto (Actual gross)?

artista_mais_aparicoes = dados.value_counts('Artist', ascending=False).head(1) # Contagem da artista com mais aparições

media_artista = dados.groupby('Artist')['Actual gross'].mean() # Cálculo da média de faturamento das artistas
artista_maior_media = media_artista.sort_values(ascending=False).head(1) # Isolando a artista com maior média 

# Retorno da solução:
resposta_q1 = (f'Q1:\n\n--- A artista com mais aparições é {artista_mais_aparicoes.index[0]} com {artista_mais_aparicoes.iloc[0]}, possuindo a média de faturamento bruto de ${artista_maior_media.values[0]:.2f}\n\n')


# Q2 - Das turnês que aconteceram dentro de um ano, qual a turnê com a maior média de faturamento bruto (Average gross)?

turne_um_ano = dados[dados['Start year'] == dados['End year']] # Separando turnês de apenas um ano
maior_media_turne = turne_um_ano.sort_values('Average gross', ascending=False).head(1) # Separando a média

# Retorno da solução:
resposta_q2 = (f'Q2:\n\n--- Das turnes que aconteceram em um ano, a turnê com a maior media de faturamento bruto é {maior_media_turne["Tour title"].values[0]} com uma média de ${maior_media_turne["Average gross"].values[0]:.2f}\n\n')


# Q3- Quais são as 3 turnês que possuem o show (unitário) mais lucrativo? Cite também o nome de cada artista e o valor por show. 
# Utilize a coluna "Adjusted gross (in 2022 dollars)". Caso necessário, crie uma coluna nova para essa conta.
dados['Gain per show'] = dados['Adjusted gross (in 2022 dollars)'] / dados['Shows'] # Criando nova coluna com o cálculo da divisão
shows_mais_lucrativos = dados.sort_values('Gain per show', ascending=False).head(3) # Separando os shows com mais lucro

# Retorno da solução:
resposta_q3 = ('Q3:\n\n--- As três turnês que possuem o show mais lucrativo são:\n')
for i, row in shows_mais_lucrativos.iterrows():
    resposta_q3 += (f'- A turnê {row["Tour title"]}, da artista {row["Artist"]} com um lucro por show de ${row["Gain per show"]:.2f}\n')

with open('volume/respostas.txt', 'w', encoding='utf-8') as arquivo:
    arquivo.write(resposta_q1)
    arquivo.write(resposta_q2)
    arquivo.write(resposta_q3)

# Q4 - Para a artista que mais aparece nessa lista e que tenha o maior somatório de faturamento bruto, 
# crie um gráfico de linhas que mostra o faturamento por ano da turnê (use a coluna Start Year). Apenas os anos com turnês.
faturamento_artista = dados.groupby('Artist')['Actual gross'].sum() # Somando o faturamento das artistas
artista_maior_faturamento = faturamento_artista.sort_values(ascending=False).head(1) # Separando a artista com maior faturamento
dados_artista = dados[dados['Artist'] == 'Taylor Swift'] # Dividindo os dados desta artista
faturamento_ano = dados_artista.groupby('Start year')['Actual gross'].sum().sort_index() # Somando o faturamento da artista para implantar no gráfico

# Implantação do gráfico(solução):
plt.figure(figsize=(8,4)) # Tamanho
plt.title('Faturamento por ano das turnês de Taylor Swift') #Título do gráfico
plt.plot(faturamento_ano.index, faturamento_ano.values, color='purple', marker='o') # Plot do gráfico
plt.xlabel('Ano') # Legenda Eixo X
plt.ylabel('Faturamento Bruto ($) em Milhões') # Legenda Eixo Y
plt.ticklabel_format(style='sci', axis='y', scilimits=(6,6)) # Formatação do número eixo Y
plt.tight_layout() # Layout justificado
plt.grid(True) # Ativação da 'grade' no gráfico
plt.savefig('volume/Q4.png') # Salvar figura 


# Q5 - Faça um gráfico de colunas demonstrando as 5 artistas com mais shows na lista.
artistas_mais_shows = dados.groupby('Artist')['Shows'].sum().sort_values(ascending=False).head(5) # Separando artistas com mais shows

#Implantação do gráfico(solução):
fig, ax = plt.subplots(figsize=(8, 5)) # Tamanho
barras = ax.bar(artistas_mais_shows.index, artistas_mais_shows.values, color='orange') # Plot do gráfico com nome de variável
plt.title('Artistas com mais shows') # Título do gráfico
plt.xlabel('Nome das artistas') # Legenda Eixo X
plt.ylabel('Quantidade de Shows') # Legenda Eixo Y
ax.bar_label(barras, label_type='center', color='white', padding=3) # Números das colunas
plt.xticks(rotation=2) # Rotação dos itens Eixo X
plt.tight_layout() # Layout justificado
plt.savefig('volume/Q5.png') # Salvar figura 