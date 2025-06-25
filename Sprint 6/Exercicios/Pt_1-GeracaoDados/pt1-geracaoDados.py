# Importações
import random
import time
import os
import names

inicio = time.time()

random.seed(40)

qtd_nomes_unicos = 39080

qtd_nomes_aleatorios = 10000000

# Geração de nomes únicos
aux = []

for i in range(0, qtd_nomes_unicos):
    aux.append(names.get_full_name())
    
print(f"Gerando {qtd_nomes_aleatorios} nomes aleatórios...")

# Geração de nomes aleatórios
dados = []

for i in range(0, qtd_nomes_aleatorios):
    dados.append(random.choice(aux))
    
# Salvando os dados em um arquivo
with open("nomes_aleatorios.txt", "w", encoding="utf-8") as arquivo:
    linhas = [f"{nome}\n" for nome in dados]
    arquivo.writelines(linhas)

if os.path.exists("nomes_aleatorios.txt"):
    print(f"Arquivo 'nomes_aleatorios.txt' gerado com {qtd_nomes_aleatorios} nomes aleatórios.")

fim = time.time()
print(f"Tempo de Execução: {fim - inicio:.2f} segundos.")

