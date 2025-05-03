import csv

with open('estudantes.csv', newline='', encoding='utf-8')  as arquivo:
    for linha in sorted(csv.reader(arquivo, delimiter=',')):
        nome = linha[0]
        
        notas = list(map(lambda x: float(x), linha[1:6]))
        
        ordenacao = (sorted(notas, reverse=True))[:3]
        
        media = round(sum(ordenacao) / len(ordenacao), 2)
        
        notas_formatadas = list(map(int, ordenacao)) #Formata as notas da ordenacao para nenhuma casa decimal
        
        print(f'Nome: {nome} Notas: {notas_formatadas} Média: {media}')