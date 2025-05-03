with open('number.txt', 'r') as arquivo:
    numeros = arquivo.readlines()
    
numeros = list(map(lambda x:int(x.strip()), numeros))

pares = list(filter(lambda x: x % 2 == 0, numeros))

ordenacao_pares = sorted(pares, reverse=True)

pares_maiores = ordenacao_pares[:5]

soma_maiores = sum(pares_maiores)

# Exibindo os resultados
print(pares_maiores)
print(soma_maiores)