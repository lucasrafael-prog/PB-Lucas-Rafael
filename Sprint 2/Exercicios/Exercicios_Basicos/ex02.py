palavras = ['maça', 'arara', 'audio', 'radio', 'radar', 'moto'] 
palavras_palindromas = []# Lista para armazenar as palavras palíndromas

for palavra in palavras:
    if palavra == palavra[::-1]:# Verifica se a palavra é um palíndromo
        palavras_palindromas.append(palavra)# Acrescenta a palavra palíndroma na lista
        print(f'A palavra: {palavra} é um palíndromo')
    else:
        print(f'A palavra: {palavra} não é um palíndromo')