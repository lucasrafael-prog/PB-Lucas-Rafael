import random

def gerar_lista_aleatoria(tamanho):
    lista_aleatoria = []
    for i in range(tamanho):
        lista_aleatoria.append(random.randint(0, 1000))
    lista_aleatoria.reverse()
    return lista_aleatoria
    
print(gerar_lista_aleatoria(250))


def escrever_lista_animais(lista_animais):
    lista_animais = ['Cachorro', 'Gato', 'Elefante', 'Leão', 'Urso', 'Zebra', 'Hipopótamo', 'Girafa', 'Tigre', 'Coelho', 'Jacaré', 'Cavalo', 'Macaco', 'Peixe', 'Pássaro', 'Raposa', 'Cobra', 'Vaca', 'Baleia', 'Lobo']
    lista_animais.sort()
    [print(animal) for animal in lista_animais]
    with open('animais.txt', 'w', encoding='utf-8') as arquivo:
        linhas = [f"{animal}\n" for animal in lista_animais]
        arquivo.writelines(linhas)

print(escrever_lista_animais([]))