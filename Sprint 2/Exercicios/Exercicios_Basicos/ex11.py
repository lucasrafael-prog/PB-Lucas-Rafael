def dividir_lista(lista, tamanho):
    #Dividindo uma lista em três partes iguais
    tamanho = len(lista) // 3
    parte1 = lista[:tamanho]
    parte2 = lista[tamanho:tamanho*2]
    parte3 = lista[tamanho*2:tamanho*3]
    print(f'{parte1} {parte2} {parte3}')

dividir_lista(lista=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], tamanho=4)