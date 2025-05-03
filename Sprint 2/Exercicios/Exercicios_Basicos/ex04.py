def remover_duplicados(lista):
    resultado = list(set(lista))
    return resultado

print(remover_duplicados(lista=['abc', 'abc', 'abc', '123', 'abc', '123', '123']))