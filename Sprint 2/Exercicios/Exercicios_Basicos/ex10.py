def somar(numeros):
    soma = 0
    for numero in numeros:
        soma += numero
    return soma

numeros_str= "1,3,4,6,10,76"
numeros=[int(i) for i in numeros_str.split(",")]
print(somar(numeros))