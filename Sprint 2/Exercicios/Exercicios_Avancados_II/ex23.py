def calcular_valor_maximo(operadores,operandos) -> float:
    expressoes = [f'{operandos[0]} {operadores[0]} {operandos[1]}' for operadores, operandos in zip(operadores, operandos)]
    resultados = list(map(eval, expressoes))
    return max(resultados)
print(calcular_valor_maximo(operadores=['-','-','*','/','%'], operandos=[(3,6), (-7,4.9), (8,-8), (10,2), (8,4)]))
print(calcular_valor_maximo(operadores=['-','-','*','/','%', '+','-'], operandos=[(3,6), (-7,4.9), (8,-8), (10,2), (8,4), (82, 6), (-100, 58)]))
