from functools import reduce

def calcula_saldo(lancamentos) -> float:
    valores = map(lambda i : -i[0] if i[1] == 'D' else i[0], lancamentos)
    
    saldo = reduce(lambda c, d: c + d, valores)
    
    return saldo
    
print(calcula_saldo(lancamentos=[(10,'D'),
        (300,'C'),
        (20,'C'),
        (80,'D'),
        (300,'D')
    ]))