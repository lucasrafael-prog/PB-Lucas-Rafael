class Ordenadora:
    def __init__(self, listaBaguncada):
        self.listaBaguncada = listaBaguncada
        
    def ordenacaoCrescente(self):
        self.listaBaguncada.sort()
        return self.listaBaguncada
        
    def ordenacaoDecrescente(self):
        self.listaBaguncada.sort(reverse=True)
        return self.listaBaguncada

crescente = Ordenadora(listaBaguncada=[3,4,2,1,5])

decrescente = Ordenadora(listaBaguncada=[9,7,6,8])

print(crescente.ordenacaoCrescente())

print(decrescente.ordenacaoDecrescente())