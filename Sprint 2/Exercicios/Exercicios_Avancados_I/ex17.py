class Calculo:
    def soma(self, x, y):
        return x + y
    
    def subtracao(self, x, y):
        return x - y
    
    
x = 4
y = 5
calculo = Calculo()
print(calculo.soma(x, y))
print(calculo.subtracao(x, y))