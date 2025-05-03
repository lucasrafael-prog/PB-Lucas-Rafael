a = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
b = [] # Lista vazia para armazenar  números
for i in range(len(a)):
    if a[i] % 2 != 0: # Verifica se o número é ímpar
        b.append(a[i])
        
print(b)