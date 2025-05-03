def my_map(list, f):
    novalista = []
    for item in list:
        novalista.append(f(item))
    return novalista

list=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

novalista = my_map(list, lambda x: x ** 2)
print(novalista)