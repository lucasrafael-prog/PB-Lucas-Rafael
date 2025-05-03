primeirosNomes = ['Joao', 'Douglas', 'Lucas', 'José']
sobreNomes = ['Soares', 'Souza', 'Silveira', 'Pedreira']
idades = [19, 28, 25, 31]

for i, primeirosNomes in(enumerate(primeirosNomes)):
    print(f'{i} - {primeirosNomes} {sobreNomes[i]} está com {idades[i]} anos')