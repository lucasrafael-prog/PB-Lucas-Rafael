import json

with open("person.json" , "r") as arquivo:
    dados = json.load(arquivo)
    
print(dados)