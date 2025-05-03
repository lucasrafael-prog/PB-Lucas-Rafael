def maiores_que_media(conteudo:dict)->list:
    media = sum(conteudo.values()) / len(conteudo)
    produtos = list(conteudo.keys())
    maiores = list(filter(lambda item: item[1] > media, conteudo.items()))

    return sorted(maiores, key=lambda item: item[1])

print(maiores_que_media(conteudo={
    "arroz": 4.99,
    "feijão": 3.49,
    "macarrão": 2.99,
    "leite": 3.29,
    "pão": 1.99
}))