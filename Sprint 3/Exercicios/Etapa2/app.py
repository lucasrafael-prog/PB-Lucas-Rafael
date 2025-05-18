import hashlib
while True:
    texto = input(str("Digite um texto: "))

    string_to_hash = texto

    hash_item = hashlib.sha1(string_to_hash.encode('utf-8'))

    hash_hexa = hash_item.hexdigest()

    print(hash_hexa)