class Pessoa:
    def __init__(self, id):
        self.id = id
        self.__nome = None
    
    @property
    def nome(self):
        return self.__nome
    
    @nome.setter
    def set_nome(self, nome):
        self.__nome = nome
        

pessoa = Pessoa(1)
pessoa.set_nome = "Lucas"
print(pessoa.id, pessoa.nome)