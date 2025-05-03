class Passaro:
    def voar(self):
        print("Voando...")

    def emitirsom(self):
        pass

class Pato(Passaro):
    def emitirsom(self):
        super().emitirsom()
        print("Pato emitindo som...")
        print("Quack Quack")

class Pardal(Passaro):
    def emitirsom(self):
        super().emitirsom()
        print("Pardal emitindo som...")
        print("Piu Piu")

print("Pato")
pato = Pato()
pato.voar()
pato.emitirsom()

print("Pardal")
pardal = Pardal()
pardal.voar()
pardal.emitirsom()
