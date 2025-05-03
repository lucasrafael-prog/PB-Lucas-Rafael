class Lampada:
    def __init__(self, ligada=False):
        self.ligada = False
    
    def liga(self):
        self.ligada = True
    
    def desliga(self):
        self.ligada = False
    
    def esta_ligada(self):
        return self.ligada
    
def main():
    lampada = Lampada()
    
    lampada.liga()
    print("A lâmpada está ligada?", lampada.esta_ligada())
    
    lampada.desliga()
    print("A lâmpada está ligada?", lampada.esta_ligada())
    
if __name__ == "__main__":
    main()