def parametros(*args, **kwargs):
    for i in args:
        print(i)
    for k in kwargs:
        print(kwargs[k])
    
# Teste da função
parametros(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)