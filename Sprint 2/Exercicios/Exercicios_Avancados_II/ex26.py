def pares_ate(n:int):
    for i in range(2, n+1):
        if i % 2 == 0:
            yield i


if __name__ == '__main__':
    generator = pares_ate(n=80)
    print(generator)
    while True:
        try:
            print(next(generator))
        except StopIteration:
            break