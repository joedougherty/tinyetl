from tinyetl import TinyETL

etl = TinyETL('daily_gas_prices', True)

@etl.task
def say_hi(name):
    print("hello {}".format(name))

@etl.task
def trivial():
    return True

if __name__ == '__main__':
    trivial()
    say_hi('world')
