from fabric.api import task, env
from tinyetl import TinyETL

etl = TinyETL('some_daily_data', env=env)

@task
@etl.log
def say_hi(name):
    """ Greet someone. """
    print("hello {}".format(name))

@task
@etl.log
def trivial():
    """ Nothing, really! """
    return True or True

@task 
@etl.log
def some_third_thing():
    """ Print a message. """
    print("run this last")

@task
def main():
    """ Run the whole ETL job! """
    say_hi('world')
    trivial()
    some_third_thing()

