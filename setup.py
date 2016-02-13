try:
    from setuptools import setup
except ImportError:
    from distuils.core import setup

setup(
        name="tinyetl",
        version='0.0.1',
        description="Just a tiny bit of ETL",
        author="Joe Dougherty",
        author_email="joseph.dougherty@gmail.com",
        packages=['tinyetl'],
        install_requires=['fabric'],
        )
