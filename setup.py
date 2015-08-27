__author__ = 'dmarkey'

from setuptools import setup

setup(name='comet-processor',
      version='0.1',
      description='Comet processor base class',
      url='http://github.com/dmarkey/comet-processor',
      author='David Markey',
      author_email='david@dmarkey.com',
      license='MIT',
      packages=['comet_processor'],
      zip_safe=False,
      install_requires=['redis'])