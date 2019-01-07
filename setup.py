#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='target-stitch',
      version='1.8.1',
      description='Singer.io target for the Stitch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_stitch'],
      install_requires=[
          'jsonschema==2.6.0',
          'mock==2.0.0',
          'requests==2.20.0',
          'singer-python==5.4.0',
          'psutil==5.3.1',
	  'boto==2.49.0',
	  'transit-python==0.8.302',
      ],
      extras_require={
          'dev': [
              'nose==1.3.7',
              'pylint==2.1.1',
          ]
      },
      entry_points='''
          [console_scripts]
          target-stitch-internal=target_stitch:main
      ''',
      packages=find_packages()
)
