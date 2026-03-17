.. These are examples of badges you might want to add to your README:
   please update the URLs accordingly

    .. image:: https://api.cirrus-ci.com/github/<USER>/qa4sm-api.svg?branch=main
        :alt: Built Status
        :target: https://cirrus-ci.com/github/<USER>/qa4sm-api
    .. image:: https://readthedocs.org/projects/qa4sm-api/badge/?version=latest
        :alt: ReadTheDocs
        :target: https://qa4sm-api.readthedocs.io/en/stable/
    .. image:: https://img.shields.io/coveralls/github/<USER>/qa4sm-api/main.svg
        :alt: Coveralls
        :target: https://coveralls.io/r/<USER>/qa4sm-api
    .. image:: https://img.shields.io/pypi/v/qa4sm-api.svg
        :alt: PyPI-Server
        :target: https://pypi.org/project/qa4sm-api/
    .. image:: https://img.shields.io/conda/vn/conda-forge/qa4sm-api.svg
        :alt: Conda-Forge
        :target: https://anaconda.org/conda-forge/qa4sm-api
    .. image:: https://pepy.tech/badge/qa4sm-api/month
        :alt: Monthly Downloads
        :target: https://pepy.tech/project/qa4sm-api
    .. image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Twitter
        :alt: Twitter
        :target: https://twitter.com/qa4sm-api

.. image:: https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold
    :alt: Project generated with PyScaffold
    :target: https://pyscaffold.org/

|

=========
qa4sm-api
=========


    Add a short description here!


Environment variables

- QA4SM_DOTRC_PATH: Path to the dotrc file that contains the API URL and token.


A longer description of your project goes here...


"""
qa4sm list datasets
qa4sm list all
qa4sm list versions --dataset C3S_combined
qa4sm list variables --dataset C3S_combined --version v202505
qa4sm list filters --dataset C3S_combined --version v202505
qa4sm list period --dataset C3S_combined --version v202505

qa4sm status
qa4sm login --username preimesberger --password
qa4sm status run 9aeb663b-e24e-4541-8331-6ec3e0318d1f

qa4sm download results 9aeb663b-e24e-4541-8331-6ec3e0318d1f
qa4sm download config 9aeb663b-e24e-4541-8331-6ec3e0318d1f

qa4sm validate ./config.json --override period=2020-12-31


.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.6. For details and usage
information on PyScaffold see https://pyscaffold.org/.
