go-carbon [![Build Status](https://travis-ci.org/fayizk1/go-carbon.svg?branch=master)](https://travis-ci.org/fayizk1/go-carbon)
============

Golang implementation of Graphite/Carbon server with classic architecture: Agent -> Cache -> Persister

![Architecture](doc/design.png)

### Features
* Receive metrics from TCP and UDP ([plaintext protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol))
* Stores in leveldb
