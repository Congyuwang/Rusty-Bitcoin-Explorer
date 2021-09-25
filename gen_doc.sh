#!/bin/bash

pydocstyle bitcoinquery/
pdoc -o ./docs/ -d google ./bitcoin_explorer
