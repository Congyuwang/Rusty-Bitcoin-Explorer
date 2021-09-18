#!/bin/bash

pydocstyle bitcoinquery/
pdoc -o ./docs/ -d google ./bitcoinquery
