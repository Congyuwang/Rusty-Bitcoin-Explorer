#!/bin/bash

pydocstyle bitcoinquery/
pdoc -o ./docs/html/ -d google ./bitcoinquery
