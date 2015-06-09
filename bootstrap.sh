#!/bin/bash

./cleanup.sh

export AUTOM4TE=autom4te2.6x
autoreconf2.6x -ivf
./configure
make dist
