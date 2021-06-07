#!/bin/bash

# Build Demo Application
fpc -g -gh -dDEBUG -FEbin/ -FUtmp/ -Fusrc/ -Fulibs/synapse/source/lib/ src/$1.pas
#echo $0
#mv -f src/$1 bin/$1

#rm -rf src/*.o
#rm -rf src/*.ppu
