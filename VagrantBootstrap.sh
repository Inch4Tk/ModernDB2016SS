#!/usr/bin/env bash

apt-get update
apt-get install -y build-essential
apt-get install -y checkinstall
apt-get install -y valgrind

wget https://cmake.org/files/v3.5/cmake-3.5.2.tar.gz
tar xf cmake-3.5.2.tar.gz
cd cmake-3.5.2
./configure
make
checkinstall