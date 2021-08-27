#!/bin/bash
rm -rf build/*
rm -rf wheel/*
docker run --rm -e PLAT=manylinux2010_x86_64 -v `pwd`:/io dev-ssl /io/build-wheels.sh
