#!/bin/bash
rm -rf build/*
rm -rf wheel/*
docker run --rm --privileged hypriot/qemu-register
docker run --rm -e PLAT=manylinux2014_aarch64 -v `pwd`:/io quay.io/pypa/manylinux2014_aarch64 /io/build-wheels.sh
docker run --rm -e PLAT=manylinux2010_x86_64 -v `pwd`:/io dev-ssl /io/build-wheels.sh
