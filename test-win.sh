#!/bin/bash
for ver in 27 34 35 36 37; do
    echo "install $ver >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    py=/cygdrive/c/Python$ver/python
    $py -m pip uninstall dolphindb -y
    # $py -m pip install --upgrade pip
    # $py -m pip install numpy
    # $py -m pip install pandas
    $py -m pip install future
    $py -m pip install dolphindb --no-index -f dist
done

# test

for ver in 27 34 35 36 37; do
    echo "test $ver >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    py=/cygdrive/c/Python$ver/python
    $py tests/connection.py
done

