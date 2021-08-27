# api-python-cpp

This project contains the source code and compiling method for DolphinDB Python API. If you would like to know about how to use DolphinDB Python API, please refer to [DolphinDB Python API](../../../api_python3)

## C++ API
To compile DolphinDB API, please refer to [api-cplusplus](../../../api-cplusplus)

#### Linux

copy libDolphinDBAPI.a to api-python-cpp/pickleAPI/bin/linux_x64/python3.X/

#### Windows

copy libDolphinDBAPI.dll to api-python-cpp/pickleAPI/bin/linux_x64/python3.X/

## Install procesure

* git submodule update --init --recursive

* python setup.py develop --user


