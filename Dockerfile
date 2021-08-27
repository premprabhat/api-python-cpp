FROM quay.io/pypa/manylinux2010_x86_64:latest
RUN yum install -y atlas-devel
RUN yum install -y cmake
RUN yum install -y gcc
RUN yum install -y libuuid
RUN ln -s /lib64/libuuid.so.1 /lib64/libuuid.so
RUN yum install -y openssl
RUN yum install -y openssl-devel
#RUN for PYBIN in /opt/python/*/bin; do "${PYBIN}/pip" install numpy; "${PYBIN}/pip" install pandas; done
