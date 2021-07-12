# from https://github.com/Gradiant/dockerized-spark/blob/e57182e3f7c0ca1043062097ea375406ad1b9a51/alpine/Dockerfile.python
# and https://towardsdatascience.com/apache-spark-cluster-on-docker-ft-a-juyterlab-interface-418383c95445
ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:8-alpine3.9

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apk update && \
    apk add python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    apk update

## Adding apache arrow build dependencies
RUN apk add --no-cache build-base bash cmake boost-dev py3-numpy py-numpy-dev python3-dev autoconf zlib-dev flex bison
RUN pip3 install cython pandas wheel
## Downloading and building arrow source
RUN wget -qO- https://github.com/apache/arrow/archive/apache-arrow-4.0.1.tar.gz | tar xvz -C /opt
RUN ln -s /opt/arrow-apache-arrow-4.0.1 /opt/arrow

ENV ARROW_BUILD_TYPE=release \
    ARROW_HOME=/opt/dist \
    PARQUET_HOME=/opt/dist

RUN /bin/bash
RUN mkdir /opt/dist
RUN mkdir -p /opt/arrow/cpp/build
RUN cd /opt/arrow/cpp/build  && \
    cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_PARQUET=on \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_BUILD_TESTS=OFF \
      -DPYTHON_EXECUTABLE=/usr/bin/python3 \
      .. && \
    make -j4 && \
    make install

#build pyarrow
RUN cp -r /opt/dist/* /
RUN cd /opt/arrow/python && python3 setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
       --with-parquet --with-plasma --inplace --bundle-arrow-cpp bdist_wheel

RUN cd /opt/arrow/python/dist/ && \
    pip3 install pyarrow-4.0.1-cp36-cp36m-linux_x86_64.whl
#RUN mkdir -p /opt/dist/python && cp /opt/arrow/python/dist/* /opt/dist/python

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]