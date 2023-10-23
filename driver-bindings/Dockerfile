FROM alpine:3.16

ENV THRIFT_VERSION v0.19.0

RUN buildDeps=" \
    automake \
    bison \
    curl \
    flex \
    g++ \
    boost-dev \
    libressl-dev \
    libevent-dev \
    libtool \
    make \
    pkgconf \
    autoconf \
  "; \
  apk update && apk add --no-cache $buildDeps \
  && curl -k -sSL "https://github.com/apache/thrift/archive/${THRIFT_VERSION}.tar.gz" -o thrift.tar.gz \
  && mkdir -p /usr/src/thrift \
  && tar zxf thrift.tar.gz -C /usr/src/thrift --strip-components=1 \
  && rm thrift.tar.gz \
  && cd /usr/src/thrift \
  && ./bootstrap.sh \
  && ./configure --disable-libs \
  && make \
  && make install \
  && cd / \
  && rm -rf /usr/src/thrift \
  && apk del --purge $buildDeps \
  && apk add --no-cache libstdc++

CMD [ "thrift" ]