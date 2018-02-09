FROM ubuntu:16.04

# Metadata
LABEL base.image="ubuntu:16.04"
LABEL software="JTracker CLI"
LABEL website="https://github.com/jthub/jt-cli"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

RUN apt-get -yqq update && \
    apt-get -yqq install curl

RUN curl -sSL -o tmp.tar.gz --retry 10 https://github.com/jthub/jt-cli/archive/0.2.0a6.tar.gz && \
    mkdir -p /tmp/jt-cli && \
    tar -C /tmp/jt-cli --strip-components 1 -zxf tmp.tar.gz && \
    python3 /tmp/jt-cli/setup.py install && \
    rm -rf /tmp/jt-cli

CMD ["/bin/bash"]