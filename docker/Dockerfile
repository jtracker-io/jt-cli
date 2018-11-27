FROM ubuntu:16.04

# Metadata
LABEL base.image="ubuntu:16.04"
LABEL software="JTracker CLI"
LABEL website="https://github.com/jtracker-io/jt-cli"

# File Author / Maintainer
MAINTAINER Junjun Zhang <junjun.ca@gmail.com>

USER root

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get -yqq update && \
    apt-get -yqq install curl && \
    apt-get -yqq install python2.7 && \
    apt-get -yqq install python-pip && \
    pip --quiet install requests

RUN curl -sSL -o tmp.tar.gz --retry 10 https://github.com/jtracker-io/jt-cli/archive/0.2.0a23.tar.gz && \
    mkdir -p /tmp/jt-cli && \
    tar -C /tmp/jt-cli --strip-components 1 -zxf tmp.tar.gz && \
    apt-get -yqq install python3-pip && \
    pip3 install -r /tmp/jt-cli/requirements.txt  && \
    cd /tmp/jt-cli/ && \
    python3 setup.py install && \
    cd && \
    rm -rf /tmp/jt-cli

CMD ["/bin/bash"]
