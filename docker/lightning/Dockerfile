# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

# build instruction:
# docker build -t dockername --file=/path/to/lightning/docker/lightning/Dockerfile /path/to/lightning

FROM python:3.11-buster
USER root

# Install necessary dependencies

RUN apt-get update --fix-missing -qy

RUN apt-get install -qy --no-install-recommends wget \
  build-essential \
  cmake \
  zlib1g-dev \
  libbz2-dev \
  liblzma-dev \
  libncurses5-dev \
  libncursesw5-dev \
  gcc \
  make \
  git

RUN pip install --upgrade pip matplotlib numpy pandas scikit-learn scipy

RUN wget --progress=dot:giga https://go.dev/dl/go1.20.10.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.20.10.linux-amd64.tar.gz

ENV PATH $PATH:/usr/local/go/bin:/root/go/bin

# Getting lightning

COPY . /lightning
RUN cd /lightning && make && cd / && rm -rf lightning
