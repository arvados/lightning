# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

<<<<<<< HEAD
=======
# build instruction:
# docker build -t dockername --file=/path/to/lightning/docker/vcfutil/Dockerfile /path/to/lightning


>>>>>>> 20765-moving-gvcf-regions
FROM python:3.11-buster

USER root

RUN apt-get update -q

RUN apt-get install -qy build-essential wget cmake zlib1g-dev \
    libbz2-dev liblzma-dev libncurses5-dev libncursesw5-dev git vcftools

# Setting up gvcf_region
COPY ./gvcf_regions /gvcf_regions

# Getting HTSLIB 1.9 for tabix/bgzip

RUN wget https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2 && tar -xjvf htslib-1.9.tar.bz2 && \
    cd htslib-1.9 && ./configure && make && make install

WORKDIR /

# Installing samtools 1.9

RUN wget https://github.com/samtools/samtools/releases/download/1.9/samtools-1.9.tar.bz2 && tar -xjvf samtools-1.9.tar.bz2 && \
    cd samtools-1.9 && ./configure && make && make install

WORKDIR /

# Installing bcftools 1.9

RUN wget https://github.com/samtools/bcftools/releases/download/1.9/bcftools-1.9.tar.bz2 && tar -xjvf bcftools-1.9.tar.bz2 && \
    cd bcftools-1.9 && ./configure && make && make install

WORKDIR /

# Installing rtg tools v3.8.4

RUN apt-get install -qy unzip

RUN wget https://github.com/RealTimeGenomics/rtg-tools/releases/download/3.9.1/rtg-tools-3.9.1-linux-x64.zip && \
    unzip rtg-tools-3.9.1-linux-x64.zip && \
    cd rtg-tools-3.9.1 && ./rtg help

ENV PATH="/rtg-tools-3.9.1:${PATH}"

WORKDIR /

# Installing bedops v2.4.35

RUN wget https://github.com/bedops/bedops/releases/download/v2.4.35/bedops_linux_x86_64-v2.4.35.tar.bz2 && \
    tar jxvf bedops_linux_x86_64-v2.4.35.tar.bz2 && \
    cp bin/* /usr/local/bin

WORKDIR /

# Installing bedtools 2.27.1

RUN wget https://github.com/arq5x/bedtools2/releases/download/v2.27.1/bedtools-2.27.1.tar.gz && \
    tar -xzvf bedtools-2.27.1.tar.gz && \
    cd bedtools2 && make && cp bin/* /usr/local/bin
