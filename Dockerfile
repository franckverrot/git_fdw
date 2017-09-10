FROM ubuntu:latest
MAINTAINER Franck Verrot <franck@verrot.fr>

RUN apt-get update -qq && \
    apt-get install -y \
      build-essential \
      flex bison \
      git \
      libc6-dev-i386 \
      libpq-dev \
      postgresql-9.5 \
      postgresql-server-dev-all \
      rake \
      redis-server \
      libgit2-dev
RUN pg_createcluster -p 5433 9.5 my_cluster

WORKDIR /git_fdw
ADD . /git_fdw

RUN make install
