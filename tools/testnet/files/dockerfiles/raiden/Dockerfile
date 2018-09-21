FROM ubuntu:18.04
MAINTAINER Ulrich Petri <ulrich@brainbot.com>

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN \
    apt update && \
    apt install -y curl && \
    rm -rf /var/lib/apt/lists/*


# This is defined down here to avoid rebuilding the "apt" part on raiden version change
# Can either be a direct download URL or a txt file pointing to the latest download
ARG RAIDEN_BIN_URL=https://raiden-nightlies.ams3.digitaloceanspaces.com/_LATEST-linux.txt

ADD fetch_raiden_bin.sh /usr/local/bin/fetch_raiden_bin.sh
RUN /usr/local/bin/fetch_raiden_bin.sh

ENTRYPOINT ["/usr/local/bin/raiden"]
