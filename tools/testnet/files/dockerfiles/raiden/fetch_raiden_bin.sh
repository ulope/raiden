#!/usr/bin/env bash

set -xe

if [[ ${RAIDEN_BIN_URL} == *txt ]]; then
    # Text file pointing to latest release
    RAIDEN_BIN_URL=$(dirname ${RAIDEN_BIN_URL})/$(curl ${RAIDEN_BIN_URL})
fi

curl -o /tmp/raiden.tar.gz ${RAIDEN_BIN_URL}
tar -C /tmp -xzf /tmp/raiden.tar.gz
mv /tmp/raiden-*-linux /usr/local/bin/raiden
rm /tmp/raiden*
