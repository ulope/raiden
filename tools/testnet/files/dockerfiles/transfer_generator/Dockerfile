FROM brainbot/raiden

MAINTAINER Ulrich Petri <ulrich@brainbot.com>

ADD transfer_generator.py /usr/local/bin/transfer_generator.py
ADD scenario-testnet-token.yml /usr/share

ENTRYPOINT ["/usr/local/bin/transfer_generator.py"]
