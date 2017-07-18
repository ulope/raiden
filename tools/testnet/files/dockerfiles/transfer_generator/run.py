#!/usr/bin/env python2
import json

import click
import logging
import requests
from ethereum import slogging
from pyethapp.rpc_client import JSONRPCClient
from requests.exceptions import RequestException

from raiden.network.rpc.client import patch_send_message, patch_send_transaction

log = slogging.getLogger(__name__)


API_URL_ADDRESS = "http://{}/api/1/address"
API_URL_TOKENS = "http://{}/api/1/tokens"
API_URL_TOKEN_PARTNERS = "{}/{{}}/partners".format(API_URL_TOKENS)
API_URL_CHANNELS = "http://{}/api/1/channels"


@click.command()
@click.option("--ensure-supply/--no-ensure-supply", default=True,
              help="Make sure all nodes have enough tokens to continue")
@click.option("--supply", default=500)
@click.option("--host", default="localhost")
@click.option("--port", default=8545)
@click.argument("token-address")
@click.argument("private-key")
@click.argument("nodes", nargs=-1)
def main(token_address, private_key, nodes, ensure_supply, supply, host, port):
    token_address = token_address.lower()

    if not token_address.startswith('0x'):
        token_address = "0x{}".format(token_address)

    if len(token_address) != 42:
        log.error("Invalid token address %s is invalid", token_address)

    client = JSONRPCClient(
        host,
        port,
        print_communication=False,
        privkey=private_key
    )
    patch_send_message(client)
    patch_send_transaction(client)

    with open("token.abi") as f:
        token_ctr = client.new_contract_proxy(json.load(f), token_address)

    addresses = {}
    balances = {}
    txhashes = []
    for node in nodes:
        address = addresses[node] = requests.get(
            API_URL_ADDRESS.format(node)).json()['our_address'].lower()
        balance = balances[address] = token_ctr.balanceOf(address)
        if ensure_supply and balance < supply:
            supply_balance = supply - balance
            log.info("Supplying %d tokens to %s (%s)", supply_balance, node, address)
            txhashes.append(token_ctr.transfer(address, supply_balance))

    while txhashes:
        log.debug("Waiting for transaction confirmations: %d", len(txhashes))
        txhash = txhashes[0]
        try:
            client.poll(txhash.decode('hex'), timeout=3)
            txhashes.remove(txhash)
        except Exception:
            pass

    registered_tokens = [
        t['address'].lower()
        for t in requests.get(API_URL_TOKENS.format(nodes[0])).json()
    ]

    if token_address not in registered_tokens:
        try:
            log.info("Registering token with network")
            url = "{}/{}".format(API_URL_TOKENS.format(nodes[0]), token_address)
            log.debug("url: %s", url)
            resp = requests.put(url)
            code = resp.status_code
            msg = resp.text
        except RequestException as ex:
            code = -1
            msg = str(ex)
        if not 199 < code < 300:
            log.error("Couldn't register token with network: %d %s", code, msg)
            return

    for i, node in enumerate(nodes):
        partner_node = nodes[(i + 1) % len(nodes)]
        partner_address = addresses[node]
        channel_partners = requests.get(API_URL_TOKEN_PARTNERS.format(node, token_address)).json()
        for channel_partner in channel_partners:
            if channel_partner['partner_address'].lower() == partner_address:
                break
        else:
            log.info("Creating channel %s (%s) -> %s (%s)",
                     node, addresses[node], partner_node, partner_address)
            requests.put(
                API_URL_CHANNELS,
                json=dict(
                    partner_address=partner_address,
                    token_address=token_address,
                    balance=balances[addresses[node]]
                )
            )


if __name__ == "__main__":
    slogging.configure(":debug")
    # Fix pyethapp.rpc_client not using slogging library
    rpc_logger = logging.getLogger('pyethapp.rpc_client')
    rpc_logger.setLevel(logging.DEBUG)
    rpc_logger.parent = slogging.getLogger()

    main()
