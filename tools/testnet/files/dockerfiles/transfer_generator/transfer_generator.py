#!/usr/bin/env python2
import random
import uuid
from collections import defaultdict

import click
import logging
import requests
import time

import yaml
from ethereum import slogging, _solidity as solidity
from pyethapp.rpc_client import JSONRPCClient
from raiden.utils import get_contract_path
from requests.exceptions import RequestException

from raiden.network.rpc.client import patch_send_message, patch_send_transaction

log = slogging.getLogger("scenario")


TOKEN_BALANCE_MIN = 5 * 10 ** 4

TIMEOUT = 120

API_URL_ADDRESS = "http://{}/api/1/address"
API_URL_TOKENS = "http://{}/api/1/tokens"
API_URL_CHANNELS = "http://{}/api/1/channels"
API_URL_TRANSFERS = "http://{}/api/1/transfers/{}/{}"
API_URL_CONNECT = "http://{}/api/1/connection/{}"


def wait_for_txs(client, txhashes):
    while txhashes:
        log.debug("Waiting for transaction confirmations", outstanding=len(txhashes))
        txhash = txhashes[0]
        try:
            client.poll(txhash.decode('hex'), timeout=3)
            txhashes.remove(txhash)
        except Exception:
            pass


def get_or_deploy_token(client, scenario):
    """

    :param client:
    :type client: JSONRPCClient
    :param scenario:
    :type scenario:
    :return:
    :rtype:
    """
    contracts = solidity.compile_file(get_contract_path('InfiniToken.sol'))

    address = scenario.get('token', {}).get('address')
    if address:
        token_ctr = client.new_contract_proxy(contracts['InfiniToken.sol:InfiniToken']['abi'],
                                              address)
        log.debug("Reusing token", address=address, name=token_ctr.name(),
                  symbol=token_ctr.symbol())
        return token_ctr

    name = "Testtoken {}".format(uuid.uuid4())
    symbol = "TTT"
    name = scenario.get('token', {}).get('name', name)
    symbol = scenario.get('token', {}).get('symbol', symbol)

    log.debug("Deploying token", name=name, symbol=symbol)

    token = client.deploy_solidity_contract(
        client.sender,
        'InfiniToken',
        contracts,
        [],
        (name, symbol),
        'InfiniToken.sol'
    )
    log.debug("Deployed token", address=token.address.encode('hex'))
    return token


@click.command()
@click.option("--private-key", required=True, envvar="PRIVATEKEY")
@click.option("--host", default="localhost")
@click.option("--port", default=8545)
@click.argument("scenario-file", type=click.File())
@click.argument("raiden-nodes", nargs=-1)
def main(scenario_file, raiden_nodes, private_key, host, port):
    client = JSONRPCClient(
        host,
        port,
        print_communication=False,
        privkey=private_key
    )
    patch_send_message(client)
    patch_send_transaction(client)

    scenario = yaml.safe_load(scenario_file)

    token_ctr = get_or_deploy_token(client, scenario)
    token_address = "0x{}".format(token_ctr.address.encode('hex'))

    node_to_address = {}
    for node in raiden_nodes:
        address = node_to_address[node] = requests.get(
            API_URL_ADDRESS.format(node), timeout=TIMEOUT
        ).json()['our_address'].lower()
        balance = token_ctr.balanceOf(address)
        if balance < TOKEN_BALANCE_MIN:
            log.debug("Minting %d tokens for %s", TOKEN_BALANCE_MIN, address)
            token_ctr.mintFor(TOKEN_BALANCE_MIN, address)

    registered_tokens = [
        t['address'].lower()
        for t in requests.get(API_URL_TOKENS.format(raiden_nodes[0]), timeout=TIMEOUT).json()
    ]

    if token_address not in registered_tokens:
        try:
            log.info("Registering token with network")
            url = "{}/{}".format(API_URL_TOKENS.format(raiden_nodes[0]), token_address)
            log.debug("", url=url)
            resp = requests.put(url, timeout=TIMEOUT)
            code = resp.status_code
            msg = resp.text
        except RequestException as ex:
            code = -1
            msg = str(ex)
        if not 199 < code < 300:
            log.error("Couldn't register token with network: %d %s", code, msg)
            return

    active_nodes = []
    for node in raiden_nodes:
        url = API_URL_CONNECT.format(node, token_address)
        log.info("Connecting to token network", node=node, token=token_address, url=url)
        try:
            resp = requests.put(url, json=dict(funds=TOKEN_BALANCE_MIN / 2), timeout=TIMEOUT)
            code = resp.status_code
            msg = resp.text
            active_nodes.append(node)
        except RequestException as ex:
            code = -1
            msg = str(ex)
        if not 199 < code < 300:
            log.error("Couldn't join token network: %d %s", code, msg)

    log.info("Active nodes", nodes=active_nodes)
    raiden_nodes = active_nodes

    excpected_amounts = defaultdict(int)

    delay_min = scenario.get('delay_min', 2)
    delay_max = scenario.get('delay_min', 20)

    iterations = scenario.get('interations', 0)
    i = 0

    while True:
        for node in raiden_nodes:
            partner_node = None
            while partner_node is None or partner_node == node:
                partner_node = random.choice(raiden_nodes)
            partner_address = node_to_address[partner_node]

            url = API_URL_TRANSFERS.format(node, token_address, partner_address)
            amount = random.randint(1, 5)
            log.info("Transfering", from_=node, to=partner_node, amount=amount)
            excpected_amounts[partner_node] += amount
            try:
                resp = requests.post(url, json=dict(amount=amount), timeout=TIMEOUT)
                code = resp.status_code
                msg = resp.text
            except RequestException as ex:
                code = -1
                msg = str(ex)
            if not 199 < code < 300:
                log.error("Couldn't transfer: %d %s", code, msg)
                continue

            time.sleep(random.randint(delay_min, delay_max))
        i += 1
        if i >= iterations:
            break

    log.info("Expected transfers", amounts=dict(excpected_amounts))


if __name__ == "__main__":
    slogging.PRINT_FORMAT = '%(asctime)s %(levelname)s: %(name)s\t%(message)s'
    slogging.configure(":debug")
    # Fix pyethapp.rpc_client not using slogging library
    rpc_logger = logging.getLogger('pyethapp.rpc_client')
    rpc_logger.setLevel(logging.DEBUG)
    rpc_logger.parent = slogging.getLogger()

    main()
