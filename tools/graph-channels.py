#!/usr/bin/env python
import json
from collections import namedtuple
from itertools import groupby
from operator import attrgetter

import click
import gevent
import requests
from gevent import monkey

API_URL_ADDRESS = "http://{}/api/1/address"
API_URL_CHANNELS = "http://{}/api/1/channels"


Channel = namedtuple("Channel", ('balance', 'source', 'target', 'state', 'token_address'))


def get_url(url, node, auth, selector=None):
    kw = {}
    if auth:
        kw['auth'] = auth
    json = requests.get(url.format(node), **kw).json()
    if selector:
        json = json[selector]
    return json, node


def mk_graph(channels, token_address):
    graph = ["digraph g {"]
    for channel in channels:
        graph.append(
            '''"{c.source}" -> "{c.target}" [label="{c.balance}, {c.state}"]'''.format(c=channel))
    graph.append('labelloc="t";\nlabel="Token Address: {}";'.format(token_address))
    graph.append("}")
    return "\n".join(graph)


def mk_d3(channels, token_address):
    channels = list(channels)
    return {
        'nodes': [
            {'id': node}
            for node in set(c.source for c in channels) | set(c.target for c in channels)
        ],
        'links': [
            {'source': c.source, 'target': c.target, 'value': c.balance}
            for c in channels
        ]
    }


@click.command(context_settings={'auto_envvar_prefix': "GRAPH_CHANNELS"})
@click.option("-f", "--first-node", default=1, show_default=True)
@click.option("-l", "--last-node", default=100, show_default=True)
@click.option("-s", "--step", default=1, show_default=True)
@click.option("-d", "--domain", default="raidentestnet.ulo.pe", show_default=True)
@click.option("-h", "--host-format", default="raiden-{:04d}", show_default=True)
@click.option("-a", "--auth")
@click.option("-o", "--output", type=click.Choice(['d3', 'dot']), default='d3', show_default=True)
def main(first_node, last_node, step, domain, host_format, auth, output):
    if auth is not None:
        auth = tuple(auth.split(":", 1))
    nodes = ["{}.{}".format(host_format.format(n), domain)
             for n in range(first_node, last_node + 1, step)]

    print("Fetching addresses")
    addr_getters = [
        gevent.spawn(get_url, API_URL_ADDRESS, node, auth, 'our_address')
        for node in nodes
    ]
    gevent.joinall(addr_getters)
    address_to_node = {getter.value[0]: getter.value[1] for getter in addr_getters}

    print("Fetching channels")
    channel_getters = [gevent.spawn(get_url, API_URL_CHANNELS, node, auth) for node in nodes]
    gevent.joinall(channel_getters)
    channels = [
        Channel(
            c['balance'],
            getter.value[1].replace(".{}".format(domain), ''),
            address_to_node.get(
                c['partner_address'], c['partner_address']).replace(".{}".format(domain), ''),
            c['state'],
            c['token_address']
        )
        for getter in channel_getters
        for c in getter.value[0]
    ]
    for token_address, channels in groupby(channels, attrgetter('token_address')):
        suffix = 'dot' if output == 'dot' else 'json'
        file_name = "network_{}.{}".format(token_address, suffix)
        with open(file_name, 'w') as f:
            if output == 'dot':
                f.write(mk_graph(channels, token_address))
            elif output == 'd3':
                json.dump(mk_d3(channels, token_address), f, indent=2)
            print("Generated {}".format(file_name))


if __name__ == "__main__":
    monkey.patch_all()
    main()
