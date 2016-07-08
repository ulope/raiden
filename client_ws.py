


from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport
from tinyrpc import RPCClient
from tinyrpc.client import RPCProxy

rpc_client = RPCClient(
    JSONRPCProtocol(),
    HttpPostClientTransport('http://127.0.0.1:1337/')
)

api = rpc_client.get_proxy()
proxy = RPCProxy(rpc_client, prefix='raiden.api.', one_way=False)
# prefix = api.__dict__['prefix']
# print(prefix1)
# call a method called 'reverse_string' with a single string argument
assets = proxy.get_assets()
partner = proxy.get_partner_addresses()
partner_asset = proxy.get_partner_addresses(assets[0])
print assets, partner, partner_asset"
# result = proxy.transfer('asset_add"ress', 10, 'target')

# result = rpc_client.call('raiden.api.transfer', ['asset_address', 10, 'target'])
