from raiden.tests.utils.network import mk_app, create_network
from raiden.app import APIRPCServer

if __name__ == '__main__':

    app_list = create_network(num_nodes=5, num_assets=3, channels_per_node=1)
    app0 = app_list[0]

    rpc = APIRPCServer(app0, host='127.0.0.1', port=1337)
    rpc.start()
