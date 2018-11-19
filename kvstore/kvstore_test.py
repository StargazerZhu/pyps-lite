from multiprocessing import Process

from .kvstore import KVStore
from .kvstore_server import KVStoreServer


def test_case1():

    def server(port):
        kvstore_server = KVStoreServer(port=port)
        kvstore_server.request_handle()

    def worker(ports):
        kvstore_worker = KVStore(ports=ports)
        kvstore_worker.push()

    server_ports = range(5550, 5558, 2)

    for server_port in server_ports:
        Process(target=server, args=(server_port,)).start()

    # Now we can connect a client to all these servers
    Process(target=worker, args=(server_ports,)).start()


if __name__ == '__main__':
    test_case1()
