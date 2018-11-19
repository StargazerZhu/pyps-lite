import zmq
import time
import sys
import pyarrow
import numpy as np
from multiprocessing import Process


class KVStoreServer(object):

    def __init__(self, port="5556"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)
        self.port = port

    def request_handle(self):
        for reqnum in range(5):
            # Wait for next request from client
            serialized_x = self.socket.recv()
            deserialized_x = pyarrow.deserialize(serialized_x)
            print("Received request #%s: %s" % (reqnum, deserialized_x))
            self.socket.send_string("World from %s" % self.port)

