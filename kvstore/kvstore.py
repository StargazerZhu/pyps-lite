import zmq
import time
import datetime
import sys
import pyarrow
import numpy as np
from multiprocessing import Process


class KVStore(object):

    def __init__(self, ports="5556"):

        self.context = zmq.Context()
        print("Connecting to server with ports %s" % ports)
        self.socket = self.context.socket(zmq.REQ)
        self.ports = list(ports)
        for port in self.ports:
            self.socket.connect("tcp://localhost:%s" % port)

    def push(self):
        for request in range(20):
            print("Sending request ", request, "...")
            x = [(1, 2), 'hello', 3, 4, np.array([5.0, 6.0])]
            start_time = datetime.datetime.now()
            serialized_x = pyarrow.serialize(x).to_buffer()
            self.socket.send(serialized_x)
            send_end_time = datetime.datetime.now()
            message = self.socket.recv()
            recv_end_time = datetime.datetime.now()
            print("Received reply ", request, "[", message, "], sending in",
                  (send_end_time-start_time).total_seconds(), "seconds, receiving in",
                  (recv_end_time- start_time).total_seconds(), "seconds.")
            # time.sleep(1)

    def pull(self):
        pass