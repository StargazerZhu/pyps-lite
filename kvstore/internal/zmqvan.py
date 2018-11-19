import os
import zmq
import time
import random
import threading
from .utils import *
from .resender import Resender


class ZMQVan(object):
    """Van sends messages to remote nodes

    This class is a ZMQ based implementation.
    If environment variable PS_RESEND is set to be 1, then van will resend a
    message if it no ACK message is received within PS_RESEND_TIMEOUT
    millisecond.
    """
    def __init__(self):
        self.context = None

        self.init_stage = True
        self._start_mu = threading.Lock()

        self._resender = None

    def start(self):
        """Start a van

        * Must call this function before calling `send`.
        * This function initializes all connections to other nodes.

        :return:
        """
        # Start zmq
        if self.context is None:
            self.context = zmq.Context()
            self.context.set(zmq.MAX_SOCKETS, 65536)

        if self.init_stage:
            if ('PORT' in os.environ) and (os.environ['PORT'] != ''):
                port = int(os.environ['PORT'])
            else:
                port = get_available_port()

            port = self._bind(port)

    def send(self, msg) -> int:
        """Send a message. It is a thread-safe function.
        :param msg: the message to be sent.
        :return: the number of bytes sent. -1 if failed
        """
        pass

    def stop(self):
        """
        stop the van and stop receiving threads
        :return:
        """
        pass

    @property
    def isready(self):
        """Whether it is ready for sending. thread safe
        :return:
        """
        return None

    def _bind(self, port, hostname=None, max_retry=0) -> int:
        """Bind to my node
        do multiple retries on binding the port. since it's possible that
        different nodes on the same machine picked the same port

        :param max_retry: int, the number of maximum retry
        :return: return the port binded, -1 if failed.
        """
        self._receiver = self.context.socket(zmq.ROUTER)
        assert self._receiver is not None, \
            "create receiver socket failed: "
        local = get_env('DMLC_LOCAL', True)
        if hostname is None:
            hostname = '*'
        if local:
            addr = "ipc:///tmp/"
        else:
            addr = "tcp://" + hostname + ":"

        seed = time.time() + port
        random.seed(seed)
        for i in range(max_retry+1):
            address = addr + str(port)
            ret = self._receiver.bind(address)
            if ret == 0:
                break
            if i == max_retry:
                port = -1
            else:
                port = random.randrange(40000) + 10000

        return port

    def _connect(self, node_id, port, hostname=None):
        """Connect to a node
        :return:
        """
        pass

    def _send_message(self, msg):
        """Send a message
        :param msg: the message to be sent
        :return: the number of bytes sent
        """
        pass

    def _recv_message(self):
        """Block until received a message
        :return: (msg, num):
            msg: message to
            num: the number of bytes received. -1 if failed or timeout
        """
        pass

