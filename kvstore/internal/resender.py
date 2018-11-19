from collections import namedtuple
import time
import threading
import logging
from datetime import datetime

from .zmqvan import ZMQVan
from .message import *

Entry = namedtuple('Entry', ['msg', 'send_time', 'num_retry'])


class Resender(object):
    """Resend a message if no ack is received within a given time
    """
    def __init__(self, timeout, max_retry, van):
        """Constructor function
        :param timeout: int, in milliseconds
        :param max_retry: int, maximum number to retry sending
        :param van: ZMQVan instance that sends msg out
        """
        self._local = threading.local()

        self._initializer(timeout, max_retry, van)

        self._monitor = None

    def __enter__(self, timeout=1000, max_retry=40, van=None):
        self._exit_lock = threading.Lock()
        self._exit_lock.acquire()

        self._initializer(timeout, max_retry, van)
        # creating a monitoring process.
        self._monitor = threading.Thread(target=self._monitoring,
                                         name="Monitor Thread")
        self._monitor.start()
        self._mu = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local.exit = True
        self._monitor.join()

    def add_out_going(self, msg):
        """Add an outgoing message
        :param msg: Message, the message to be sent
        """
        assert isinstance(msg, Message)

        if msg.meta.control.cmd == Command.ACK:
            return
        key = self._get_key(msg)
        self._mu.acquire()
        if key not in self._send_buff:
            return

        self._send_buff[key].msg = msg
        self._send_buff[key].send = datetime.now()
        self._send_buff[key].num_retry = 0
        self._mu.release()

    def add_incoming(self, msg):
        """Add an incoming message
        :param msg: Message
        :return: bool, True if msg has been added or obtain ACK
        """
        assert isinstance(msg, Message)

        if msg.meta.control.cmd == Command.TERMINATE:
            return False
        elif msg.meta.control.cmd == Command.ACK:
            self._mu.acquire()
            key = msg.meta.control.msg_sig
            if key in self._send_buff:
                del self._send_buff[key]
            self._mu.release()
            return True
        else:
            self._mu.acquire()
            key = self._get_key(msg)
            duplicated = key in self._acked
            if not duplicated:
                self._acked.add(key)
            self._mu.release()

            ack = Message()
            ack.meta.recver = msg.meta.sender
            ack.meta.sender = msg.meta.recver
            ack.meta.control.cmd = Command.ACK
            ack.meta.control.msg_sig = key
            self._van.Send(ack)

            # Warning
            if duplicated:
                logging.warning("Duplicated messages:", str(msg))

            return duplicated

    def _get_key(self, msg):
        assert msg.meta.timestamp is not None, str(msg)
        id = msg.meta.app_id
        if msg.meta.sender is None:
            sender = self._van.my_node().id
        else:
            sender = msg.meta.sender

        recver = msg.meta.recver
        ret = int(id) << 48 | int(sender) << 40 | int(recver) << 32
        ret |= msg.meta.timestamp << 1 | msg.meta.request

        return ret

    def _monitoring(self):
        while not self._exit:
            time.sleep(self._timeout)
            resend = []
            now = datetime.now()
            self._mu.acquire()
            try:
                for key in self._send_buff:
                    val = self._send_buff[key]
                    if (val.send + self._timeout) * (1+val.num_retry) < now:
                        resend.append(val.msg)
                    self._send_buff[key].num_retry += 1
                    warning_str = "%s: Timeout to get the ACK message. Resend (retry=%d) %s"
                    warning_str = warning_str % (
                        str(self._van.my_node()),
                        self._send_buff[key].num_retry,
                        str(val.msg))
                    logging.warning(warning_str)
            finally:
                self._mu.release()

            for msg in resend:
                self._van.send(msg)

    def _initializer(self, timeout, max_retry, van):
        self._timeout = timeout
        self._max_retry = max_retry
        assert isinstance(van, ZMQVan)
        self._van = van

        self._local.exit = False

        # private variables
        self._send_buff = {}  # int -> Entry
        self._acked = set([])
