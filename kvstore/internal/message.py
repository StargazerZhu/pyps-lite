from collections import namedtuple
from enum import Enum


class Role(Enum):
    SERVER = 1
    WORKER = 2
    SCHEDULER = 4


class Command(Enum):
    EMPTY = 1
    TERMINATE = 2
    ADD_NODE = 3
    BARRIER = 4
    ACK = 5
    HEARTBEAT = 6


class Node(namedtuple('Node', [
    'id', 'role', 'hostname', 'port', 'ip', 'is_recovery'])):

    def __str__(self):
        ss = "role="
        ss += str(self.role).lower()
        if self.id is not None:
            ss += ", id=%d" % self.id
        ss += ", ip={}, port={}, is_recovery={}".format(
            self.ip, self.port, self.is_recovery
        )
        return ss


class Control(namedtuple('Control', ['cmd', 'node', 'msg_sig'])):

    @property
    def is_empty(self):
        return self.cmd == Command.EMPTY

    def __str__(self):
        if self.is_empty:
            return ""
        ss = "cmd=%s" % str(self.cmd)
        if len(self.node) > 0:
            ss += ", node={"
            for n in self.node:
                ss += " %s" % str(n)
            ss += "}"
        if self.cmd == Command.ACK:
            ss += ", msg_sig=" + str(self.msg_sig)


class Meta(namedtuple('Meta', [
    'timestamp', 'control', 'recver', 'sender', 'app_id', 'request',
    'head', 'body', 'data_type'])):

    def __str__(self):
        ss = ""
        ss += self.sender
        ss += " => {}. Meta: request={}".format(self.recver, self.request)
        if self.timestamp is not None:
            ss += ", timestamp=%d" % self.timestamp
        if self.control is not None:
            ss += ", control={ %s }" % str(self.control)
        else:
            ss += ", app_id=%d" % self.app_id
        if self.head != '':
            ss += ", head=%s" % self.head
        if len(self.body) > 0:
            ss += ", body=%s" % self.body
        if len(self.data_type) > 0:
            ss += ", data_type={"
            for d in self.data_type:
                ss += " " + str(d)
            ss += "}"
        return ss


class Message(namedtuple('Message', ['meta', 'data'])):

    def add_data(self, val):
        self.data.append(val)
        self.meta.data_type.append(type(val))

    def __str__(self):
        ss = str(self.meta)
        ss += " Body:"
        ss += str(self.data)
        return ss

