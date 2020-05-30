#!/usr/bin/env python
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.
import abc
import datetime
import logging
import socket
import time
import requests

from urllib import request

from axon.common.config import PACKET_SIZE
from axon.traffic.traffic_objects import TrafficRecord


class Client(abc.ABC):

    @abc.abstractmethod
    def ping(self):
        """
        Send the traffic to a endpoint
        :return:  None
        """
        pass

    @abc.abstractmethod
    def record(self):
        """
        Record the data to a data source
        :return: None
        """
        pass


class TCPClient(Client):

    PROTOCOL = "TCP"

    def __init__(self, source, destination, port, metric_cache,
                 connected=True, action=1, request_count=1):
        """
        Client to send TCP requests
        :param source: source ip
        :type source: string
        :param destination: destination ip
        :type destination: str
        :param port: destination server port
        :type port: int
        :param connected: whether endpoints are connected or not
        :type connected: bool
        :param action: whether traffic will be accepted or rejected by server
                      i.e. 0 for reject and 1 for allow
        :type action: int
        :param record_queue: traffic record queue
        :type record_queue: queue.Queue
        """
        self._source = source
        self._port = port
        self._destination = destination
        self._start_time = None
        self._metric_cache = metric_cache
        self._request_count = request_count
        self._connected = connected
        self._action = action
        self.log = logging.getLogger(__name__)

    def _create_socket(self, address_family=socket.AF_INET,
                       socket_type=socket.SOCK_STREAM):
        """
        Creates a socket
        :param address_family: address family
        :type address_family: socket address family object
        :param socket_type: type of the socket i.e. STREAM or DATAGRAM
        :type socket_type: socket type object
        :return: created socket
        :rtype: socket object
        """
        sock = socket.socket(address_family, socket_type)
        sock.settimeout(10)
        return sock

    def __connect(self, sock):
        """
        Create a connection to the server
        :param sock: socket object
        :type sock: socket
        """
        sock.connect((self._destination, self._port))

    def _send_receive(self, sock, payload):
        """
        Send and recieve the packet
        :param sock: socket which is connected to server
        :type sock: socket
        :param payload: data to be send
        :type payload: str
        :return: data returned from server
        :rtype: str
        """
        try:
            sock.send(payload)
            sock.recv(PACKET_SIZE)
        except Exception as e:
            try:
                self.log.error("Exception %s for TCP %s -> "
                               "%s:%s, trying again",
                               repr(e), self._source,
                               self._destination, self._port)
                time.sleep(.5)
                sock.send(payload)
                sock.recv(PACKET_SIZE)
            except Exception:
                self.log.error("Exception %s for TCP %s -> "
                               "%s:%s, second try",
                               repr(e), self._source,
                               self._destination, self._port)
                raise

    def _get_latency(self):
        """
        Get latency of the request
        :return: latency of the request
        :rtype: float
        """
        time_diff = datetime.datetime.now() - self._start_time
        return int(time_diff.seconds * 1000 + time_diff.microseconds * .001)

    def is_traffic_successful(self, success):
        if not bool(self._connected):
            result = bool(self._connected) == bool(success)
        else:
            # change it later, when action accepts more values
            result = bool(self._action) == bool(success)
        return result

    def record(self, success=True, error=None):
        """
        Record the traffic to data source
        :return: None
        """
        success = self.is_traffic_successful(success)
        metric = TrafficRecord.to_metric(
            self._source, self._destination, self._port, self.PROTOCOL,
            self._connected, success)
        counter = self._metric_cache.counter(metric)
        counter.inc()

    def ping(self):
        payload = 'Dinkirk'.encode()
        connected = False
        try:

            for _ in range(self._request_count):
                try:
                    sock = self._create_socket()
                    self._start_time = datetime.datetime.now()
                    self.__connect(sock)
                    self._send_receive(sock, payload)
                    self.record()
                except Exception as e:
                    self.record(success=False, error=str(e))
                finally:
                    if sock:
                        sock.close()
        except Exception as ex:
            self.log.error("Error %s happened during opening socket" % ex)


class UDPClient(TCPClient):

    PROTOCOL = "UDP"

    def ping(self):
        payload = 'Dinkirk'.encode()
        try:
            sock = self._create_socket(socket.AF_INET, socket.SOCK_DGRAM)
            for _ in range(self._request_count):
                try:
                    self._start_time = datetime.datetime.now()
                    self._send_receive(sock, payload)
                    self.record()
                except Exception as e:
                    self.record(success=False, error=str(e))
        except Exception as ex:
            self.log.error(
                "Error %s happened during creating UDP sockets" %
                ex)
        finally:
            if sock:
                sock.close()

    def _send_receive(self, sock, payload):
        """
        Send and receive the packet
        :param sock: socket which is connected to server
        :type sock: socket
        :param payload: data to be send
        :type payload: str
        :return: data returned from server
        :rtype: str
        """
        try:
            sock.sendto(payload, (self._destination, self._port))
            sock.recvfrom(PACKET_SIZE)
        except Exception as e:
            try:
                self.log.error("Exception %s for UDP %s -> "
                               "%s:%s, trying again",
                               repr(e), self._source,
                               self._destination, self._port)
                time.sleep(.5)
                sock.sendto(payload, (self._destination, self._port))
                sock.recvfrom(PACKET_SIZE)
            except Exception:
                self.log.error("Exception %s for UDP %s -> %s:%s, second try",
                               repr(e), self._source,
                               self._destination, self._port)
                raise


class HTTPClient(TCPClient):

    PROTOCOL = "HTTP"

    def _use_urllib(self, url):
        req = request.Request(url, headers={'Connection': 'close'})
        result = request.urlopen(req)
        return result.code

    def _use_session(self, session, url):
        response = session.get(url)
        return response.status_code

    def _send_receive(self, session=None):
        url = 'http://%s:%s' % (self._destination, self._port)
        status = None

        def get_response():
            if session:
                return self._use_session(session, url)
            else:
                return self._use_urllib(url)
        try:
            status = get_response()
            if status != 200:
                raise Exception("HTTP Request failed with status %s" % status)
        except Exception as e:
            if status:
                raise
            try:
                self.log.error("Exception %s for HTTP %s "
                               "-> %s:%s, trying again",
                               str(e), self._source,
                               self._destination, self._port)
                time.sleep(1)
                status = get_response()
                if status.code != 200:
                    raise Exception(
                        "HTTP Request failed with status %s" % status)
            except Exception:
                self.log.error("Exception %s for HTTP %s ->"
                               " %s:%s, second try",
                               str(e), self._source,
                               self._destination, self._port)
                raise

    def ping(self):
        session = requests.Session() if self._request_count > 1 else None
        for _ in range(self._request_count):
            try:
                self._start_time = datetime.datetime.now()
                self._send_receive(session)
                self.record()
            except Exception as e:
                self.record(success=False, error=str(e))
        if session:
            session.close()


class TrafficClient():
    pass
