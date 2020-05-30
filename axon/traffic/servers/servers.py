#!/usr/bin/env python
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.
import asyncio
import os
import ssl


DEFAULT_HTTP_RESPONSE = b"Hello From Axon"
SERVER_CERT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'axon.crt')
SERVER_KEY_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'axon.key')


class HTTPProtocol(asyncio.Protocol):
    
    def connection_made(self, transport):
        # peername = transport.get_extra_info('peername')
        # print('Connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        request_lines = data.decode().split("\n")
        try:
            method, path, proto = request_lines[0].split()
            if method == 'GET':
                self.start_response()
                self.transport.write(DEFAULT_HTTP_RESPONSE)
            else:
                self.start_response(status=b"405")
                self.transport.write(b"405\r\n")
        except Exception as e:
            print(e)
        self.transport.close()

    def start_response(
            self, content_type=b"text/html; charset=utf-8",  status=b"200"):
        self.transport.write(b"HTTP/1.0 %b NA\r\n" % status)
        self.transport.write(b"Content-Type: ")
        self.transport.write(content_type)
        self.transport.write(b"\r\n\r\n")


class EchoServerProtocol:
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.transport.sendto(data, addr)


class EchoServerClientProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)
        self.transport.close()


def tcp_serve(host, port, loop, reuse_port=True, sock=None, backlog=100):
    server_coroutine = loop.create_server(
        EchoServerClientProtocol, host, port,
        reuse_port=reuse_port, sock=sock, backlog=backlog)
    return server_coroutine


def udp_serve(host, port, loop, reuse_port=True, sock=None):
    server_coroutine = loop.create_datagram_endpoint(
        EchoServerProtocol, local_addr=(host, port),
        reuse_port=reuse_port, sock=sock)
    return server_coroutine


def http_serve(host, port, loop, reuse_port=True, sock=None, backlog=100):
    server_coroutine = loop.create_server(
        HTTPProtocol, host, port,
        reuse_port=reuse_port, sock=sock, backlog=backlog)
    return server_coroutine


def https_serve(host, port, loop, reuse_port=True, sock=None, backlog=100):
    ssl_cntext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_cntext.load_cert_chain(SERVER_CERT_FILE, SERVER_KEY_FILE)
    loop.debug = True
    server_coroutine = loop.create_server(
        HTTPProtocol, host, port,
        reuse_port=reuse_port, sock=sock,
        backlog=backlog, ssl=ssl_cntext)
    return server_coroutine


class ServerFactory(object):
    def __init__(self, server):
        print(server)

    @classmethod
    def create_server(cls, server, loop):
        if server.protocol == 'TCP':
            return tcp_serve(server.endpoint, server.port, loop)
        elif server.protocol == 'UDP':
            return udp_serve(server.endpoint, server.port, loop)
        elif server.protocol == 'HTTP':
            return http_serve(server.endpoint, server.port, loop)
        elif server.protocol == 'HTTPS':
            return https_serve(server.endpoint, server.port, loop)