import asyncio
from threading import Thread
import time

from axon.traffic.servers import ServerFactory


class TrafficServerWorker(object):
    def __init__(self, uid):
        self._uid = uid
        self._servers = list()
        self._running_servers = dict()
        self._loop = None

    def initialize(self):
        thread = Thread(target=self._run_servers)
        thread.daemon = True
        thread.start()
        time.sleep(1)

    def _run_servers(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def add_servers(self, servers):
        for server in servers:
            if server not in self._servers:
                self._servers.append(server)
                async_server = ServerFactory.create_server(server, self._loop)
                server_instance = asyncio.run_coroutine_threadsafe(
                    async_server, self._loop)
                self._running_servers[server] = server_instance.result()

    def delete_servers(self, servers):
        for server in servers:
            if server in servers:
                self._stop_server(server)
                self._servers.remove(server)

    def delete_all_servers(self):
        for server in self._servers:
            self._stop_server(server)
            self._servers.remove(server)

    def _stop_server(self, server):
        server_instance = self._running_servers[server]
        server_instance.close()
        future = asyncio.run_coroutine_threadsafe(
            server_instance.wait_closed(), self._loop)
        future.result()
        del self._running_servers[server]

    def get_server_count(self):
        return len(self._servers)

    def has_server(self, server):
        return server in self._servers