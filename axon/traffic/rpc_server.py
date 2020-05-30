from multiprocessing.connection import Client, Listener
from multiprocessing import Process
import os
import pickle
import time

family = 'AF_UNIX' if os.name == 'posix' else 'AF_PIPE'


class RPCServer(Process):
    def __init__(self, name, handler):
        super().__init__(name=name)
        self._socket = Listener(family=family)
        self._handler = handler

    def _handle_request_no_blocking(self, request):
        pass

    def _handle_request(self, request):
        try:
            while True:
                method, args, kwargs = pickle.loads(request.recv())
                method = getattr(self._handler, method)
                try:
                    response = method(*args, **kwargs)
                    request.send(pickle.dumps(response))
                except Exception as ex:
                    request.send(pickle.dumps(ex))
        except EOFError:
            pass

    def run(self):
        if getattr(self._handler, 'initialize', None):
            self._handler.initialize()
        while True:
            request = self._socket.accept()
            self._handle_request(request)

    @property
    def address(self):
        return self._socket.address

    def stop(self):
        self._socket.close()
        time.sleep(.5)
        self.terminate()

    def is_running(self):
        return self.is_alive()


class RPCClient(object):
    def __init__(self, address):
        self._address = address

    def __connect(self):
        return Client(address=self._address)

    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            connection = self.__connect()
            connection.send(pickle.dumps((name, args, kwargs)))
            result = pickle.loads(connection.recv())
            if isinstance(result, Exception):
                raise result
            return result
        return do_rpc


class DummyHandler(object):
    def echo(self, *args, **kwargs):
        print(args, kwargs)
