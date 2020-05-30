from sqlalchemy import (
    Boolean, create_engine, Column, Integer,
    MetaData, select, Table, Unicode)
from sqlalchemy.exc import IntegrityError

from axon.traffic.traffic_objects import TrafficRule, TrafficServer


class TrafficRulesStore(object):
    """
    Stores TrafficRules in a database table using SQLAlchemy.
    """

    def __init__(self, url, engine_options=None):
        metadata = MetaData()
        self.engine = create_engine(url, **(engine_options or {}))
        self._servers_table = self._init_servers_table(metadata)
        self._clients_table = self._init_clients_table(metadata)
        self._servers_table.create(self.engine, True)
        self._clients_table.create(self.engine, True)

    @staticmethod
    def _init_servers_table(metadata):
        """Create Server rule table"""
        table = Table(
            'servers', metadata,
            Column('id', Unicode, primary_key=True),
            Column('endpoint', Unicode, nullable=False),
            Column('port', Integer, nullable=False),
            Column('protocol', Unicode, nullable=False),
            Column('enabled', Boolean, nullable=False, default=True)
        )
        return table

    @staticmethod
    def _init_clients_table(metadata):
        """Create clients rule table"""
        table = Table(
            'clients', metadata,
            Column('id', Unicode, primary_key=True),
            Column('port', Integer, nullable=False),
            Column('protocol', Unicode, nullable=False),
            Column('source', Unicode, nullable=False),
            Column('destination', Unicode, nullable=False),
            Column('allowed', Boolean, nullable=False),
            Column('enabled', Boolean, nullable=False, default=True),
            Column('request_count', Integer, nullable=False, default=1)
        )
        return table

    def add_client(self, client):
        """Add client to database"""
        add = self._clients_table.insert.values(**{client.as_dict()})
        try:
            self.engine.execute(add)
        except IntegrityError:
            raise

    def add_client_batch(self, clients):
        """Add clients in batch to database"""
        self.engine.execute(
            self._clients_table.insert(),
            [client.as_dict() for client in clients])

    def add_server(self, server):
        """Add server to database"""
        add = self._servers_table.insert().values(**{server.as_dict()})
        try:
            self.engine.execute(add)
        except IntegrityError:
            raise

    def add_server_batch(self, servers):
        """Add servers in batch to database"""
        self.engine.execute(
            self._servers_table.insert(),
            [server.as_dict() for server in servers])

    def delete_servers(self, endpoint, port=None, protocol=None, enabled=None):
        """
        Delete Servers
        :param endpoint: Endpoint IP on which server is listening
        :type endpoint: str
        :param port: port on which server is listening
        :type port: integer
        :param protocol: protocol which server is serving
        :type protocol: str
        :param enabled: whether server is enabled or disabled
        :type enabled: bool
        """
        delete = self._servers_table.delete()
        delete = self.__where_server_query(
            delete, endpoint, port, protocol, enabled)
        result = self.engine.execute(delete)
        if result.rowcount == 0:
            raise Exception("Not found")

    def delete_all_servers(self):
        """Delete all servers from database"""
        delete = self._servers_table.delete()
        self.engine.execute(delete)

    def delete_clients(self, source, port=None, protocol=None,
                       destination=None, enabled=None, allowed=None):
        """
        Delete clients matching given criteria from Database
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param enabled: whether the rule is enabled ir not
        :type enabled: bool
        :param allowed: whether traffic allowed
        :type allowed: bool
        """
        delete = self._clients_table.delete()
        delete = self.__where_client_query(delete, source, port, protocol,
                                           destination, enabled, allowed)
        result = self.engine.execute(delete)
        if result.rowcount == 0:
            raise Exception("Not found")

    def delete_all_clients(self):
        """
        Delete all clients
        """
        delete = self._clients_table.delete()
        self.engine.execute(delete)

    def __where_server_query(self, query, endpoint=None, port=None,
                             protocol=None, enabled=None):
        if endpoint is not None:
            query = query.where(
                self._servers_table.c.endpoint == endpoint)
        if port is not None:
            query = query.where(
                self._servers_table.c.port == port)
        if protocol is not None:
            query = query.where(
                self._servers_table.c.protocol == protocol)
        if enabled is not None:
            query = query.where(
                self._servers_table.c.enabled == enabled)
        return query

    def __where_client_query(self, query, source=None, port=None,
                             protocol=None, destination=None,
                             enabled=None, allowed=None):
        if source is not None:
            query = query.where(
                self._clients_table.c.source == source)
        if port is not None:
            query = query.where(
                self._clients_table.c.port == port)
        if protocol is not None:
            query = query.where(
                self._clients_table.c.protocol == protocol)
        if enabled is not None:
            query = query.where(
                self._clients_table.c.enabled == enabled)
        if destination is not None:
            query = query.where(
                self._clients_table.c.destination == destination)
        if allowed is not None:
            query = query.where(
                self._clients_table.c.allowed == allowed)
        return query

    def get_servers(self, endpoint=None, port=None,
                    protocol=None, enabled=None):
        """
        List all servers matching given criteria
        :param endpoint: Endpoint IP on which server is listening
        :type endpoint: str
        :param port: port on which server is listening
        :type port: integer
        :param protocol: protocol which server is serving
        :type protocol: str
        :param enabled: whether server is enabled or disabled
        :type enabled: bool
        """
        selectables = select([self._servers_table])
        selectables = self.__where_server_query(
            selectables, endpoint, port, protocol, enabled)
        servers = self.engine.execute(selectables).fetchall()
        return [
            TrafficServer(
                server.id, server.endpoint, server.port,
                server.protocol, server.enabled) for server in servers
        ]

    def get_clients(self, source=None, port=None, protocol=None,
                    destination=None, enabled=None, allowed=None):
        """
        List clients matching given criteria from Database
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param enabled: whether the rule is enabled ir not
        :type enabled: bool
        :param allowed: whether traffic allowed
        :type allowed: bool
        """
        selectables = select([self._clients_table])
        selectables = self.__where_client_query(
            selectables, source, port,
            protocol, destination, enabled, allowed)
        clients = self.engine.execute(selectables).fetchall()
        return [
            TrafficRule(
                client.id, client.source, client.destination,
                client.port, client.protocol, client.allowed, client.enabled,
                client.request_count) for client in clients
        ]

    def disable_servers(self, endpoint=None, port=None, protocol=None):
        """
        Disable all servers matching given criteria
        :param endpoint: Endpoint IP on which server is listening
        :type endpoint: str
        :param port: port on which server is listening
        :type port: integer
        :param protocol: protocol which server is serving
        :type protocol: str
        """
        update = self._servers_table.update().values(**{'enabled': False})
        update = self.__where_server_query(update, endpoint, port,
                                           protocol, True)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No Server found with condition")

    def enable_servers(self, endpoint=None, port=None, protocol=None):
        """
        Enable all servers matching given criteria
        :param endpoint: Endpoint IP on which server is listening
        :type endpoint: str
        :param port: port on which server is listening
        :type port: integer
        :param protocol: protocol which server is serving
        :type protocol: str
        """
        update = self._servers_table.update().values(**{'enabled': True})
        update = self.__where_server_query(update, endpoint, port,
                                           protocol, False)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No Server found with condition")

    def disable_clients(self, source=None, port=None,
                        protocol=None, destination=None, allowed=None):
        """
        Disable clients matching given criteria
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param allowed: whether traffic allowed
        :type allowed: bool
        """
        update = self._clients_table.update().values(**{'enabled': False})
        update = self.__where_client_query(update, source, port,
                                           protocol, destination,
                                           True, allowed)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No client found with condition")

    def enable_clients(self, source=None, port=None,
                       protocol=None, destination=None, allowed=None):
        """
        Enable clients matching given criteria
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param allowed: whether traffic allowed
        :type allowed: bool
        """
        update = self._clients_table.update().values(**{'enabled': True})
        update = self.__where_client_query(update, source, port,
                                           protocol, destination,
                                           False, allowed)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No Client found with condition")

    def deny_clients(self, source=None, port=None,
                     protocol=None, destination=None, enabled=None):
        """
        Change rule to deny
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param enabled: whether rule is enabled
        :type enabled: Boolean
        """
        update = self._clients_table.update().values(**{'allowed': False})
        update = self.__where_client_query(update, source, port,
                                           protocol, destination,
                                           enabled, True)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No client found with condition")

    def allowed_clients(self, source=None, port=None,
                      protocol=None, destination=None, enabled=None):
        """
        Change rule to allow
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param enabled: whether rule is enabled
        :type enabled: Boolean
        """
        update = self._clients_table.update().values(**{'allowed': True})
        update = self.__where_client_query(update, source, port,
                                           protocol, destination,
                                           enabled, False)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No Client found with condition")

    def update_request_count(self, request_count, source=None,
                             port=None, protocol=None, destination=None,
                             enabled=None, allowed=None):
        """
        Update request count per session for clients matching given criteria
        :param request_count: request count per session
        :type request_count: int
        :param source: source of client rule
        :type source: str
        :param port: port number
        :type port: int
        :param protocol: protocol name
        :type protocol: str
        :param destination: destination address
        :type destination: str
        :param enabled: whether the rule is enabled ir not
        :type enabled: Boolean
        :param allowed: whether traffic allowed
        :type allowed: Boolean
        """
        update = self._clients_table.update().values(
            **{'request_count': request_count})
        update = self.__where_client_query(update, source, port,
                                           protocol, destination,
                                           enabled, allowed)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise Exception("No Client found with condition")

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
