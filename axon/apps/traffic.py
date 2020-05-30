#!/usr/bin/env python
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.

from collections import defaultdict, OrderedDict
import logging
from uuid import uuid4

from axon.apps.base import app_registry, BaseApp, exposed, exposify
from axon.common.monit_queues import get_exchange
from axon.common.subscribers import SQLRecorder, WavefrontDirectRecorder
from axon.traffic.controller import TrafficController
from axon.traffic.record_store import TrafficRecordStore
from axon.traffic.rules_store import TrafficRulesStore
from axon.traffic.traffic_objects import TrafficRule, TrafficServer


@exposify
class TrafficApp(BaseApp):

    NAME = 'TRAFFIC'

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self._rules_store = TrafficRulesStore(
            "sqlite:////Users/singhpradeep/.axon/rules.db")
        self._record_store = TrafficRecordStore(
            "sqlite:////Users/singhpradeep/.axon/records.db")
        record_db_subscriber = SQLRecorder(self._record_store)
        wavefront_subscriber = WavefrontDirectRecorder(
            'https://vmware.wavefront.com',
            "e270bbe2-2e22-4caf-961c-ab9e50e9165b", "pradeeps-mac",
            tags={"datacenter": "pradeeps-tes"})
        traffic_exchange = get_exchange('traffic')
        traffic_exchange.attach(record_db_subscriber, 30)
        traffic_exchange.attach(wavefront_subscriber, 30)
        self._traffic_controller = TrafficController(traffic_exchange)

    def initialize(self):
        self.start_servers()
        self.start_clients()

    def shutdown(self):
        self.stop_clients()
        self.start_servers()

    @exposed
    def add_server(self, protocol, port, endpoint, namespace=None):
        pass

    @exposed
    def delete_all_traffic_rules(self):
        """
        Delete all traffic rules from DB
        """
        self._rules_store.delete_all_clients()
        self._rules_store.delete_all_servers()

    @exposed
    def delete_traffic_rules_for_endpoint(self, endpoint):
        """
        Delete traffic rules for an endpoint from db
        :param endpoint: endpoint IP for which rules will be deleted
        :type endpoint: str
        """
        self._rules_store.delete_servers(endpoint=endpoint)
        self._rules_store.delete_clients(source=endpoint)

    @exposed
    def delete_clients_rule(self, endpoint=None, destination=None,
                            port=None, protocol=None,
                            allowed=None, enabled=None):
        """
        Delete Clients Rule from DB for matching criteria
        :param endpoint: endpoint for which rules need to be deleted
        :type endpoint: str
        :param destination: destination for which rules need to be deleted
        :type destination: str
        :param port: port for which rules need to be deleted
        :type port: int
        :param protocol: protocol for which rules need to be deleted
        :type protocol: str
        :param allowed: allowed value for which rules will be deleted
        :type allowed: bool
        :param enabled: enabled value for which rules need to be deleted
        :type enabled: bool
        """
        self._rules_store.delete_clients(
            source=endpoint, port=port,
            protocol=protocol, destination=destination,
            enabled=enabled, allowed=allowed)

    @exposed
    def disable_clients_rule(self, endpoint=None, destination=None,
                             port=None, protocol=None, allowed=None):
        """
        Disable Clients Rule from DB for matching criteria
        :param endpoint: endpoint for which rules need to be disabled
        :type endpoint: str
        :param destination: destination for which rules need to be disabled
        :type destination: str
        :param port: port for which rules need to be disabled
        :type port: int
        :param protocol: protocol for which rules need to be disabled
        :type protocol: str
        :param allowed: allowed value for which rules will be disabled
        :type allowed: boolean
        """
        self._rules_store.disable_clients(
            source=endpoint, port=port,
            protocol=protocol, destination=destination, allowed=allowed)
        disabled_rules = self._rules_store.get_clients(
            source=endpoint, port=port, protocol=protocol,
            destination=destination, enabled=False, allowed=allowed)
        self._traffic_controller.stop_clients(disabled_rules)

    @exposed
    def enable_clients_rule(self, endpoint=None, destination=None,
                            port=None, protocol=None, allowed=None):
        """
        Enable Clients Rule from DB for matching criteria
        :param endpoint: endpoint for which rules need to be enabled
        :type endpoint: str
        :param destination: destination for which rules need to be enabled
        :type destination: str
        :param port: port for which rules need to be enabled
        :type port: int
        :param protocol: protocol for which rules need to be enabled
        :type protocol: str
        :param allowed: allowed value for which rules will be enabled
        :type allowed: boolean
        """
        self._rules_store.enable_clients(
            source=endpoint, port=port,
            protocol=protocol, destination=destination, allowed=allowed)
        enabled_rules = self._rules_store.get_clients(
            source=endpoint, port=port, protocol=protocol,
            destination=destination, enabled=True, allowed=allowed)
        self._traffic_controller.start_clients(enabled_rules)

    @exposed
    def delete_servers_rule(self, endpoint=None, port=None,
                            protocol=None, enabled=None):
        """
        Delete servers Rule from DB for matching criteria
        :param endpoint: endpoint for which rules need to be deleted
        :type endpoint: str
        :param port: port for which rules need to be deleted
        :type port: int
        :param protocol: protocol for which rules need to be deleted
        :type protocol: str
        :param enabled: enabled value for which rules need to be deleted
        :type enabled: bool
        """
        self._rules_store.delete_servers(endpoint, port, protocol, enabled)

    @exposed
    def disable_servers_rule(self, endpoint=None, port=None, protocol=None):
        """
        Disable servers matching given criteria
        :param endpoint: endpoint for which servers will be disabled
        :type endpoint: str
        :param port: port for which servers will be disabled
        :type port: int
        :param protocol: protocol for which servers will be disabled
        :type protocol: str
        """
        self._rules_store.disable_servers(endpoint, port, protocol)

    @exposed
    def enable_servers_rule(self, endpoint=None, port=None, protocol=None):
        """
        Enable servers matching given criteria
        :param endpoint: endpoint for which servers will be enabled
        :type endpoint: str
        :param port: port for which servers will be enabled
        :type port: int
        :param protocol: protocol for which servers will be enabled
        :type protocol: str
        """
        self._rules_store.enable_servers(endpoint, port, protocol)

    @exposed
    def register_traffic(self, traffic_configs):
        """
        Register Traffic in with Axon. Axon Stores all the traffic configuration
        in database, it does not starts any traffic as part of this API.
        Duplicate rules which are already registered with Axon Will be ignored
        :param traffic_configs: traffic configuration
        :type traffic_configs: list
        Example:
        traffic_config = [{
            'endpoint': '127.0.0.1',
            'servers': [(8080, 'HTTP'), (8585, 'TCP'), (9000, 'UDP')],
            'clients': [('127.0.0.1', 8585, 'TCP', True, True, 10),
                        ('127.0.0.1', 9000, 'UDP', True, True, 10),
                        ('127.0.0.1', 8000, 'HTTP', True, True, 10)]
        }]
        """
        self.log.info("Register traffic called with config %s" %
                      traffic_configs)
        servers = []
        clients = []
        current_servers = self._rules_store.get_servers()
        current_clients = self._rules_store.get_clients()
        for config in traffic_configs:
            endpoint = config['endpoint']
            for item in config['servers']:
                server_rule = TrafficServer(uuid4().hex, endpoint,
                                            item[0], item[1])
                servers.append(server_rule)
            for item in config['clients']:
                client_rule = TrafficRule(uuid4().hex, endpoint,
                                          item[0], item[1], item[2],
                                          item[3], item[4], item[5])
                clients.append(client_rule)

        # Remove redundant Rules
        servers = list(OrderedDict.fromkeys(servers))
        clients = list(OrderedDict.fromkeys(clients))

        # remove Existing rules
        servers = list(set(servers) - set(current_servers))
        clients = list(set(clients) - set(current_clients))

        if servers:
            self._rules_store.add_server_batch(servers)
        if clients:
            self._rules_store.add_client_batch(clients)

    @exposed
    def register_servers(self, endpoint, servers):
        """
        Register list of servers with an endpoint
        :param endpoint: ip on which server will listen
        :type endpoint: str
        :param servers: list of server tuple
        :type servers: list

        Example: Servers
        [(8585, 'TCP'), ('9090', 'UDP')]
        """
        servers_list = list()
        current_servers = self._rules_store.get_servers(endpoint=endpoint)
        for server in servers:
            server_rule = TrafficServer(uuid4().hex, endpoint,
                                        server[0], server[1])
            servers_list.append(server_rule)

        # Remove redundant Rules
        servers_list = list(OrderedDict.fromkeys(servers_list))

        # remove Existing rules
        servers_list = list(set(servers_list) - set(current_servers))
        if servers_list:
            self._rules_store.add_server_batch(servers_list)

    @exposed
    def register_clients(self, endpoint, clients):
        """
        Register a list of clients with an endpoint
        :param endpoint: source IP from which traffic will be generated
        :type endpoint: str
        :param clients: list of client tuples
        :type clients: list

        Example: Clients (destination, port, protocol,
                          allowed, enabled, req_count)
        [('127.0.0.1', 8585, 'TCP', True, True, 10),
         ('127.0.0.1', 9000, 'UDP', True, True, 10),
         ('127.0.0.1', 8000, 'HTTP', True, True, 10)]
        """
        clients_list = list()
        current_clients = self._rules_store.get_clients(source=endpoint)
        for client in clients:
            client_rule = TrafficRule(uuid4().hex, endpoint, client[0],
                                      client[1], client[2], client[3],
                                      client[4], client[5])
            clients_list.append(client_rule)

        # Remove redundant Rules
        clients_list = list(OrderedDict.fromkeys(clients_list))

        # remove Existing rules
        clients_list = list(set(clients_list) - set(current_clients))

        if clients_list:
            self._rules_store.add_client_batch(clients_list)

    @exposed
    def get_traffic_rules(self, endpoint=None):
        """
        Get all the traffic rules registered with axon
        :param endpoint: endpoint IP for which rule will be listed
        :type endpoint: str
        """
        clients = self._rules_store.get_clients(endpoint)
        servers = self._rules_store.get_servers(endpoint)
        result = defaultdict(dict)
        for server in servers:
            server_tuple = (server.port, server.protocol)
            if result[server.endpoint].get('servers'):
                result[server.endpoint]['servers'].append(server_tuple)
            else:
                result[server.endpoint]['servers'] = [server_tuple]
                result[server.endpoint]['endpoint'] = server.endpoint
        for client in clients:
            client_tuple = (client.destination, client.port,
                            client.protocol, client.allowed,
                            client.enabled, client.request_count)
            if result[client.source].get('clients'):
                result[client.source]['clients'].append(client_tuple)
            else:
                result[client.source]['clients'] = [client_tuple]
                result[client.source]['endpoint'] = client.source
        return list(result.values())

    @exposed
    def list_servers(self):
        pass

    @exposed
    def get_server(self, protocol, port):
        pass

    @exposed
    def start_servers(self, endpoint=None, port=None, protocol=None):
        """
        Start servers for matching criteria
        :param endpoint: endpoint IP
        :type endpoint: str
        :param port: port on which server is listening
        :type port: int
        :param protocol: protocol which server is serving
        :type protocol: str
        """
        server_rules = self._rules_store.get_servers(
            endpoint=endpoint, port=port, protocol=protocol, enabled=True)
        print(server_rules)
        self._traffic_controller.start_servers(server_rules)

    @exposed
    def stop_servers(self, endpoint=None, port=None, protocol=None):
        """
        Start servers for matching criteria
        :param endpoint: endpoint IP
        :type endpoint: str
        :param port: port on which server is listening
        :type port: int
        :param protocol: protocol which server is serving
        :type protocol: str
        """
        server_rules = self._rules_store.get_servers(
            endpoint=endpoint, port=port, protocol=protocol, enabled=True)
        self._traffic_controller.stop_servers(server_rules)

    @exposed
    def stop_clients(self, endpoint=None, destination=None,
                     port=None, protocol=None, allowed=None):
        """
        Stop all Clients who are sending traffic for matching criteria
        :param endpoint: endpoint for which clients needs to be stopped
        :type endpoint: str
        :param destination: destination for which clients needs to be stopped
        :type destination: str
        :param port: port for which clients needs to be stopped
        :type port: int
        :param protocol: protocol for which clients needs to be stopped
        :type protocol: str
        :param allowed: allowed value for clients needs to be stopped
        :type allowed: boolean
        """
        client_rules = self._rules_store.get_clients(
            source=endpoint, port=port, protocol=protocol,
            destination=destination, enabled=True, allowed=allowed)
        self._traffic_controller.stop_clients(client_rules)

    @exposed
    def start_clients(self, endpoint=None, destination=None,
                      port=None, protocol=None, allowed=None):
        """
        Start all Clients who are sending traffic for matching criteria
        :param endpoint: endpoint for which clients needs to be started
        :type endpoint: str
        :param destination: destination for which clients needs to be started
        :type destination: str
        :param port: port for which clients needs to be started
        :type port: int
        :param protocol: protocol for which clients needs to be started
        :type protocol: str
        :param allowed: allowed value for clients needs to be started
        :type allowed: boolean
        """
        client_rules = self._rules_store.get_clients(
            source=endpoint, port=port, protocol=protocol,
            destination=destination, enabled=True, allowed=allowed)
        self._traffic_controller.start_clients(client_rules)


app_registry[TrafficApp.NAME] = TrafficApp

if __name__ == "__main__":
    app = TrafficApp()
    print(app.__dict__)
    print(dir(TrafficApp))
    # traffic_config = [{
    #     'endpoint': '127.0.0.1',
    #     'servers': [(8080, 'HTTP'), (8585, 'TCP'),
    #                 (9000, 'UDP')],
    #     'clients': [('127.0.0.1', 8585, 'TCP', True, True, 10),
    #                 ('127.0.0.1', 9000, 'UDP', True, True, 10),
    #                 ('127.0.0.1', 8080, 'HTTP', True, True, 10)]
    # }]
    # app.register_traffic(traffic_config)
    # app.start_servers()
    #
    # import time
    # time.sleep(20)
    # app.start_clients()
    # time.sleep(120)