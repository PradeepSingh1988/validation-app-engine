from collections import defaultdict
import logging
import math
from multiprocessing import cpu_count, Queue
import uuid

from axon.common.local_cache import MemCache
from axon.traffic.rpc_server import RPCClient, RPCServer
from axon.traffic.clients.worker import TrafficGenWorker
from axon.traffic.servers.worker import TrafficServerWorker

CLIENT_WORKER_COUNT = min(2, cpu_count() or 1)
SERVER_WORKER_COUNT = min(2, cpu_count() or 1)


class TrafficController(object):
    log = logging.getLogger(__name__)

    def __init__(self, exchange, rules_registry=None, workers_registry=None):
        self._rules_registry = rules_registry if rules_registry else MemCache()
        self._workers_registry = workers_registry if workers_registry else MemCache()
        self._heartbeat_queue = Queue()
        self._exchange = exchange

    @staticmethod
    def _get_worker_context():
        return {'uid': str(uuid.uuid4().hex)}

    @staticmethod
    def _create_worker(name, handler):
        """Create worker process"""
        worker = RPCServer(name, handler)
        worker.start()
        return worker

    def _get_all_workers(self, worker_type='server'):
        """get all workers from registry by a given type"""
        return [
            worker for worker in self._workers_registry.get_all_keys() if
            worker.startswith(worker_type)]

    def _get_all_rules(self, rule_type="server"):
        return [
            rule for rule in self._rules_registry.get_all_keys() if
            rule.startswith(rule_type)]

    def _add_rules_to_existing_workers(
            self, new_rules, rules_per_worker, worker_type="server"):
        """Add Rules to existing workers"""
        for worker, index in enumerate(self._get_all_workers(worker_type)):
            context = self._workers_registry.get(worker)
            start = rules_per_worker * index
            end = start + rules_per_worker
            rules = new_rules[start:end]
            if rules:
                client = RPCClient(context.get('address'))
                if worker_type == 'server':
                    client.add_servers(rules)
                else:
                    client.add_clients(rules)
                # add rule --> worker relationship in registry, so that
                # rules can be controlled later
                for rule in rules:
                    if worker_type == "server":
                        key = "server_%s" % rule.id
                    else:
                        key = "client_%s" % rule.id
                    self._rules_registry.add(key, context)

    def _create_workers_and_add_rules(
            self, new_rules, workers_to_be_created,
            rules_per_worker, worker_type="server"):
        """Create new workers and add servers"""
        for i in range(workers_to_be_created):
            try:
                context = self._get_worker_context()
                start = rules_per_worker * i
                end = start + rules_per_worker
                rules = new_rules[start:end]
                # If no rules, exit from here
                if not rules:
                    break
                if worker_type == "server":
                    handler = TrafficServerWorker(context['uid'])
                else:
                    handler = TrafficGenWorker(
                        context['uid'], self._heartbeat_queue, self._exchange)
                # Start the Worker
                name = "axon_%s_worker_%s" % (worker_type, context['uid'])
                worker = self._create_worker(name, handler)
                context.update({'address': worker.address, 'process': worker})

                # Add the workers info in registry
                key = 'server_%s' % context['uid']
                if worker_type == "server":
                    key = 'server_%s' % context['uid']
                else:
                    key = 'client_%s' % context['uid']
                self._workers_registry.add(key, context)

                # add rule to worker and start the traffic
                client = RPCClient(context.get('address'))
                if worker_type == 'server':
                    client.add_servers(rules)
                else:
                    client.add_clients(rules)

                # add rule --> worker relationship in registry, so that
                # rules can be controlled later
                for rule in rules:
                    if worker_type == "server":
                        key = "server_%s" % rule.id
                    else:
                        key = "client_%s" % rule.id
                    self._rules_registry.add(key, context)
            except Exception as ex:
                print(ex)

    def _delete_rule_from_worker(self, rules, worker_type="server"):
        """Delete and stop rules if its running"""
        # Prepare workers to rule mapping
        workers_rule_map = defaultdict(list)
        for rule in rules:
            rule_key = "%s_%s" % (worker_type, rule.id)
            rule_owner_context = self._rules_registry.get(rule_key)
            if rule_owner_context:
                workers_rule_map[rule_owner_context['uid']].append(rule)

        # delete rules on worker side
        for worker, rules in workers_rule_map.items():
            context = self._workers_registry.get(worker)
            client = RPCClient(context.get('address'))
            if worker_type == "server":
                client.delete_servers(rules)
            else:
                client.delete_clients(rules)
            # delete from registry
            rule_keys = ["%s_%s" % (worker_type, rule.id) for rule in rules]
            self._rules_registry.delete_many(rule_keys)

    def _delete_all_rules_from_workers(self, worker_type="server"):
        for worker in self._get_all_workers(worker_type):
            # Stop traffic
            context = self._workers_registry.get(worker)
            client = RPCClient(context.get('address'))
            if worker_type == "server":
                client.delete_all_servers()
            else:
                client.delete_all_clients()
            # Stop Server
            process = context.get('process')
            if process:
                process.stop()
        # TODO dont delete client rules
        rules = self._get_all_rules(worker_type)
        self._rules_registry.delete_many(rules)

    def start_servers(self, server_rules):
        """Start server for given set of server rules if its not running"""

        existing_server_rules = self._get_all_rules("server")
        # Check if server is already running for given rule
        new_rules = [rule for rule in server_rules if
                     "%s_%s" % ("server", rule.id) not in existing_server_rules]
        current_workers = len(self._get_all_workers("server"))
        workers = SERVER_WORKER_COUNT
        servers_per_worker = int(math.ceil(float(len(new_rules)) / workers))

        # divide new rules with existing workers
        if current_workers and current_workers == workers:
            self._add_rules_to_existing_workers(new_rules, servers_per_worker)
        # If less workers, the create new workers and divide rules among them
        else:
            workers_to_be_created = workers - current_workers
            self._create_workers_and_add_rules(
                new_rules, workers_to_be_created, servers_per_worker)

    def get_servers(self):
        """
        Get servers from workers, which they are managing
        Not being used from outside
        """
        for worker in self._get_all_workers(worker_type="server"):
            context = self._workers_registry.get(worker)
            client = RPCClient(context.get('address'))
            print(worker, client.get_server_count())

    def stop_servers(self, rules):
        """Stop Server for given set of rules if its running"""
        self._delete_rule_from_worker(rules, "server")

    def stop_all_servers(self):
        """Stop all Servers running in the system"""
        self._delete_all_rules_from_workers("server")

    def start_clients(self, rules):
        """
        Start traffic clients for given set of client rules if its not running
        """
        existing_rules = self._get_all_rules("client")
        new_rules = [rule for rule in rules if
                     "%s_%s" % ("client", rule.id) not in existing_rules]
        current_workers = len(self._get_all_workers(worker_type="client"))
        workers = CLIENT_WORKER_COUNT
        rules_per_worker = int(math.ceil(float(len(new_rules)) / workers))

        # divide new rules with existing workers
        if current_workers and current_workers == workers:
            self._add_rules_to_existing_workers(
                new_rules, rules_per_worker, worker_type="client")
        # If less workers, the create new workers and divide rules among them
        else:
            workers_to_be_created = workers - current_workers
            self._create_workers_and_add_rules(
                new_rules, workers_to_be_created,
                rules_per_worker, worker_type="client")

    def get_client_rules(self):
        """Get rules from workers, which they are managing"""
        for worker in self._get_all_workers(worker_type="client"):
            context = self._workers_registry.get(worker)
            client = RPCClient(context.get('address'))
            print(worker, client.get_rule_count())

    def stop_clients(self, rules):
        """Stop Client for given set of rules if its running"""
        self._delete_rule_from_worker(rules, "client")

    def stop_all_clients(self):
        """Stop all Traffic running in the system"""
        self._delete_all_rules_from_workers(worker_type="client")