from collections import deque
import logging
from threading import Lock
import time
import uuid


class TrafficServer(object):
    __slots__ = ('id', 'endpoint', 'port', 'protocol', 'enabled')

    def __init__(self, id, endpoint, port, protocol, enabled=True):
        self.id = id
        self.endpoint = endpoint
        self.port = port
        self.protocol = protocol
        self.enabled = enabled

    def as_dict(self):
        return {
            'id': self.id,
            'endpoint': self.endpoint,
            'port': self.port,
            'protocol': self.protocol,
            'enabled': self.enabled
        }

    def __str__(self):
        return "{}({})".format(self.__class__.__name__,
                               ("endpoint=%s, port=%s, "
                                "protocol=%s, enabled=%s" % (
                                    self.endpoint, self.port,
                                    self.protocol, self.enabled)))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, TrafficServer):
            return (self.endpoint == other.endpoint
                    and self.port == other.port
                    and self.protocol == other.protocol)
        return False

    def __hash__(self):
        return hash(self.__str__())


class TrafficRule(object):
    __slots__ = ('id', 'source', 'destination', 'port',
                 'protocol', 'allowed', 'enabled', 'request_count')

    def __init__(self, id, source, destination, port,
                 protocol, allowed=True,
                 enabled=True, request_count=1):
        self.id = id
        self.source = source
        self.destination = destination
        self.port = port
        self.protocol = protocol
        self.allowed = allowed
        self.enabled = enabled
        self.request_count = request_count

    def as_dict(self):
        return {
            'id': self.id,
            'source': self.source,
            'destination': self.destination,
            'port': self.port,
            'protocol': self.protocol,
            'enabled': self.enabled,
            'allowed': self.allowed,
            'request_count': self.request_count
        }

    def __str__(self):
        return "{}({})".format(self.__class__.__name__,
                               ("source=%s, destination=%s, port=%s, "
                                "protocol=%s, allowed=%s" % (
                                    self.source, self.destination,
                                    self.port, self.protocol, self.allowed)))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, TrafficRule):
            return (self.source == other.source
                    and self.destination == other.destination
                    and self.port == other.port
                    and self.protocol == other.protocol
                    and self.allowed == other.allowed)
        return False

    def __hash__(self):
        return hash(self.__str__())


class TrafficRuleCollection(object):
    log = logging.getLogger(__name__)

    def __init__(self):
        self._rules_map = {}
        self._rules = deque()
        self._mutex = Lock()

    def __add_rule(self, rule):
        if rule not in self._rules_map:
            self._rules.append(rule)
            self._rules_map[rule] = 1

    def add_rule(self, rule):
        with self._mutex:
            self.__add_rule(rule)

    def __delete_rule(self, rule):
        try:
            rule = self._rules_map[rule]
            self._rules.remove(rule)
        except KeyError:
            self.log.error(
                "Rule %s does not exists in rule collection" % rule)
            raise

    def delete_rule(self, rule):
        with self._mutex:
            self.__delete_rule(rule)

    def add_rules(self, rules):
        with self._mutex:
            for rule in rules:
                self.__add_rule(rule)

    def clear_rules(self):
        with self._mutex:
            self._rules_map.clear()
            self._rules.clear()

    def delete_rules(self, rules):
        with self._mutex:
            for rule in rules:
                self.__delete_rule(rule)

    def get_rule_count(self):
        with self._mutex:
            return len(self._rules_map)

    def __contains__(self, item):
        with self._mutex:
            return item in self._rules_map

    def round_robin_rule_generator(self):
        """
        This method generates a round robin iteration of the rules.
        """
        while self._rules_map:
            try:
                with self._mutex:
                    item = self._rules[0]
                    yield item
            except IndexError:
                self.log.error("No Traffic rules exists yet")
                return
            finally:
                with self._mutex:
                    if item in self._rules_map:
                        self._rules.rotate(-1)
        print("Exiting from generator")


class TrafficRecord:
    """
    Class to represent TrafficRecord
    """
    __slots__ = ('id', 'source', 'destination', 'port', 'protocol',
                 'success_count', 'failure_count', 'connected', 'created')

    def __init__(self, id, source, destination, port, protocol,
                 success_count=0, failure_count=0, connected=True):
        self.id = id
        self.source = source
        self.destination = destination
        self.port = port
        self.protocol = protocol
        self.success_count = success_count
        self.failure_count = failure_count
        self.connected = connected
        self.created = time.time()

    def as_dict(self):
        return {
            'id': self.id, 'source': self.source,
            'destination': self.destination, 'port': self.port,
            'protocol': self.protocol, 'success_count': self.success_count,
            'failure_count': self.failure_count, 'connected': self.connected,
            'created': self.created
        }

    def __str__(self):
        return "TrafficRecord(%s)" % \
               (",".join([self.source, self.destination,
                          self.port, self.protocol, str(self.connected)]))

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__str__())

    @classmethod
    def to_metric(cls, source, destination, port, protocol, connected, success):
        metric = "{}:{}:{}:{}:{}".format(
            source, destination, port, protocol, connected)
        return (
            "%s:%s" % (metric, 'success') if success else
            "%s:%s" % (metric, 'failure'))

    @classmethod
    def from_metric(cls, metric):
        (source, destination, port,
         protocol, connected, result) = metric.split(":")
        return cls(uuid.uuid4().hex, source, destination, port,
                   protocol, connected=(connected == 'True'))

    @classmethod
    def is_success(cls, metric):
        return "success" in metric

