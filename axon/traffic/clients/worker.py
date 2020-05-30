from threading import Event, Lock, Thread
import time


from axon.common.executor import BoundedThreadPoolExecutor
from axon.common.metric_cache import ExchangeReporter, MetricsCache
from axon.traffic.clients.clients import HTTPClient, TCPClient, UDPClient
from axon.traffic.traffic_objects import TrafficRuleCollection


class HeartBeatSender(Thread):
    """
    Sends heartbeat periodically with additional
    information to controller process.
    """
    def __init__(self, hb_queue, rules_collection, worker_uid, interval=5):
        super(HeartBeatSender, self).__init__()
        self.daemon = True
        self._hb_queue = hb_queue
        self._rules = rules_collection
        self._interval = interval
        self._uid = worker_uid

    def run(self):
        """Runs heartbeat loop"""
        time.sleep(self._interval)
        while True:
            try:
                self._hb_queue.put((self._uid, "OK",
                                    self._rules.get_rule_count(),
                                    time.time()))
            except Exception as ex:
                print(ex)
            time.sleep(self._interval)


class TrafficGenWorker(object):
    """
    Handler which handles all traffic client related operations. Exposes
    its APIs via RPCServer. APIs can be accessed by using RPCClient by
    providing RPCServers address.
    """

    def __init__(self, uid, hb_queue, exchange):
        self._hb_queue = hb_queue
        self._rule_collections = TrafficRuleCollection()
        self._stop_event = Event()
        self._run_event = Event()
        self._timer = None
        self._stop_lock = Lock()
        self._exchange = exchange
        self._pool = None
        self._uid = uid
        self._hb_interval = 5
        self._metric_cache = MetricsCache()

    def initialize(self):
        """
        RPCServer calls this method when it starts. This
        method starts heartbeat reporter and metrics reporter threads.
        """
        HeartBeatSender(
            self._hb_queue, self._rule_collections, self._uid).start()
        ExchangeReporter(self._metric_cache, self._exchange)

    @property
    def uid(self):
        """Get the uid of worker"""
        return self._uid

    @property
    def traffic_running(self):
        """Return True if traffic is running otherwise False"""
        with self._stop_lock:
            return self._run_event.is_set()

    def _generate_traffic(self, stop_event, callback):
        """
        Generate traffic in infinite loop
        :param stop_event: event which control infinite loop
        :type stop_event: Event
        :param callback: callback to be called after exiting from loop
        :type callback: func
        """
        for rule in self._rule_collections.round_robin_rule_generator():
            with self._stop_lock:
                if stop_event.is_set():
                    break
            try:
                if rule.protocol == "TCP":
                    client = TCPClient
                elif rule.protocol == "UDP":
                    client = UDPClient
                elif rule.protocol == "HTTP":
                    client = HTTPClient
                else:
                    print("Invalid protocol")
                    continue
                sender = client(rule.source, rule.destination,
                                rule.port, self._metric_cache, True,
                                rule.allowed, rule.request_count)
                self._pool.submit(sender.ping)
            except Exception as ex:
                print(ex)
        callback()

    def _cleanup_states(self):
        """Get executed as callback when loop exits"""
        if self._pool:
            self._pool.shutdown()
        with self._stop_lock:
            self._run_event.clear()
            self._stop_event.set()
            self._pool = None

    def _start_traffic(self):
        """Start traffic thread"""
        self._pool = BoundedThreadPoolExecutor(max_workers=10)
        thread = Thread(target=self._generate_traffic,
                        args=(self._stop_event, self._cleanup_states))
        thread.daemon = True
        thread.start()

    def add_clients(self, rules):
        """Add rules to rules collection"""
        try:
            self._rule_collections.add_rules(rules)
            with self._stop_lock:
                if not self._run_event.is_set():
                    self._start_traffic()
                    self._run_event.set()
        except Exception as ex:
            print(ex)

    def delete_clients(self, rules):
        """Delete rules from rules collection"""
        for rule in rules:
            self._rule_collections.delete_rule(rule)

    def delete_all_clients(self):
        """"Delete all rules from collection"""
        if not self._stop_event.is_set():
            with self._stop_lock:
                self._stop_event.set()
        self._rule_collections.clear_rules()

    def get_rule_count(self):
        """Get the number of rules managed by this worker"""
        return self._rule_collections.get_rule_count()

    def has_rule(self, rule):
        """Check if this worker is owner of a rule"""
        return rule in self._rule_collections
