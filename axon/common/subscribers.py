import abc
from collections import defaultdict
import time

from wavefront_sdk import WavefrontDirectClient, WavefrontProxyClient
from wavefront_sdk.common import metric_to_line_data

from axon.traffic.traffic_objects import TrafficRecord


class ExchangeSubscriber(abc.ABC):

    @abc.abstractmethod
    def handle(self, messages):
        pass


class SQLRecorder(ExchangeSubscriber):

    def __init__(self, record_store):
        self._record_store = record_store

    def handle(self, messages):
        traffic_records_map = {}
        for message in messages:
            for metric, value in message.items():
                success = TrafficRecord.is_success(metric)
                record = TrafficRecord.from_metric(metric)
                record = traffic_records_map.get(str(record), record)
                if success:
                    record.success_count = value
                else:
                    record.failure_count = value
                traffic_records_map[str(record)] = record
        records = list(traffic_records_map.values())
        print(int(time.time()), " ", records)
        self._record_store.add_records_batch(records)


class WavefrontRecorder(ExchangeSubscriber):
    def __init__(self, source, tags=None):
        self.source = source
        self.prefix = 'axon'
        self.tags = tags if tags else {}
        self._client = None

    def reconnect(self):
        self._client = None

    def handle(self, messages):
        metrics = []
        total_success = 0
        total_failure = 0
        protocol_success = defaultdict(int)
        protocol_failure = defaultdict(int)
        create_time = time.time()
        for message in messages:
            for metric, value in message.items():
                success = TrafficRecord.is_success(metric)
                record = TrafficRecord.from_metric(metric)
                if success:
                    name = '{}.{}.{}.success'.format(
                            self.prefix, "traffic", "request")
                    total_success += value
                    protocol_success[record.protocol.lower()] += value

                else:
                    name = '{}.{}.{}.failure'.format(
                        self.prefix, "traffic", "request")
                    total_failure += value
                    protocol_failure[record.protocol.lower()] += value
                tags = {'source': record.source,
                        'destination': record.destination,
                        'port': str(record.port),
                        'protocol': record.protocol,
                        'connected': str(record.connected)
                }
                tags.update(self.tags)
                metric = metric_to_line_data(name=name, value=value,
                                             timestamp=int(create_time),
                                             source=self.source, tags=tags,
                                             default_source=self.source)
                metrics.append(metric)

        metrics.append(
            metric_to_line_data(name="%s.%s.%s.%s.%s" %
                                (self.prefix, "traffic", "request",
                                 "total", "success"),
                                value=total_success,
                                timestamp=int(create_time),
                                source=self.source, tags=self.tags,
                                default_source=self.source)
        )
        metrics.append(
            metric_to_line_data(name="%s.%s.%s.%s.%s" %
                                (self.prefix, "traffic", "request",
                                 "total", "failure"),
                                value=total_failure,
                                timestamp=int(create_time),
                                source=self.source, tags=self.tags,
                                default_source=self.source)
        )
        for protocol, value in protocol_success.items():
            metrics.append(
                metric_to_line_data(
                    name="%s.%s.%s.%s.%s" %
                         (self.prefix, "traffic", protocol,
                          "request", "success"),
                    value=value,
                    timestamp=int(create_time),
                    source=self.source, tags=self.tags,
                    default_source=self.source))
        for protocol, value in protocol_failure.items():
            metrics.append(
                metric_to_line_data(name="%s.%s.%s.%s.%s" %
                                    (self.prefix, "traffic", protocol,
                                     "request", "failure"),
                                    value=value,
                                    timestamp=int(create_time),
                                    source=self.source, tags=self.tags,
                                    default_source=self.source))
        print(int(time.time()), " ", metrics)
        self.client.send_metric_now(metrics)


class WavefrontProxyRecorder(WavefrontRecorder):
    def __init__(self, source, host, port=2878, tags=None):
        super().__init__(source, tags)
        self.host = host
        self.port = port

    @property
    def client(self):
        if not self._client:
            self._client = WavefrontProxyClient(
                host=self.host, metrics_port=self.port,
                distribution_port=None, tracing_port=None)
        return self._client

    def handle(self, messages):
        super().handle(messages)


class WavefrontDirectRecorder(WavefrontRecorder):

    def __init__(self, server, token, source, tags=None):
        super().__init__(source, tags)
        self.server = server
        self.token = token

    @property
    def client(self):
        if self._client is None:
            self._client = WavefrontDirectClient(
                self.server, self.token, batch_size=10000)
        return self._client

    def handle(self, messages):
        super().handle(messages)
        self._client.flush_now()