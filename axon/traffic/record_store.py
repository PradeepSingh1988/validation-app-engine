import time

from sqlalchemy import (
    Boolean, create_engine, Column, Float, Integer,
    MetaData, select, Table, Unicode)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func


class TrafficRecordStore(object):
    """
    Stores Trafficstats in a database table using SQLAlchemy.
    """

    def __init__(self, url, engine_options=None):
        metadata = MetaData()
        self.engine = create_engine(url, **(engine_options or {}))
        self._records_table = self._init_records_table(metadata)
        self._failures_table = self._init_failures_table(metadata)
        self._records_table.create(self.engine, True)
        self._failures_table.create(self.engine, True)

    @staticmethod
    def _init_records_table(metadata):
        table = Table(
            'trafficrecords', metadata,
            Column('id', Unicode, primary_key=True),
            Column('source', Unicode, nullable=False, index=True),
            Column('destination', Unicode, nullable=False, index=True),
            Column('port', Integer, nullable=False, index=True),
            Column('protocol', Unicode, nullable=False, index=True),
            Column('connected', Boolean, default=True),
            Column('success_count', Integer, default=0),
            Column('failure_count', Integer, default=0),
            Column('created', Float(25), index=True)
        )
        return table

    @staticmethod
    def _init_failures_table(metadata):
        table = Table(
            'trafficfailures', metadata,
            Column('id', Unicode, primary_key=True),
            Column('source', Unicode, nullable=False, index=True),
            Column('destination', Unicode, nullable=False, index=True),
            Column('port', Integer, nullable=False, index=True),
            Column('protocol', Unicode, nullable=False, index=True),
            Column('connected', Boolean, nullable=False, default=True),
            Column('created', Float(25), index=True)
        )
        return table

    def add_traffic_failures(self, record):
        add = self._failures_table.insert.values(**record)
        try:
            self.engine.execute(add)
        except IntegrityError:
            raise

    def add_failures_batch(self, records):
        self.engine.execute(
            self._failures_table.insert(), [record for record in records])

    def add_traffic_recod(self, record):
        add = self._records_table.insert.values(record.as_dict())
        try:
            self.engine.execute(add)
        except IntegrityError:
            raise

    def add_records_batch(self, records):
        self.engine.execute(
            self._records_table.insert(),
            [record.as_dict() for record in records])

    def __where_record_query(self, query, source=None, destination=None,
                             port=None, protocol=None,
                             start_time=None, end_time=None):
        if source is not None:
            query = query.where(
                self._records_table.c.source == source)
        if port is not None:
            query = query.where(
                self._records_table.c.port == port)
        if protocol is not None:
            query = query.where(
                self._records_table.c.protocol == protocol)
        if destination is not None:
            query = query.where(
                self._records_table.c.destination == destination)
        if start_time is not None:
            query = query.where(
                self._records_table.c.created <= end_time)
        if end_time is not None:
            query = query.where(
                self._records_table.c.created >= start_time)
        return query

    def get_failures_detail(self, source=None, destination=None,
                            port=None, protocol=None,
                            start_time=None, end_time=None):
        selectables = select([self._failures_table])
        start_time, end_time = self.__get_time_filters(start_time, end_time)
        selectables = self.__where_record_query(
            selectables, source, destination,
            port, protocol, start_time, end_time)
        failures = self.engine.execute(selectables).fetchall()
        return failures

    def get_records(self, source=None, destination=None,
                    port=None, protocol=None):
        selectables = select([self._records_table])
        selectables = self.__where_record_query(
            selectables, source, destination, port, protocol)
        records = self.engine.execute(selectables).fetchall()
        return records

    def __get_time_filters(self, start_time=None, end_time=None):
        if start_time is None and end_time is None:
            end_time = time.time()
            start_time = end_time - 300
        elif start_time is not None and end_time is not None:
            end_time = end_time
            start_time = start_time
        elif end_time is not None:
            start_time = end_time - 300
        elif start_time is not None:
            end_time = time.time()
        return start_time, end_time

    def get_traffic_stats(self, source=None, destination=None,
                          port=None, protocol=None,
                          start_time=None, end_time=None):
        selectables = select(
            [func.sum(self._records_table.c.success_count),
             func.sum(self._records_table.c.failure_count)])
        start_time, end_time = self.__get_time_filters(start_time, end_time)
        selectables = self.__where_record_query(
            selectables, source, destination,
            port, protocol, start_time, end_time)
        failure_count = self.engine.execute(selectables).fetchall()
        return failure_count

    def get_traffic_failures(self, source=None, destination=None,
                             port=None, protocol=None,
                             start_time=None, end_time=None):
        selectables = select([self._records_table.c.source,
                              self._records_table.c.destination,
                              self._records_table.c.port,
                              self._records_table.c.protocol,
                              self._records_table.c.created,
                              self._records_table.c.failure_count])
        selectables = self.__where_record_query(
            selectables, source, destination, port, protocol)
        start_time, end_time = self.__get_time_filters(start_time, end_time)
        selectables = self.__where_record_query(
            selectables, source, destination,
            port, protocol, start_time, end_time)
        selectables = selectables.where(
            self._records_table.c.failure_count > 0)
        records = self.engine.execute(selectables).fetchall()
        return records

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
