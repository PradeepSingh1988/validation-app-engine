#!/usr/bin/env python
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.

'''
Base class for all Apps on the platform.
'''

import abc
from functools import wraps

app_registry = {}


class BaseApp(abc.ABC):

    NAME = ''

    @property
    def name(self):
        """
        Returns name of the App.
        """
        assert self.NAME, "Invalid Name for app"
        return self.NAME

    @abc.abstractmethod
    def initialize(self):
        pass

    @abc.abstractmethod
    def shutdown(self):
        pass


class StateLessBaseApp(BaseApp):

    def initialize(self):
        pass

    def shutdown(self):
        pass


class exposed(object):
    def __init__(self, func):
        self.func = func


def exposify(cls):
    for k, v in list(cls.__dict__.items()):
        if isinstance(v, exposed):
            setattr(cls, "exposed_%s" % (k,), v.func)
    return cls
