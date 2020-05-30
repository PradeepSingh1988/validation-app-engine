#!/usr/bin/env python
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.

from axon.apps.base import app_registry, exposed, exposify, StateLessBaseApp
from axon.utils.network_utils import NamespaceManager


@exposify
class NamespaceApp(StateLessBaseApp):

    NAME = "NAMESPACE"

    def __init__(self):
        self._ns_manager = NamespaceManager()

    @exposed
    def list_namespaces(self):
        return self._ns_manager.get_all_namespaces()

    @exposed
    def get_namespace(self, namespace):
        return self._ns_manager.get_namespace(namespace)

    @exposed
    def list_namespaces_ips(self):
        return self._ns_manager.get_all_namespaces_ips()


app_registry[NamespaceApp.NAME] = NamespaceApp
