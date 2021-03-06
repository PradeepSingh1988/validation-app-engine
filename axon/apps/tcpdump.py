#!/usr/bin/env python
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: BSD-2 License
# The full license information can be found in LICENSE.txt
# in the root directory of this project.
'''
App for running tcpdump tool and generating .pcap files.
'''
import logging
import os

from axon.apps.base import app_registry, exposed, exposify
import axon.apps.console as console


log = logging.getLogger(__name__)


class TCPDumpRerunError(Exception):
    pass


@exposify
class TCPDump(console.Console):

    NAME = "TCPDUMP"

    def __init__(self):
        self._pcap_handles = {}  # a <str: subprocess.Popen> pair

    def _get_identifier(self, dst_file):
        """
        Maintains identifier scheme for identifying each tcpdump request
        uniquely.
        """
        return os.path.join('/tmp', dst_file)

    def _get_pcap_handle(self, dst_file):
        '''
        Return subprocess.Popen Handle which is running the corresponding
        subprocess for a packet capture request.
        '''
        dst_file = self._get_identifier(dst_file)
        return self._pcap_handles.get(dst_file, None)

    @exposed
    def start_pcap(self, dst_file, interface='eth0', args=''):
        """
        Starts Packet Capture with 'tcpdump' command for given params.
        """
        if self._get_pcap_handle(dst_file):
            msg = "A tcpdump directing to %s is already running" % dst_file
            raise TCPDumpRerunError(msg)

        dst_file = self._get_identifier(dst_file)
        cmnd = 'tcpdump -i %s %s -w %s' % (interface, args, dst_file)
        p = self._start_subprocess(cmnd)
        self._pcap_handles[dst_file] = p

    @exposed
    def stop_pcap(self, dst_file):
        """
        Stops packet capture for the destination file `dst_file`.
        """
        ident = self._get_identifier(dst_file)
        proc = self._get_pcap_handle(ident)
        if proc:
            self._kill_subprocess(proc)
            self._get_pcap_handles.pop(ident)

    @exposed
    def is_running(self, dst_file):
        ident = self._get_identifier(dst_file)
        proc = self._get_pcap_handle(ident)
        return self._is_alive(proc)

    @exposed
    def stop(self):
        """
        Stops PCAP app.
        """
        for ident, proc in self._pcap_handles.items():
            log.info("Stopping packet capture for %s", ident)
            self._kill_subprocess(proc, close_fds=True)


app_registry[TCPDump.NAME] = TCPDump
