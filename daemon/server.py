# ovirt-imageio-daemon
# Copyright (C) 2015-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

from __future__ import absolute_import

import celery
import json
import logging
import logging.config
import os
import signal
import sys
import time
import ConfigParser
import io
from wsgiref import simple_server

from six.moves import socketserver

import systemd.daemon
import webob

from webob.exc import (
    HTTPBadRequest,
    HTTPNotFound,
)

from ovirt_imageio_common import configloader
from ovirt_imageio_common import directio
from ovirt_imageio_common import errors
from ovirt_imageio_common import ssl
from ovirt_imageio_common import util
from ovirt_imageio_common import version
from ovirt_imageio_common import web
from ovirt_imageio_daemon import server as imageio_server

from ovirt_imageio_daemon import config
from ovirt_imageio_daemon import pki
from ovirt_imageio_daemon import uhttp
from ovirt_imageio_daemon import tickets
from ovirt_imageio_daemon import wsgi
from ovirt_imageio_daemon import profile

import celery_tasks
import nfs_mount

CONF_DIR = "/etc/ovirt-imageio-daemon"

log = logging.getLogger("server")
remote_service = None
local_service = None
control_service = None
running = True


def main(args):
    configure_logger()
    try:
        log.info("Starting (pid=%s, version=%s)", os.getpid(), version.string)
        configloader.load(config, [os.path.join(CONF_DIR, "daemon.conf")])
        signal.signal(signal.SIGINT, terminate)
        signal.signal(signal.SIGTERM, terminate)
        start(config)
        try:
            systemd.daemon.notify("READY=1")
            log.info("Ready for requests")
            while running:
                time.sleep(30)
        finally:
            stop()
        log.info("Stopped")
    except Exception:
        log.exception(
            "Service failed (remote_service=%s, control_service=%s, running=%s)",
            remote_service, control_service, running)
        sys.exit(1)


def configure_logger():
    imageio_server.configure_logger()


def terminate(signo, frame):
    global running
    imageio_server.terminate(signo, frame)
    running = False

def start(config):

    global remote_service, local_service, control_service
    assert not (remote_service or local_service or control_service)

    log.debug("Starting remote service on port %d", config.images.port)
    remote_service = RemoteService(config)
    remote_service.start()

    log.debug("Starting local service on socket %r", config.images.socket)
    local_service = LocalService(config)
    local_service.start()

    log.debug("Starting control service on socket %r", config.tickets.socket)
    control_service = ControlService(config)
    control_service.start()


def stop():
    imageio_server.stop()

def response(status=200, payload=None):
    """
    Return WSGI application for sending response in JSON format.
    """
    body = json.dumps(payload) if payload else ""
    return webob.Response(status=status, body=body,
                          content_type="application/json")

class RemoteService(imageio_server.RemoteService):

    def __init__(self, config):
        self._config = config
        self._server = wsgi.WSGIServer(
            (config.images.host, config.images.port),
            wsgi.WSGIRequestHandler)
        if config.images.port == 0:
            config.images.port = self.port
        self._secure_server()
        app = web.Application(config, [(r"/images/(.*)", Images),
                                       (r"/tasks/(.*)", Tasks)])
        self._server.set_app(app)
        log.debug("%s listening on port %d", self.name, self.port)


class LocalService(imageio_server.LocalService):
    """
    Service used to access images locally.

    Access to this service requires a valid ticket that can be installed using
    the control service.
    """

    name = "local.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.UnixWSGIServer(
            config.images.socket, uhttp.UnixWSGIRequestHandler)
        if config.images.socket == "":
            config.images.socket = self.address
        app = web.Application(config, [(r"/images/(.*)", Images),
                                       (r"/tasks/(.*)", Tasks)])
        self._server.set_app(app)
        log.debug("%s listening on %r", self.name, self.address)


class ControlService(imageio_server.ControlService):
    """
    Service used to control imageio daemon on a host.

    The service is using unix socket owned by a program managing the host. Only
    this program can access the socket.
    """

    name = "control.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.UnixWSGIServer(
            config.tickets.socket, uhttp.UnixWSGIRequestHandler)
        if config.tickets.socket == "":
            config.tickets.socket = self.address
        app = web.Application(config, [
            (r"/tickets/(.*)", imageio_server.Tickets),
            (r"/profile/", profile.Handler)])
        self._server.set_app(app)
        log.debug("%s listening on %r", self.name, self.address)

class Images(imageio_server.Images):
    """
    Request handler for the /images/ resource.
    """
    log = logging.getLogger("images")

    def __init__(self, config, request, clock=None):
        super(Images, self).__init__(config, request, clock)

    def post(self, ticket_id):
        if not ticket_id:
            raise HTTPBadRequest("Ticket id is required")

        with open(os.path.join(CONF_DIR, "daemon.conf")) as f:
            sample_config = f.read()
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        config.readfp(io.BytesIO(sample_config))

        nfsshare = config.get('nfs_config', 'nfs_share')
        mountpath = config.get('nfs_config', 'mount_path')
        if not nfs_mount.is_mounted(nfsshare, mountpath):
            if not nfs_mount.mount_backup_target(nfsshare, mountpath):
                raise HTTPBadRequest("Backup target not mounted.")

        body = self.request.body
        methodargs = json.loads(body)
        if not 'backup_path' in methodargs:
            raise HTTPBadRequest("Malformed request. Requires backup_path in the body")

        destdir = os.path.split(methodargs['backup_path'])[0]
        if not os.path.exists(destdir):
            raise HTTPBadRequest("Backup_path does not exists")

        # TODO: cancel copy if ticket expired or revoked
        if methodargs['method'] == 'backup':
            offset = 0
            size = None
            if self.request.range:
                offset = self.request.range.start
                if self.request.range.end is not None:
                    size = self.request.range.end - offset

            ticket = tickets.authorize(ticket_id, "read", 0, size)
            ticket.extend(1800)
            self.log.debug("disk %s to %s for ticket %s",
                           body, ticket.url.path, ticket_id)
            try:
                ctask = celery_tasks.backup.apply_async((ticket_id,
                                                        ticket.url.path,
                                                        methodargs['backup_path'],
                                                        tickets.get(ticket_id).size,
                                                        methodargs['type'],
                                                        self.config.daemon.buffer_size,
                                                        methodargs['recent_snap_id']),
                                                        #queue='backup_tasks',
                                                        retry=True,
                                                        retry_policy={
                                                            'max_retries': 3,
                                                            'interval_start': 3,
                                                            'interval_step': 0.5,
                                                            'interval_max': 0.5,
                                                        })
            except celery_tasks.backup.OperationalError as exc:
                self.log.info("Submitting celery task raised: %r", exc)
            print "Submitted backup"
        elif methodargs['method'] == 'restore':
            size = self.request.content_length
            if size is None:
                raise HTTPBadRequest("Content-Length header is required")
            if size < 0:
                raise HTTPBadRequest("Invalid Content-Length header: %r" % size)
            content_range = web.content_range(self.request)
            offset = content_range.start or 0

            ticket = tickets.authorize(ticket_id, "write", offset, size)
            ticket.extend(1800)

            self.log.debug("disk %s to %s for ticket %s",
                           body, ticket.url.path, ticket_id)
            try:
                ctask = celery_tasks.restore.apply_async((ticket_id,
                                                    ticket.url.path,
                                                    methodargs['backup_path'],
                                                    methodargs['disk_format'],
                                                    tickets.get(ticket_id).size,
                                                    self.config.daemon.buffer_size),
                                                    #queue='restore_tasks',
                                                    retry = True,
                                                    retry_policy={
                                                        'max_retries': 3,
                                                        'interval_start': 3,
                                                        'interval_step': 0.5,
                                                        'interval_max': 0.5,
                                                    })
            except celery_tasks.backup.OperationalError as exc:
                self.log.info("Submitting celery task raised: %r", exc)
            print "Submitted restore"
        else:
            raise HTTPBadRequest("Invalid method")

        r = response(status=206)
        r.headers["Location"] = "/tasks/%s" % ctask.id
        return r

class Tasks(object):
    """
    Request handler for the /tasks/ resource.
    """
    log = logging.getLogger("tasks")

    def __init__(self, config, request, clock=None):
        self.config = config
        self.request = request
        self.clock = clock

    def get(self, task_id):
        if not task_id:
            raise HTTPBadRequest("Task id is required")

        self.log.info("Retrieving task %s", task_id)
        result = {}
        try:
            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)
            if ctasks.result:
                result = ctasks.result
            if isinstance(result, Exception):
                result = {'Exception': result.message}
            result['status'] = ctasks.status
            #ctasks.get()
        except KeyError:
            raise HTTPNotFound("No such task %r" % task_id)
        except Exception as e:
            raise Exception(e.message)
        return response(payload=result)


class ThreadedWSGIServer(socketserver.ThreadingMixIn,
                         simple_server.WSGIServer):
    """
    Threaded WSGI HTTP server.
    """
    daemon_threads = True


class WSGIRequestHandler(simple_server.WSGIRequestHandler):
    """
    WSGI request handler using HTTP/1.1.
    """

    protocol_version = "HTTP/1.1"

    def address_string(self):
        """
        Override to avoid slow and unneeded name lookup.
        """
        return self.client_address[0]

    def handle(self):
        """
        Override to use fixed ServerHandler.
        Copied from wsgiref/simple_server.py, using our ServerHandler.
        """
        self.raw_requestline = self.rfile.readline(65537)
        if len(self.raw_requestline) > 65536:
            self.requestline = ''
            self.request_version = ''
            self.command = ''
            self.send_error(414)
            return

        if not self.parse_request():  # An error code has been sent, just exit
            return

        handler = ServerHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())

    def log_message(self, format, *args):
        """
        Override to avoid unwanted logging to stderr.
        """


class ServerHandler(simple_server.ServerHandler):

    # wsgiref handers ignores the http request handler's protocol_version, and
    # uses its own version. This results in requests returning HTTP/1.0 instead
    # of HTTP/1.1 - see https://bugzilla.redhat.com/1512317
    #
    # Looking at python source we need to define here:
    #
    #   http_version = "1.1"
    #
    # Bug adding this break some tests.
    # TODO: investigate this.

    def write(self, data):
        """
        Override to allow writing buffer object.
        Copied from wsgiref/handlers.py, removing the check for StringType.
        """
        if not self.status:
            raise AssertionError("write() before start_response()")

        elif not self.headers_sent:
            # Before the first output, send the stored headers
            self.bytes_sent = len(data)    # make sure we know content-length
            self.send_headers()
        else:
            self.bytes_sent += len(data)

        self._write(data)
        self._flush()


class UnixWSGIRequestHandler(uhttp.UnixWSGIRequestHandler):
    """
    WSGI over unix domain socket request handler using HTTP/1.1.
    """
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        """
        Override to avoid unwanted logging to stderr.
        """
