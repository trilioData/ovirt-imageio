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

import subprocess
from six.moves import socketserver

import systemd.daemon
import webob

from webob.exc import (
    HTTPBadRequest,
    HTTPNotFound,
)

from ovirt_imageio_common import configloader
from ovirt_imageio_common import http
from ovirt_imageio_common import ssl
from ovirt_imageio_common import util
from ovirt_imageio_common import version
from ovirt_imageio_common import errors
from ovirt_imageio_daemon import server as imageio_server

from ovirt_imageio_daemon import config
from ovirt_imageio_daemon import pki
from ovirt_imageio_daemon import uhttp
from ovirt_imageio_daemon import tickets
from ovirt_imageio_daemon import auth
from ovirt_imageio_daemon import profile
from ovirt_imageio_daemon import images
from ovirt_imageio_daemon import version as daemon_version

from celery.task.control import revoke
import celery_tasks
import nfs_mount

CONF_DIR = "/etc/ovirt-imageio-daemon"

log = logging.getLogger("server")
remote_service = None
local_service = None
control_service = None
running = True
high_version = int(daemon_version.string.split(".")[0])
major_version = int(daemon_version.string.split(".")[1])
minor_version = int(daemon_version.string.split(".")[2])

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
    conf = os.path.join(CONF_DIR, "logger.conf")
    logging.config.fileConfig(conf, disable_existing_loggers=False)


def terminate(signo, frame):
    global running
    log.info("Received signal %d, shutting down", signo)
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
    global remote_service, local_service, control_service
    log.debug("Stopping services")
    remote_service.stop()
    local_service.stop()
    control_service.stop()
    remote_service = None
    local_service = None


class Service(object):

    name = None

    def start(self):
        util.start_thread(self._run, name=self.name)

    def stop(self):
        log.debug("Stopping %s", self.name)
        self._server.shutdown()

    @property
    def port(self):
        return self._server.server_port

    @property
    def address(self):
        return self._server.server_address

    def _run(self):
        log.debug("Starting %s", self.name)
        self._server.serve_forever(
            poll_interval=self._config.daemon.poll_interval)
        log.debug("%s terminated normally", self.name)

#control_service = None

def response(status=200, payload=None):
    """
    Return WSGI application for sending response in JSON format.
    """
    body = json.dumps(payload) if payload else ""
    return webob.Response(status=status, body=body,
                          content_type="application/json")


class RemoteService(Service):
    """
    Service used to access images data from remote host.
    Access to this service requires a valid ticket that can be installed using
    the local control service.
    """

    name = "remote.service"

    def __init__(self, config):
        self._config = config
        self._server = http.Server(
            (config.images.host, config.images.port),
            http.Connection)
        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = util.Clock
        if config.images.port == 0:
            config.images.port = self.port
        self._secure_server()
        self._server.app = http.Router([
            (r"/images/(.*)", Images(config)),
            (r"/tasks/(.*)", Tasks(config)),
            (r"/ping", Ping(config))
        ])

        log.debug("%s listening on port %d", self.name, self.port)

    def _secure_server(self):
        key_file = pki.key_file(self._config)
        cert_file = pki.cert_file(self._config)
        log.debug("Securing server (certfile=%s, keyfile=%s)",
                  cert_file, key_file)
        context = ssl.server_context(
            cert_file, cert_file, key_file,
            enable_tls1_1=self._config.daemon.enable_tls1_1)
        self._server.socket = context.wrap_socket(
            self._server.socket, server_side=True)


class LocalService(Service):
    """
    Service used to access images locally.

    Access to this service requires a valid ticket that can be installed using
    the control service.
    """

    name = "local.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.Server(
            config.images.socket, uhttp.Connection)
        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = util.Clock
        if config.images.socket == "":
            config.images.socket = self.address
        self._server.app = http.Router([
            (r"/images/(.*)", Images(config)),
            (r"/tasks/(.*)", Tasks(config)),
        ])
        log.debug("%s listening on %r", self.name, self.address)


class ControlService(Service):
    """
    Service used to control imageio daemon on a host.

    The service is using unix socket owned by a program managing the host. Only
    this program can access the socket.
    """

    name = "control.service"

    def __init__(self, config):
        self._config = config
        self._server = uhttp.Server(
            config.tickets.socket, uhttp.Connection)
        # TODO: Make clock configurable, disabled by default.
        self._server.clock_class = util.Clock
        if config.tickets.socket == "":
            config.tickets.socket = self.address
        self._server.app = http.Router([
            (r"/tickets/(.*)", tickets.Handler(config)),
            (r"/profile/", profile.Handler(config)),
        ])

        log.debug("%s listening on %r", self.name, self.address)


class Images(images.Handler):
    """
    Request handler for the /images/ resource.
    """
    log = logging.getLogger("images")

    def __init__(self, config):
        super(Images, self).__init__(config)

    def check_celery_status(self):
        celery_service_status = os.system("service ovirt_celery status")
        if celery_service_status != 0:
            return False
        return True

    def post(self, req, resp, ticket_id):
        if not ticket_id:
            raise http.Error(http.BAD_REQUEST, "Ticket id is required")

        with open(os.path.join(CONF_DIR, "daemon.conf")) as f:
            sample_config = f.read()
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        config.readfp(io.BytesIO(sample_config))

        nfsshare = config.get('nfs_config', 'nfs_share')
        mountpath = config.get('nfs_config', 'mount_path')
        if not nfs_mount.is_mounted(nfsshare, mountpath):
            if not nfs_mount.mount_backup_target(nfsshare, mountpath):
                raise http.Error(http.BAD_REQUEST, "Backup target not mounted.")

        body = req.read()
        methodargs = json.loads(body)
        if not 'backup_path' in methodargs:
            raise http.Error(http.BAD_REQUEST, "Malformed request. Requires backup_path in the body")

        destdir = os.path.split(methodargs['backup_path'])[0]
        if not os.path.exists(destdir):
            raise http.Error(http.BAD_REQUEST, "Backup_path does not exists")

        # TODO: cancel copy if ticket expired or revoked
        if methodargs['method'] == 'backup':
            celery_status = self.check_celery_status()
            if not celery_status:
                err_msg = "Celery workers seems to be down at the moment. Unable to perform snapshot."\
                    " Kindly contact your administrator."
                raise http.Error(http.INTERNAL_SERVER_ERROR, err_msg)
            offset = req.content_range.first if req.content_range else 0
            size = req.content_length
            if size is None:
                raise http.Error(http.BAD_REQUEST, "Content-Length header is required")
            if size < 0:
                raise http.Error(http.BAD_REQUEST, "Invalid Content-Length header: %r" % size)

            try:
                if high_version >= 1 and major_version >=5 and minor_version >= 2:
                    ticket = auth.authorize(ticket_id, "read")
                else:
                    ticket = auth.authorize(ticket_id, "read", offset, size)
                # ticket.extend(1800)
            except errors.AuthorizationError as e:
                raise http.Error(http.FORBIDDEN, str(e))
            self.log.debug("disk %s to %s for ticket %s",
                           body, ticket.url.path, ticket_id)
            try:
                ctask = celery_tasks.backup.apply_async((ticket_id,
                                                        ticket.url.path,
                                                        methodargs['backup_path'],
                                                        ticket.size,
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
            self.log.info("Backup Job Submitted")
        elif methodargs['method'] == 'restore':
            celery_status = self.check_celery_status()
            if not celery_status:
                err_msg = "Celery workers seems to be down at the moment. Unable to perform restore." \
                          " Kindly contact your administrator."
                raise http.Error(http.INTERNAL_SERVER_ERROR, err_msg)
            size = req.content_length
            if size is None:
                raise http.Error(http.BAD_REQUEST, "Content-Length header is required")
            if size < 0:
                raise http.Error(http.BAD_REQUEST, "Invalid Content-Length header: %r" % size)
            offset = req.content_range.first if req.content_range else 0

            try:
                if high_version >= 1 and major_version >=5 and minor_version >= 2:
                    ticket = auth.authorize(ticket_id, "read")
                else:
                    ticket = auth.authorize(ticket_id, "read", offset, size)
                # ticket.extend(1800)
            except errors.AuthorizationError as e:
                raise http.Error(http.FORBIDDEN, str(e))

            self.log.debug("disk %s to %s for ticket %s",
                           body, ticket.url.path, ticket_id)
            try:
                ctask = celery_tasks.restore.apply_async((ticket_id,
                                                    ticket.url.path,
                                                    methodargs['backup_path'],
                                                    methodargs['disk_format'],
                                                    ticket.size,
                                                    self.config.daemon.buffer_size,
                                                    methodargs['restore_size'],
                                                    methodargs['actual_size']),
                                                    retry = True,
                                                    retry_policy={
                                                        'max_retries': 3,
                                                        'interval_start': 3,
                                                        'interval_step': 0.5,
                                                        'interval_max': 0.5,
                                                    })
            except celery_tasks.backup.OperationalError as exc:
                self.log.info("Submitting celery task raised: %r", exc)
            self.log.info("Restore Job Submitted")
        else:
            raise http.Error(http.BAD_REQUEST, "Invalid method")
        resp.status_code = 206
        resp.headers["Location"] = "/tasks/%s" % ctask.id
        return resp


class Tasks(object):
    """
    Request handler for the /tasks/ resource.
    """
    log = logging.getLogger("tasks")

    def __init__(self, config):
        self.config = config

    def check_celery_status(self):
        celery_service_status = os.system("service ovirt_celery status")
        if celery_service_status != 0:
            return False
        return True

    def get(self, req, resp, task_id):
        if not task_id:
            raise http.Error(http.BAD_REQUEST, "Task id is required")

        self.log.info("Retrieving task %s", task_id)
        celery_status = self.check_celery_status()
        if not celery_status:
            err_msg = "Celery workers seems to be down at the moment. Unable to retrieve task " \
                      " Kindly contact your administrator."
            self.log.info(err_msg)
            raise http.Error(http.INTERNAL_SERVER_ERROR, err_msg)
        result = {}
        try:
            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)
            if ctasks.result:
                result = ctasks.result
            if isinstance(result, Exception):
                result = {'Exception': result.message}
            result['status'] = ctasks.status
        except KeyError:
            raise http.Error(http.NOT_FOUND, "No such task %r" % task_id)
        except Exception as e:
            raise Exception(e.message)
        return resp.send_json(result)
    
    def delete(self, req, resp, task_id):
        if not task_id:
            raise http.Error(http.BAD_REQUEST, "Task id is required")

        self.log.info("Stopping execution of task with id: %s", task_id)
        result = {}
        try:
            result = revoke(task_id, terminate=True)
            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)
            if isinstance(result, Exception):
                result = {'Exception': result.message}
            result['status'] = ctasks.status
            #ctasks.get()
        except KeyError:
            raise http.Error(http.BAD_REQUEST, "No such task %r" % task_id)
        except Exception as e:
            raise Exception(e.message)
        return resp.send_json(result)

class Ping(object):
    """
    Request handler for the /ping resource.
    """
    log = logging.getLogger("ping")

    def __init__(self, config):
        self.config = config

    def get(self, req, resp):
        result = {"status": "Configured"}
        return resp.send_json(result)