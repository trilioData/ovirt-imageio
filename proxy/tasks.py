# ovirt-imageio-proxy
# Copyright (C) 2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import httplib
import logging
import requests
from webob import exc

from ovirt_imageio_common import web

import auth
import server

from ovirt_imageio_proxy import version
from ovirt_imageio_proxy.http_helper import (
    addcors,
    success_codes as http_success_codes,
)
from functools import wraps
from helpers import requiresession


class RequestHandler(object):
    """
    Request handler for the /tasks/ resource.
    """

    def __init__(self, config, request, clock=None):
        """
        Arguments:
            config (config object): proxy configuration
            request (webob.Request): underlying http request
        """
        self.config = config
        self.request = request
        self.clock = clock

    def get_imaged_url(self, request, task_id):
        uri = auth.get_session_attribute(request, auth.SESSION_IMAGED_HOST_URI)
        ticket = auth.get_session_attribute(
            request, auth.SESSION_TRANSFER_TICKET)
        return "{}/tasks/{}".format(uri, task_id)

    def make_imaged_request(self, method, imaged_url, headers, body, stream):
        timeout = (self.config.imaged_connection_timeout_sec,
                   self.config.imaged_read_timeout_sec)

        logging.debug("Connecting to host at %s", imaged_url)
        logging.debug("Outgoing headers to host:\n" +
                      '\n'.join(('  {}: {}'.format(k, headers[k])
                                 for k in sorted(headers))))

        try:
            # TODO Pool requests, keep the session somewhere?
            # TODO Otherwise, we can use request.prepare()
            print method, imaged_url, headers, body
            imaged_session = requests.Session()
            imaged_req = requests.Request(
                method, imaged_url, headers=headers, data=body)
            imaged_req.body_file = body
            # TODO log the request to vdsm
            imaged_prepped = imaged_session.prepare_request(imaged_req)
            imaged_resp = imaged_session.send(
                imaged_prepped, verify=self.config.engine_ca_cert_file,
                timeout=timeout, stream=stream)
        except requests.Timeout:
            s = "Timed out connecting to host"
            raise exc.HTTPGatewayTimeout(s)
        except requests.URLRequired:
            s = "Invalid host URI for host"
            raise exc.HTTPBadRequest(s)
        except requests.ConnectionError as e:
            s = "Failed communicating with host: " + e.__doc__
            logging.error(s, exc_info=True)
            raise exc.HTTPServiceUnavailable(s)
        except requests.RequestException as e:
            s = "Failed communicating with host: " + e.__doc__
            logging.error(s, exc_info=True)
            raise exc.HTTPInternalServerError(s)

        # logging.debug("Incoming headers from host:\n" +
        #               '\n'.join(('  {}: {}'
        #                          .format(k, imaged_resp.headers.get(k))
        #                          for k in sorted(imaged_resp.headers))))

        if imaged_resp.status_code not in http_success_codes:
            # Don't read the whole body, in case something went really wrong...
            s = next(imaged_resp.iter_content(256, False), "(empty)")
            logging.error("Failed: %s", s)
            # TODO why isn't the exception logged somewhere?
            raise exc.status_map[imaged_resp.status_code](
                "Failed response from host: {}".format(s))

        logging.debug(
            "Successful request to host: HTTP %d %s",
            imaged_resp.status_code,
            httplib.responses[imaged_resp.status_code]
        )
        return imaged_resp

    @requiresession
    @addcors
    def get(self, task_id):
        imaged_url = self.get_imaged_url(self.request, task_id)
        headers = {}
        body = ""
        stream = True  # Don't let Requests read entire body into memory

        imaged_response = self.make_imaged_request(
            self.request.method, imaged_url, headers, body, stream)

        response = server.response(status=imaged_response.status_code,
                                   message=imaged_response.raw.data)
        return response
