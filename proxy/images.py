import copy
import httplib
import logging
import uuid

import requests
from webob import exc

from functools import wraps
from ovirt_imageio_proxy.http_helper import (
    addcors,
    success_codes as http_success_codes,
)
import auth
from ovirt_imageio_proxy import config
import server

from ovirt_imageio_common import web
from ovirt_imageio_proxy.images import RequestHandler as ImageIoRequestHandler
from helpers import requiresession


class RequestHandler(ImageIoRequestHandler):
    """
    Request handler for the /images/ resource.
    """

    def __init__(self, config, request, clock=None):
        """
        :param config: config.py
        :param request: http request
        :return:
        """
        super(RequestHandler, self).__init__(config, request, clock)

    @requiresession
    @addcors
    def post(self, res_id):
        resource_id = self.get_resource_id(self.request)
        imaged_url = self.get_imaged_url_post(self.request)

        headers = self.get_default_headers(resource_id)

        body = self.request.body
        stream = False
        logging.debug("Resource %s: transferring to backup media",
                      resource_id)
        imaged_response = self.make_imaged_request(
            self.request.method, imaged_url, headers, body, stream, connection_timeout=10, read_timeout=120)

        response = server.response(imaged_response.status_code)
        response.headers = copy.deepcopy(imaged_response.headers)
        response.headers['Cache-Control'] = 'no-cache, no-store'

        return response

    def get_imaged_url_post(self, request):
        uri = auth.get_session_attribute(request, auth.SESSION_IMAGED_HOST_URI)
        ticket = auth.get_session_attribute(
            request, auth.SESSION_TRANSFER_TICKET)
        return "{}/images/{}".format(uri, ticket)

    def get_resource_id(self, request):
        resource_id = request.path_info_pop()
        if request.path_info:
            # No extra url path allowed!
            raise exc.HTTPBadRequest("Invalid resource path")

        # The requested image resource must match the one in the ticket
        try:
            uuid.UUID(resource_id)
        except ValueError:
            raise exc.HTTPBadRequest(
                "Invalid format for requested resource or no resource specified"
            )
        if (resource_id != auth.get_session_attribute(
                request, auth.SESSION_TRANSFER_TICKET)):
            raise exc.HTTPBadRequest(
                "Requested resource must match transfer ticket"
            )
        return resource_id

