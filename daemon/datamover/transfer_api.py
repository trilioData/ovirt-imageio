# Copyright (c) 2015 TrilioData, Inc.
# All Rights Reserved.

from flask import Blueprint, request, send_from_directory
from flask.views import MethodView
from webob import exc
from ovirt_utils import backup
tvm_blueprint = Blueprint("tvm_blueprint", __name__, url_prefix="/v1/admin")


class Tvm(MethodView):
    """The file search API controller for the workload manager API."""
    def snapshot_download(self, ticket_id):
        try:
            body = request.get_json()
            proxy_url = body['proxy_url']
            type = body['type']
            backup_path = body['backup_path']

            recent_snap_ids = body['recent_snap_id']

            print(f"Vm id received in api {proxy_url, type, backup_path ,recent_snap_ids,ticket_id }")
            backup(proxy_url, type, backup_path ,recent_snap_ids, ticket_id)

            return {"message":"backup job submitted"}
        except Exception as error:
            raise exc.HTTPServerError(explanation=str(error))

tvm_blueprint.add_url_rule("snapshot_download/<ticket_id>", view_func=Tvm().snapshot_download, methods=["POST"])