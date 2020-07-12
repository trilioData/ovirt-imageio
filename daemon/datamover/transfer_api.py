# Copyright (c) 2015 TrilioData, Inc.
# All Rights Reserved.
import os
from flask import Blueprint, request
from flask.views import MethodView
from webob import exc
import celery
import celery_tasks

tvm_blueprint = Blueprint("tvm_blueprint", __name__, url_prefix="/v1/admin")


class Tvm(MethodView):
    """The file search API controller for the workload manager API."""

    def snapshot_download(self, ticket_id):
        try:
            body = request.get_json()
            if "backup" == body['method']:
                proxy_url = body['proxy_url']
                type = body['type']
                backup_path = body['backup_path']
                recent_snap_ids = body['recent_snap_id']
                print(f"Vm id received in api {proxy_url, type, backup_path, recent_snap_ids, ticket_id}")
                task = celery_tasks.backup.apply_async((proxy_url, type, backup_path, recent_snap_ids, ticket_id),
                                   retry=True,
                                   retry_policy={
                                       'max_retries': 3,
                                       'interval_start': 3,
                                       'interval_step': 0.5,
                                       'interval_max': 0.5,
                                   })

                return {"message": "backup job submitted","task_id":task.id}
            else:
                restore_size = body['restore_size']
                actual_size = body['actual_size']
                disk_format = body['disk_format']

                backup_path = body['backup_path']
                task = celery_tasks.restore.apply_async((ticket_id,
                                                    backup_path,
                                                    disk_format,
                                                    restore_size,
                                                    actual_size),
                                                    retry = True,
                                                    retry_policy={
                                                        'max_retries': 3,
                                                        'interval_start': 3,
                                                        'interval_step': 0.5,
                                                        'interval_max': 0.5,
                                                    })
                return {"message": "restore job submitted","task_id":task.id}

        except Exception as error:
            raise exc.HTTPServerError(explanation=str(error))

    def check_celery_status(self):
        celery_service_status = os.system("service ovirt_celery status")
        if celery_service_status != 0:
            return False
        return True

    def get(self, task_id):
        if not task_id:
            raise exc.HTTPBadRequest("Task id is required")
        print("Retrieving task %s", task_id)

        # self.log.info("Retrieving task %s", task_id)
        # celery_status = self.check_celery_status()
        # if not celery_status:
        #     err_msg = "Celery workers seems to be down at the moment. Unable to retrieve task " \
        #               " Kindly contact your administrator."
        #     self.log.info(err_msg)
        #     raise exc.HTTPInternalServerError(err_msg)
        result = {}
        try:
            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)
            if ctasks.result:
                result = ctasks.result
            if isinstance(result, Exception):
                result = {'Exception': str(result)}
            result['status'] = ctasks.status
            print(result)
        except KeyError:
            raise exc.HTTPBadRequest("No such task %r" % task_id)
        except Exception as e:
            raise Exception(str(e))
        return (result)

tvm_blueprint.add_url_rule("snapshot_download/<ticket_id>", view_func=Tvm().snapshot_download, methods=["POST"])
tvm_blueprint.add_url_rule("tasks/<task_id>", view_func=Tvm().get, methods=["GET"])
