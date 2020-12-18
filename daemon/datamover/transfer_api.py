# Copyright (c) 2015 TrilioData, Inc.
# All Rights Reserved.
import os
import subprocess
import json
from flask import Blueprint, request
from flask.views import MethodView
from webob import exc
import celery
import celery_tasks
from celery.task.control import revoke

tvm_blueprint = Blueprint("tvm_blueprint", __name__, url_prefix="/v1/admin")

def get_ticket_info(ticket_id):
    cmd = 'curl --unix-socket /run/ovirt-imageio/sock -X GET  http://localhost/tickets/' + ticket_id
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    print(f"Ticket error {stderr}")
    print(f"Ticket information {stdout}")
    result = json.loads(stdout)
    src_path = result['url'].split('file://')[1]
    disk_size = result['size']
    return src_path, disk_size

def get_source_path_from_ticket(dest_path, ticket_id):
    src_path, disk_size = get_ticket_info(ticket_id)

    if not os.path.exists(str(src_path)):
        error = 'Source Path [{0}] does not exists.'.format(src_path)
        path_dir = os.path.dirname(src_path)
        snapshot_disk_name = os.path.basename(dest_path)
        new_path = str(os.path.join(path_dir, snapshot_disk_name))
        if not os.path.exists(new_path):
            print('Other path [{0}] too does not exists.'.format(new_path))
            raise Exception(error)
        else:
            src_path = new_path
            print('Original path does not exists. Backing up : [{0}]'.format(src_path))

    return src_path, disk_size

class Tvm(MethodView):
    """The file search API controller for the workload manager API."""
    def check_celery_status(self):
        celery_service_status = os.system("service ovirt_celery status")
        if celery_service_status != 0:
            return False
        return True

    def snapshot_download(self, ticket_id):
        celery_status = self.check_celery_status()
        if not celery_status:
            err_msg = "Celery workers seems to be down at the moment. Unable to retrieve task " \
                      " Kindly contact your administrator."
            print(err_msg)
            raise exc.HTTPInternalServerError(err_msg)
        try:
            body = request.get_json()
            if "backup" == body['method']:
                proxy_url = body['proxy_url']
                type = body['type']
                backup_path = body['backup_path']
                recent_snap_ids = body['recent_snap_id']
                src_path = body['src_path']
                disk_size = body['disk_size']
                print(f"Vm id received in api {proxy_url, type, backup_path, recent_snap_ids, ticket_id, src_path, disk_size}")
                task = celery_tasks.backup.apply_async((proxy_url, type, backup_path, recent_snap_ids, ticket_id,src_path,disk_size),
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
                try:
                    src_path, disk_size = get_ticket_info(ticket_id)
                except Exception as e:
                    err = f"Unable to fetch disk size and source path {str(e)}"
                    raise exc.HTTPInternalServerError(err)

                task = celery_tasks.restore.apply_async((ticket_id,
                                                    backup_path,
                                                    disk_format,
                                                    restore_size,
                                                    actual_size,
                                                    src_path),
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

    def get(self, task_id):
        if not task_id:
            raise exc.HTTPBadRequest("Task id is required")
        print("Retrieving task %s", task_id)

        celery_status = self.check_celery_status()
        if not celery_status:
            err_msg = "Celery workers seems to be down at the moment. Unable to retrieve task " \
                      " Kindly contact your administrator."
            print(err_msg)
            raise exc.HTTPInternalServerError(err_msg)
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

    def ping(self):
        result = {"status": "Configured"}
        return result

    def delete(self, task_id):
        if not task_id:
            raise exc.HTTPBadRequest("Task id is required")

        print(f"Stopping execution of task with id: {task_id}")
        result = {}
        try:
            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)

            ctasks.revoke(terminate=True, signal='SIGKILL')

            ctasks = celery.result.AsyncResult(task_id, app=celery_tasks.app)
            if isinstance(result, Exception):
                result = {'Exception': str(result)}
            result['status'] = ctasks.status
        except KeyError:
            raise exc.HTTPBadRequest("Task id is not found")
        except Exception as e:
            raise Exception(str(e))
        return (result)

    def get_ticket_path(self, ticket_id):
        cmd = 'curl --unix-socket /run/ovirt-imageio/sock -X GET  http://localhost/tickets/' + ticket_id
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        print(f"Ticket error {stderr}")
        print(f"Ticket information {stdout}")
        result = json.loads(stdout)
        src_path = result['url'].split('file://')[1]
        disk_size = result['size']
        result = {
            "src_path": src_path,
            "disk_size": disk_size
        }
        return result

tvm_blueprint.add_url_rule("snapshot_download/<ticket_id>", view_func=Tvm().snapshot_download, methods=["POST"])
tvm_blueprint.add_url_rule("tasks/<task_id>", view_func=Tvm().get, methods=["GET"])
tvm_blueprint.add_url_rule("ticket/<ticket_id>", view_func=Tvm().get_ticket_path, methods=["GET"])
tvm_blueprint.add_url_rule("tasks/<task_id>", view_func=Tvm().delete, methods=["DELETE"])
tvm_blueprint.add_url_rule("ping", view_func=Tvm().ping, methods=["GET"])
