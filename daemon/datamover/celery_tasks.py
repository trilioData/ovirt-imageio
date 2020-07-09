import io
import os
import uuid
import json
import time
import shutil
import subprocess
import configparser
from queue import Queue, Empty
from threading import Thread
from celery import Celery



app = Celery('celery_tasks', backend='redis', broker='redis://localhost:6379/0')

CONF_DIR = '/etc/ovirt-datamover/'
def enqueue_output(out, queue):
    line = out.read(17)
    while line:
        line = out.read(17)
        queue.put(line)
    out.close()

def generate_random_string(string_length=5):
    """Returns a random string of length string_length."""

    random = str(uuid.uuid4())
    random = random.upper()
    random = random.replace("-","")
    return random[0:string_length]

def get_source_path_from_ticket(dest_path, ticket_id):
    cmd = 'curl --unix-socket /run/ovirt-imageio/sock -X GET  http://localhost/tickets/' + ticket_id
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    # if stderr:
    #     print(('Result was %s' % stderr))
    #     raise Exception("Execution error %(exit_code)d (%(stderr)s). "
    #                     "cmd %(cmd)s" %
    #                     {'exit_code': 1,
    #                      'stderr': stderr,
    #                      'cmd': cmd})
    print(stderr)
    print(f"sfasf {stdout}")
    result = json.loads(stdout)
    src_path = result['url'].split('file://')[1]
    print(f"src_path found {src_path}")

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

    return src_path


def get_recent_snapshot(recent_snap_id, src_path):
    process = subprocess.Popen('qemu-img info --backing-chain --output json ' + src_path, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stderr:
        print(('Result was %s' % stderr))
        raise Exception("Execution error %(exit_code)d (%(stderr)s). "
                        "cmd %(cmd)s" %
                        {'exit_code': 1,
                         'stderr': stderr,
                         'cmd': 'qemu-img info --backing-chain --output json ' + src_path})

    result = json.loads(stdout)

    first_record = result[0]
    first_record_backing_file = first_record.get('backing-filename', None)
    recent_snap_path = recent_snap_id.get(str(first_record_backing_file), None)
    return result, first_record, first_record_backing_file, recent_snap_path



def download_incremental_snapshot(self, src_path, dest_path, ticket_id):
    basepath = os.path.basename(src_path)

    cmdspec = [
        'cp',
        src_path,
        dest_path,
    ]
    # cmdspec += ['-O', 'qcow2', src_path, dest_path]
    cmd = " ".join(cmdspec)
    print(('Take a Incremental snapshot with cp cmd: %s ' % cmd))
    self.update_state(state='PENDING',
                      meta={'Task': 'Starting Backup',
                            'disk_id': basepath,
                            'ticket_id': ticket_id})
    process = subprocess.Popen(cmdspec,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               bufsize=-1,
                               close_fds=True,
                               shell=False)

    queue = Queue()
    read_thread = Thread(target=enqueue_output,
                         args=(process.stdout, queue))

    read_thread.daemon = True  # thread dies with the program
    read_thread.start()

    percentage = 0.0
    while process.poll() is None:
        time.sleep(5)
        print("sleeping 5 sec")
        continue
        # try:
        #     try:
        #         output = queue.get(timeout=300)
        #     except Empty:
        #         print("Error in queue.get()")
        #         continue
        #     except Exception as ex:
        #         print("Error in queue.get()")
        #         print(ex)
        #
        #     print(f" op {output.decode('utf-8')}")
        #     # percentage = re.search(r'\d+\.\d+', output.decode('utf-8')).group(0)
        #     percentage = output.decode('utf-8')
        #     message = (("copying from %(path)s to "
        #             "%(dest)s %(percentage)s %% completed\n") %
        #            {'path': src_path,
        #             'dest': dest_path,
        #             'percentage': str(percentage)})
        #     print(message)
        #
        #     # percentage = float(percentage)
        #     self.update_state(state='PENDING',
        #                       meta={'percentage': percentage,
        #                             'disk_id': basepath,
        #                             'ticket_id': ticket_id})
        #
        # except Exception as ex:
        #     print(ex)
        #     pass

    _returncode = process.returncode  # pylint: disable=E1101
    if _returncode:
        print(('Result was %s' % _returncode))
        raise Exception("Execution error %(exit_code)d (%(stderr)s). "
                        "cmd %(cmd)s" %
                        {'exit_code': _returncode,
                         'stderr': process.stderr.read(),
                         'cmd': cmd})

def download_full_snapshot(self, src_path, dest_path, ticket_id):
    basepath = os.path.basename(src_path)

    cmdspec = [
        'qemu-img',
        'convert',
        '-p',
    ]
    cmdspec += ['-O', 'qcow2', src_path, dest_path]
    cmd = " ".join(cmdspec)
    print(('Take a full snapshot with qemu-img convert cmd: %s ' % cmd))
    self.update_state(state='PENDING',
                      meta={'Task': 'Starting Backup',
                            'disk_id': basepath,
                            'ticket_id': ticket_id})
    process = subprocess.Popen(cmdspec,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               bufsize=-1,
                               close_fds=True,
                               shell=False)

    queue = Queue()
    read_thread = Thread(target=enqueue_output,
                         args=(process.stdout, queue))

    read_thread.daemon = True  # thread dies with the program
    read_thread.start()

    percentage = 0.0
    while process.poll() is None:
        try:
            try:
                output = queue.get(timeout=300)
            except Empty:
                print("Error in queue.get()")
                continue
            except Exception as ex:
                print("Error in queue.get()")
                print(ex)

            print(f" op {output.decode('utf-8')}")
            # percentage = re.search(r'\d+\.\d+', output.decode('utf-8')).group(0)
            percentage = output.decode('utf-8')
            message = (("copying from %(path)s to "
                    "%(dest)s %(percentage)s %% completed\n") %
                   {'path': src_path,
                    'dest': dest_path,
                    'percentage': str(percentage)})
            print(message)

            # percentage = float(percentage)
            self.update_state(state='PENDING',
                              meta={'percentage': percentage,
                                    'disk_id': basepath,
                                    'ticket_id': ticket_id})

        except Exception as ex:
            print(ex)
            pass

    _returncode = process.returncode  # pylint: disable=E1101
    if _returncode:
        print(('Result was %s' % _returncode))
        raise Exception("Execution error %(exit_code)d (%(stderr)s). "
                        "cmd %(cmd)s" %
                        {'exit_code': _returncode,
                         'stderr': process.stderr.read(),
                         'cmd': cmd})

def perform_staging_operation(self, result,src_path,dest_path, first_record,recent_snap_path, recent_snap_ids, ticket_id ):
    tempdir = None
    path = src_path
    dest = dest_path
    basepath = os.path.basename(src_path)

    try:
        self.update_state(state='PENDING',
                          meta={'Task': 'Copying manual snapshots to staging area',
                                'disk_id': basepath,
                                'ticket_id': ticket_id})
        temp_random_id = generate_random_string(5)
        with open(os.path.join(CONF_DIR, "datamover.conf")) as f:
            sample_config = f.read()
        config = configparser.RawConfigParser(allow_no_value=True)
        config.readfp(io.BytesIO(sample_config))
        mountpath = config.get('nfs_config', 'mount_path')
        # mountpath = "/var/triliovault-mounts/NjYuNzAuMTc4LjIyNTovZGF0YS9uZXdfMjAwZw=="
        tempdir = mountpath + '/staging/' + temp_random_id
        os.makedirs(tempdir)
        commands = []
        for index, record in enumerate(result):
            filename = os.path.basename(str(record.get('filename', None)))
            recent_snap_path = recent_snap_ids.get(str(record.get('backing-filename')), None)
            dest_file_format = record.get("format", "qcow2")
            if record.get('backing-filename', None) and str(
                    record.get('backing-filename', None)) and not recent_snap_path:
                try:
                    # self.update_state(state='PENDING',
                    #                   meta={'Task': 'Copying manual snapshots to staging area',
                    #                         'disk_id': os.path.basename(path),
                    #                         'ticket_id': ticket_id})
                    print(("Coping file: {} to staging area.".format(path)))
                    shutil.copy(path, tempdir)
                    backing_file = os.path.basename(str(record.get('backing-filename', None)))
                    backing_file_format = result[index + 1].get("format", "qcow2")
                    print((
                              "Copy to staging area completed. Rebasing file(format): {}({}) with backing file(format):{}({})".format(
                                  filename, dest_file_format, backing_file, backing_file_format)))
                    command = "qemu-img rebase -u -f {} -F {} -b {} {}".format(dest_file_format, backing_file_format,
                                                                               backing_file, filename)
                    commands.append(command)
                    # self.update_state(state='PENDING',
                    #                   meta={'Task': 'Disk copy to staging area Completed',
                    #                         'disk_id': os.path.basename(path),
                    #                         'ticket_id': ticket_id})
                except IOError as e:
                    err = "Unable to copy file. Error: [{0}]".format(e)
                    print(err)
                    raise Exception(err)
                except Exception as ex:
                    error = "Unexpected error : [{0}]".format(ex)
                    print(error)
                    raise Exception(error)
            else:
                try:
                    # self.update_state(state='PENDING',
                    #                   meta={'Task': 'Copying manual snapshots to staging area',
                    #                         'disk_id': os.path.basename(path),
                    #                         'ticket_id': ticket_id})
                    print(("Coping file(format): {}({}) to staging area.".format(path, dest_file_format)))
                    shutil.copy(path, tempdir)
                    print("Copy to staging area completed. Its the Final disk.")
                    if dest_file_format != "raw":
                        command = "qemu-img rebase -u -f {} {}".format(dest_file_format, filename)
                        commands.append(command)
                    else:
                        print("Its a RAW format disk cannot be rebased")
                    self.update_state(state='PENDING',
                                      meta={'Task': 'Disk copy to staging area Completed',
                                            'disk_id': os.path.basename(path),
                                            'ticket_id': ticket_id})
                except IOError as e:
                    err = "Unable to copy file. Error: [{0}]".format(e)
                    print(err)
                    raise Exception(err)
                except Exception as ex:
                    error = "Unexpected error: [{0}]".format(str(ex))
                    print(error)
                    raise Exception(error)
                break
            path = str(record.get('full-backing-filename'))
        string_commands = ";".join(str(x) for x in commands)
        process = subprocess.Popen(string_commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=tempdir, shell=True)
        stdout, stderr = process.communicate()
        if stderr:
            print(("Unable to perform operation. Error: {}".format(stderr)))
            shutil.rmtree(tempdir)
            raise Exception(stderr)
        self.update_state(state='PENDING',
                          meta={'Task': 'Starting backup process...',
                                'disk_id': basepath,
                                'ticket_id': ticket_id})
        cmdspec = [
            'qemu-img',
            'convert',
            '-p',
        ]
        filename = os.path.basename(str(first_record.get('filename', None)))
        path = os.path.join(tempdir, filename)
        cmdspec += ['-O', 'qcow2', path, dest]
        print("Executing cmd: {}", format(cmdspec))
        process = subprocess.Popen(cmdspec,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   bufsize=-1,
                                   close_fds=True,
                                   shell=False)

        queue = Queue()
        read_thread = Thread(target=enqueue_output,
                             args=(process.stdout, queue))

        read_thread.daemon = True  # thread dies with the program
        read_thread.start()

        percentage = 0.0
        while process.poll() is None:
            try:
                try:
                    output = queue.get(timeout=300)
                except Empty:
                    continue
                except Exception as ex:
                    print(ex)

                # percentage = re.search(r'\d+\.\d+', output).group(0)
                percentage = output.decode('utf-8')

                print((("copying from %(path)s to "
                        "%(dest)s %(percentage)s %% completed\n") %
                       {'path': path,
                        'dest': dest,
                        'percentage': str(percentage)}))

                print(f"staging .....{percentage}")
                self.update_state(state='PENDING',
                                  meta={'percentage': percentage,
                                        'disk_id': basepath,
                                        'ticket_id': ticket_id})

            except Exception as ex:
                pass
        if recent_snap_path:
            print(("Performing qemu rebase on disk {}, Setting backing file as: {}".format(dest, recent_snap_path)))
            process = subprocess.Popen('qemu-img rebase -u -f qcow2 -F qcow2 -b ' + recent_snap_path + ' ' + dest,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            if stderr:
                err = "Unable to change the backing file:{} error: {}".format(dest, stderr)
                print(err)
                raise Exception(err)
    finally:
        if tempdir:
            if os.path.exists(tempdir):
                shutil.rmtree(tempdir)

@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.backup")
def backup(self, download_url, snapshot_type, backup_dest_path, recent_snap_ids, ticket_id):
    src_path = get_source_path_from_ticket(backup_dest_path, ticket_id)
    print('Backup type: [{0}]'.format(snapshot_type))
    if snapshot_type == "full":
        print("performing full")
        download_full_snapshot(self, src_path, backup_dest_path, ticket_id)
        print("performed full")
    else:
        src_qemu_info, first_record, first_record_backing_file, recent_snap_path = get_recent_snapshot(recent_snap_ids, src_path)
        if first_record_backing_file and recent_snap_path:

            # if first_record_backing_file and recent_snap_path:
            try:
                # We must use the daemon for downloading a backup disk.
                print(f"transfer url {download_url}... starting download cp")
                download_incremental_snapshot(self, src_path, backup_dest_path, ticket_id)
                # with ui.ProgressBar() as pb:
                #     client.download(
                #         download_url,
                #         backup_dest_path,
                #         "ca.pem",
                #         # incremental=incremental,
                #         secure=False,
                #         progress=pb)
            finally:
                print(f"Done downloading snapshot to path through client {backup_dest_path}")
            dest_file_format = first_record.get('format', 'qcow2')
            print(("Performing qemu rebase on disk(format) {}({}), Setting backing file(format) as: {}(qcow2)".format(
                backup_dest_path,
                dest_file_format,
                recent_snap_path)))
            process = subprocess.Popen(
                "qemu-img rebase -u -f {} -F qcow2 -b {} {}".format(dest_file_format, recent_snap_path,
                                                                    backup_dest_path),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            if stderr:
                err = "Unable to change the backing file:{} ,recent snap path {}, error: {}".format(backup_dest_path, recent_snap_path, stderr)
                print(err)
                raise Exception(err)
        else:
            print("perform staging area tasks")
            perform_staging_operation(self, src_qemu_info, src_path, backup_dest_path, first_record, recent_snap_path, recent_snap_ids,  ticket_id)
