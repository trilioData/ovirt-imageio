import io
import stat
import os
import re
import math
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

CONF_DIR = '/etc/trilio-datamover/'
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

def get_size_from_ticket(ticket_id):
    counter = 2
    result = {}
    while counter > 0:
        try:
            cmd = 'curl --unix-socket /run/ovirt-imageio/sock -X GET  http://localhost/tickets/' + ticket_id
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            result = json.loads(stdout)
            break
        except Exception as e:
            print(f"Unable to load stdout while getting ticket size {stdout, stderr}")
        counter = counter-1
    return int(result['size'])

def get_source_path_from_ticket(dest_path, ticket_id):
    cmd = 'curl --unix-socket /run/ovirt-imageio/sock -X GET  http://localhost/tickets/' + ticket_id
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    print(f"Ticket error {stderr}")
    print(f"Ticket information {stdout}")
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



def download_incremental_snapshot(self, src_path, dest_path, ticket_id, size):
    basepath = os.path.basename(src_path)
    with open(src_path, "rb") as src:
        with open(dest_path, "wb") as f:
            copied = 0
            while True:
                buf = src.read(8388608)
                if not buf:
                    break
                f.write(buf)
                copied += len(buf)
                percentage = float(copied) / size * 100
                percentage = "%.2f" % percentage
                self.update_state(state='PENDING',
                                  meta={'percentage': percentage,
                                        'disk_id': basepath,
                                        'ticket_id': ticket_id})

def download_full_snapshot(self, src_path, dest_path, ticket_id):
    basepath = os.path.basename(src_path)

    cmdspec = [
        'qemu-img',
        'convert',
        '-p',
        '-t',
        'none',
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

    q = Queue()
    read_thread = Thread(target=enqueue_output,
                         args=(process.stdout, q))

    read_thread.daemon = True  # thread dies with the program
    read_thread.start()

    percentage = 0.0
    while process.poll() is None:
        try:
            extend_ticket(ticket_id)
        except Exception as e:
            print(f"Exception in ticket extend {str(e)}")

        time.sleep(5)
        try:
            try:
                qsize = q.qsize()
                if qsize:
                    output = q.queue[qsize-1]
                    q.queue.clear()
                else:
                    continue
                #output = q.get(timeout=300)
            except Empty:
                print("Error in queue.get()")
                continue
            except Exception as ex:
                print("Error in queue.get()")
                print(ex)

            percentage = output.decode('utf-8').replace('%','').split()
            if len(percentage) > 1 and '-' not in percentage:
                percentage = f"{percentage[1]}{percentage[0]}"


            # percentage = re.search(r'\d+\.\d+', output.decode('utf-8')).group(0)

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

        Config = configparser.RawConfigParser(allow_no_value=True)
        Config.read(os.path.join(CONF_DIR, "datamover.conf"))
        try:
            mountpath = Config.get('nfs_config', 'mount_path')
        except Exception as e:
            print(f"Unable to get nfs config {e}")
            mountpath = Config.get('s3_config', 'mount_path')

        print(f"mount path {mountpath}")
        if not mountpath:
            raise Exception("Unable to read nfs mount path from daemon.conf")
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
            '-t',
            'none',
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
                extend_ticket(ticket_id)
            except Exception as e:
                print(f"Exception in ticket extend {str(e)}")

            time.sleep(5)
            try:
                try:
                    qsize = queue.qsize()
                    if qsize:
                        output = queue.queue[qsize - 1]
                        queue.queue.clear()
                    else:
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

def is_blk_device(dev):
    try:
        if stat.S_ISBLK(os.stat(dev).st_mode):
            return True
        return False
    except Exception:
        print ('Path %s not found in is_blk_device check', dev)
        return False


def extend_ticket(ticket_id):
    pass
    # try:
    #     cmd = "curl --unix-socket /run/ovirt-imageio/sock -X PATCH  --data '{\"timeout\":7}' http://localhost/tickets/" + ticket_id
    #     process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    #     stdout, stderr = process.communicate()
    #     if stdout:
    #         print(f"Ticket extend result {stdout}")
    #         pass
    # except Exception as e:
    #     print(f"Exception in extending ticket {ticket_id}")


def check_for_odirect_support(src, dest, flag='oflag=direct'):

    # Check whether O_DIRECT is supported
    try:
        nova_utils.execute('dd', 'count=0', 'if=%s' % src, 'of=%s' % dest,
                           flag, run_as_root=True)
        return True
    except Exception:
        return False

@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.backup")
def backup(self, download_url, snapshot_type, backup_dest_path, recent_snap_ids, ticket_id, src_path, size):
    print('Backup type: [{0}]'.format(snapshot_type))
    print(f"src_path found {src_path}, disk_size {size}")
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
                download_incremental_snapshot(self, src_path, backup_dest_path, ticket_id, size)
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


@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.restore")
def restore(self, ticket_id, backup_image_file_path, disk_format, restore_size,
            actual_size, volume_path):
    def transfer_qemu_image_to_volume(
            volume_path,
            backup_image_file_path,
            disk_format):

        # Get Backing file if present for current disk.
        qemu_cmd = "qemu-img info --output json {}".format(volume_path)
        print(f"Executing volume path info cmd: {qemu_cmd}")
        qemu_process = subprocess.Popen(qemu_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = qemu_process.communicate()
        if stderr:
            print("Unable to get backing file info. Error: {}".format(stderr))
            raise Exception(stderr)

        data = json.loads(stdout)
        backing_file = data.get("backing-filename", None)
        backing_file_format = data.get("backing-filename-format", "qcow2")

        log_msg = 'Qemu info for [{0}] : [{1}]. Backing Path: [{2}]'.format(volume_path, data, backing_file)
        print(log_msg)

        target = volume_path

        basepath = os.path.basename(volume_path)

        # Convert and concise disk to a tmp location
        cmdspec = [
            'qemu-img',
            'convert',
            '-p',
            '-t',
            'none',
        ]

        # if is_blk_device(volume_path) and \
        #         check_for_odirect_support(backup_image_file_path,
        #                                   volume_path, flag='oflag=direct'):
        #         pass

        cmdspec += ['-O', disk_format, backup_image_file_path, target]

        # default_cache = True
        # if default_cache is True:
        #     if '-t' in cmdspec:
        #         cmdspec.remove('-t')
        #     if 'none' in cmdspec:
        #         cmdspec.remove('none')
        cmd = " ".join(cmdspec)

        self.update_state(state='PENDING',
                          meta={'Task': 'Starting restore process',
                                'disk_id': basepath,
                                'ticket_id': ticket_id})

        print('transfer_qemu_image_to_volume cmd %s ' % cmd)
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

        while process.poll() is None:
            while not queue.empty():
                try:
                    output = queue.get(timeout=300)
                    # percentage_found = re.search(r'(\d+\.\d+)', output)
                    # percentage = percentage_found.group() if percentage_found else None
                    percentage = output.decode('utf-8').replace('%', '').split()
                    if len(percentage) > 1 and '-' not in percentage:
                        percentage = f"{percentage[1]}{percentage[0]}"

                    self.update_state(state='PENDING',
                                      meta={'percentage': percentage,
                                            'disk_id': basepath,
                                            'ticket_id': ticket_id})
                except Exception as e:
                    print(f"Exception in updating restore status {str(e)}")

        _returncode = process.returncode  # pylint: disable=E1101
        if _returncode:
            print(('Result was %s' % _returncode))
            stderr = process.stderr.read()
            error = "Execution error %(exit_code)d (%(stderr)s). cmd %(cmd)s" % {'exit_code': _returncode,
                                                                                 'stderr': stderr.decode('utf-8'),
                                                                                 'cmd': cmd}
            self.update_state(state='EXCEPTION',
                              meta={'exception': error,
                                    'disk_id': basepath})
            raise Exception(stderr.decode('utf-8'))
        else:
            percentage = 100
            self.update_state(state='PENDING',
                              meta={'percentage': percentage,
                                    'disk_id': basepath,
                                    'ticket_id': ticket_id})

        process.stdin.close()

        if backing_file and disk_format != "raw":
            try:
                self.update_state(state='PENDING',
                                  meta={'status': 'Performing Rebase operation to point disk to its backing file',
                                        'ticket_id': ticket_id})

                basedir = os.path.dirname(volume_path)

                # backing_file = "{}/{}".format(basedir, backing_file) # In Case if required

                print('Rebasing volume: [{0}] to backing file: [{1}]. ' \
                         'Volume format: [{2}]'.format(volume_path, backing_file, disk_format))

                # process = subprocess.Popen('qemu-img info --output json {}'.format(backing_file),
                #                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                # stdout, stderr = process.communicate()
                # backing_file_format = None
                # if stderr:
                #     print("Error in qemu-img info operation: {}".format(stderr))
                # else:
                #     result = json.loads(stdout)
                #     backing_file_format = result.get("format")

                print("Setting backing file format to {}", backing_file_format)
                process = subprocess.Popen(
                    'qemu-img rebase -u -f qcow2 -F {} -b {} {}'.format(backing_file_format, backing_file, target),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=basedir, shell=True)

                stdout, stderr = process.communicate()
                if stderr:
                    error = "Unable to change the backing file " + volume_path + " " + stderr.decode('utf-8')
                    print(error)
                    raise Exception(error)

            except IOError as ex:
                print(ex)
                err = "Unable to move temp file as temp file was never created. Exception: [{0}]".format(ex)
                print(err)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': err})
                raise Exception(err)

            except Exception as ex:
                print(ex)
                error = 'Error occurred: [{0}]'.format(ex)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': error})
                raise Exception(error)

    print(f"volume_path found {volume_path}")

    # Determine right format and send it accordingly
    if disk_format == "cow":
        disk_format = "qcow2"
    else:
        disk_format = "raw"

    def __get_lvm_size_in_gb(stdout):
        try:
            block_size = stdout.decode('utf-8').split('SIZE="')[1].split("\"")[0]
        except Exception as ex:
            match_found = re.search("SIZE=\"([A-Z, 0-9]+)\"", stdout)
            if match_found:
                block_size = match_found.group(1)
        print(f"in get lvm size {block_size}")
        if "K" in block_size:
            lvm_size = math.ceil(float(block_size.split('K')[0]))
            lvm_size = math.ceil(lvm_size / (1024 * 1024))
        elif "M" in block_size:
            lvm_size = math.ceil(float(block_size.split('M')[0]))
            lvm_size = math.ceil(lvm_size / 1024)
        elif "G" in block_size:
            lvm_size = math.ceil(float(block_size.split('G')[0]))
        elif "T" in block_size:
            lvm_size = math.ceil(float(block_size.split('T')[0]) * 1024)

        return lvm_size

    print(f" Restore size {restore_size}, actual size {actual_size}")

    if is_blk_device(volume_path):
        lvm_info = subprocess.Popen('lsblk -P ' + volume_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    shell=True)
        stdout, stderr = lvm_info.communicate()
        print(f"blk device cmd lsblk -P : {stdout.decode('utf-8'), stderr.decode('utf-8')}")

        if not stderr:
            print("STDOUT: {}".format(stdout))
            lvm_size_in_gb = __get_lvm_size_in_gb(stdout)
            print(f"lvm size in gb {lvm_size_in_gb}")
            if restore_size and actual_size:
                if lvm_size_in_gb < restore_size:
                    print("LVM size before extend: {}".format(lvm_size_in_gb))
                    lvm_path = os.readlink(volume_path)
                    extend_by = restore_size - lvm_size_in_gb
                    if extend_by + lvm_size_in_gb > actual_size:
                        print(
                            "No more space remained for doing the disk restore. Disk is already being extended to actual size")
                    self.update_state(state='PENDING',
                                      meta={'status': 'Extending Block by {} GB'.format(extend_by),
                                            'ticket_id': ticket_id})
                    lvm_extend_cmd = "sudo -u root lvextend -L +{}G {}".format(extend_by, lvm_path)
                    self.update_state(state='PENDING',
                                      meta={'status': 'Successfully Extended Block',
                                            'ticket_id': ticket_id})
                    lvm_extend = subprocess.Popen(lvm_extend_cmd, stdout=subprocess.PIPE, shell=True)
                    stdout, stderr = lvm_extend.communicate()
                    if stderr:
                        print("Failed to extend LVM disk Exception:" + stderr.decode('utf-8'))
                    else:
                        lvm_info = subprocess.Popen('lsblk -P ' + volume_path, stdout=subprocess.PIPE, shell=True)
                        stdout, stderr = lvm_info.communicate()
                        if not stderr:
                            lvm_size = __get_lvm_size_in_gb(stdout)
                            print("LVM size after extend: {}".format(lvm_size))
                else:
                    self.update_state(state='PENDING',
                                      meta={'status': 'Error Extending Block',
                                            'ticket_id': ticket_id})
                    print("LVM size is already larger than restore size. No need to extend the disk")
            else:
                self.update_state(state='PENDING',
                                  meta={'status': 'Error Extending Block',
                                        'ticket_id': ticket_id})
                print("Snapshot restore size or actual size of VM is None. Skipping LVM block extend..")
        else:
            self.update_state(state='PENDING',
                              meta={'status': 'Error Extending Block',
                                    'ticket_id': ticket_id})
            print("error getting actual size of the lvm block")

    transfer_qemu_image_to_volume(volume_path, backup_image_file_path, disk_format)
