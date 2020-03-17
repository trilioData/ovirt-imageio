import os
import re
import stat
import subprocess
from Queue import Queue, Empty
from threading import Thread
import uuid
import pwd
import grp
import os
import uuid
import shutil
import sys
import ConfigParser
import io

import math
from celery import Celery
from custom_exceptions import QemuImageConvertError
from celery.contrib import rdb

from ovirt_imageio_common import directio
from kombu import Queue as kqueue
import json
import random
import logging
import logging.config

log = logging.getLogger("server")

CONF_DIR = "/etc/ovirt-imageio-daemon"


app = Celery('celery_tasks', backend='redis', broker='redis://localhost:6379/0')
'''app.conf.task_queues = (
    kqueue('backup_tasks'),
    kqueue('restore_tasks'),
)'''
#rdb.set_trace()

def is_blk_device(dev):
    try:
        if stat.S_ISBLK(os.stat(dev).st_mode):
            return True
        return False
    except Exception:
        print ('Path %s not found in is_blk_device check', dev)
        return False


def check_for_odirect_support(src, dest, flag='oflag=direct'):

    # Check whether O_DIRECT is supported
    try:
        nova_utils.execute('dd', 'count=0', 'if=%s' % src, 'of=%s' % dest,
                           flag, run_as_root=True)
        return True
    except Exception:
        return False


def enqueue_output(out, queue):
    line = out.read(17)
    while line:
        line = out.read(17)
        queue.put(line)
    out.close()

@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.backup")
def backup(self, ticket_id, path, dest, size, type, buffer_size, recent_snap_id):

    print 'Backing source disk : [{0}] to target location: [{1}]'.format(path, dest)

    if not os.path.exists(str(path)):
        error = 'Source Path [{0}] does not exists.'.format(path)
        path_dir = os.path.dirname(path)
        snapshot_disk_name = os.path.basename(dest)
        new_path = str(os.path.join(path_dir, snapshot_disk_name))
        if not os.path.exists(new_path):
            print 'Other path [{0}] too does not exists.'.format(new_path)
            raise Exception(error)
        else:
            path = new_path
            print 'Original path does not exists. Backing up : [{0}]'.format(path)

    basepath = os.path.basename(path)
    print 'Backup type: [{0}]'.format(type)
    if type == "full":
        cmdspec = [
            'qemu-img',
            'convert',
            '-p',
        ]
        cmdspec += ['-O', 'qcow2', path, dest]
        cmd = " ".join(cmdspec)
        print('Take a full snapshot with qemu-img convert cmd: %s ' % cmd)
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
                    continue
                except Exception as ex:
                    print(ex)

                percentage = re.search(r'\d+\.\d+', output).group(0)

                print(("copying from %(path)s to "
                           "%(dest)s %(percentage)s %% completed\n") %
                          {'path': path,
                           'dest': dest,
                           'percentage': str(percentage)})

                percentage = float(percentage)

                self.update_state(state='PENDING',
                                  meta={'percentage': percentage,
                                        'disk_id': basepath,
                                        'ticket_id': ticket_id})

            except Exception as ex:
                print ex
                pass

        _returncode = process.returncode  # pylint: disable=E1101
        if _returncode:
            print(('Result was %s' % _returncode))
            raise Exception("Execution error %(exit_code)d (%(stderr)s). "
                            "cmd %(cmd)s" %
                            {'exit_code': _returncode,
                             'stderr': process.stderr.read(),
                             'cmd': cmd})
    else:
        process = subprocess.Popen('qemu-img info --backing-chain --output json ' + path, stdout=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        if stderr:
            print(('Result was %s' % stderr))
            raise Exception("Execution error %(exit_code)d (%(stderr)s). "
                            "cmd %(cmd)s" %
                            {'exit_code': 1,
                             'stderr': stderr,
                             'cmd': 'qemu-img info --backing-chain --output json ' + path})

        result = json.loads(stdout)

        first_record = result[0]
        first_record_backing_file = first_record.get('backing-filename', None)
        recent_snap_path = recent_snap_id.get(str(first_record_backing_file), None)
        if first_record_backing_file and recent_snap_path:
            print('Executing task id {0.id}, args: {0.args!r} kwargs: {0.kwargs!r}'.format(
                self.request))
            try:
                with open(path, "r+") as src:
                    with open(dest, "w+") as f:
                        copied = 0
                        while True:
                            buf = src.read(buffer_size)
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
            except Exception as exc:
                log.error("Error in writing data to dest:{}".format(path))
                raise Exception(exc.message)
            process = subprocess.Popen('qemu-img rebase -u -b ' + recent_snap_path + ' ' + dest, stdout=subprocess.PIPE, shell=True)
            if stderr:
                log.error("Unable to change the backing file", dest, stderr)
        else:
            tempdir = None
            try:
                self.update_state(state='PENDING',
                                  meta={'Task': 'Copying manual snapshots to staging area',
                                        'disk_id': basepath,
                                        'ticket_id': ticket_id})
                temp_random_id = generate_random_string(5)
                with open(os.path.join(CONF_DIR, "daemon.conf")) as f:
                    sample_config = f.read()
                config = ConfigParser.RawConfigParser(allow_no_value=True)
                config.readfp(io.BytesIO(sample_config))
                mountpath = config.get('nfs_config', 'mount_path')
                tempdir = mountpath + '/staging/' + temp_random_id
                os.makedirs(tempdir)
                commands = []
                for record in result:
                    filename = os.path.basename(str(record.get('filename', None)))
                    recent_snap_path = recent_snap_id.get(str(record.get('backing-filename')), None)
                    if record.get('backing-filename', None) and str(record.get('backing-filename', None)) and not recent_snap_path:
                        try:
                            self.update_state(state='PENDING',
                                              meta={'Task': 'Copying manual snapshots to staging area',
                                                    'disk_id': os.path.basename(path),
                                                    'ticket_id': ticket_id})
                            shutil.copy(path, tempdir)
                            backing_file = os.path.basename(str(record.get('backing-filename', None)))

                            command = 'qemu-img rebase -u -b ' + backing_file + ' ' + filename
                            commands.append(command)
                            self.update_state(state='PENDING',
                                              meta={'Task': 'Disk copy to staging area Completed',
                                                    'disk_id': os.path.basename(path),
                                                    'ticket_id': ticket_id})
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
                            self.update_state(state='PENDING',
                                              meta={'Task': 'Copying manual snapshots to staging area',
                                                    'disk_id': os.path.basename(path),
                                                    'ticket_id': ticket_id})
                            shutil.copy(path, tempdir)
                            command = 'qemu-img rebase -u ' + filename
                            commands.append(command)
                            self.update_state(state='PENDING',
                                              meta={'Task': 'Disk copy to staging area Completed',
                                                    'disk_id': os.path.basename(path),
                                                    'ticket_id': ticket_id})
                        except IOError as e:
                            err = "Unable to copy file. Error: [{0}]".format(e)
                            print(err)
                            raise Exception(err)
                        except Exception as ex:
                            error = "Unexpected error: [{0}]".format(ex.message)
                            print(error)
                            raise Exception(error)
                        break
                    path = str(record.get('full-backing-filename'))
                string_commands = ";".join(str(x) for x in commands)
                process = subprocess.Popen(string_commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE
                                           , cwd=tempdir, shell=True)
                stdout, stderr = process.communicate()
                if stderr:
                    shutil.rmtree(tempdir)
                    raise Exception(stdout)
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

                        percentage = re.search(r'\d+\.\d+', output).group(0)

                        print(("copying from %(path)s to "
                               "%(dest)s %(percentage)s %% completed\n") %
                              {'path': path,
                               'dest': dest,
                               'percentage': str(percentage)})

                        percentage = float(percentage)

                        self.update_state(state='PENDING',
                                          meta={'percentage': percentage,
                                                'disk_id': basepath,
                                                'ticket_id': ticket_id})

                    except Exception as ex:
                        pass
                if recent_snap_path:
                    process = subprocess.Popen('qemu-img rebase -u -b ' + recent_snap_path + ' ' + dest, stdout=subprocess.PIPE, shell=True)
                    stdout, stderr = process.communicate()
                    if stderr:
                        log.error("Unable to change the backing file", dest, stderr)
            finally:
                if tempdir:
                    if os.path.exists(tempdir):
                        shutil.rmtree(tempdir)


@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.restore")
def restore(self, ticket_id, volume_path, backup_image_file_path, disk_format, size, buffer_size, restore_size, actual_size):

    def transfer_qemu_image_to_volume(
            volume_path,
            backup_image_file_path,
            disk_format):

        # Get Backing file if present for current disk.
        qemu_cmd = ["qemu-img", "info", "--output", "json", volume_path]
        qemu_process = subprocess.Popen(qemu_cmd, stdout=subprocess.PIPE)
        data, err = qemu_process.communicate()
        data = json.loads(data)
        backing_file = data.get("backing-filename", None)

        log_msg = 'Qemu info for [{0}] : [{1}]. Backing Path: [{2}]'.format(volume_path, data, backing_file)
        print log_msg

        target = volume_path

        basepath = os.path.basename(volume_path)

        # Convert and concise disk to a tmp location
        cmdspec = [
            'qemu-img',
            'convert',
            '-p',
        ]

        if is_blk_device(volume_path) and \
            check_for_odirect_support(backup_image_file_path,
                                      volume_path, flag='oflag=direct'):
            cmdspec += ['-t', 'none']

        cmdspec += ['-O', disk_format, backup_image_file_path, target]

        default_cache = True
        if default_cache is True:
            if '-t' in cmdspec:
                cmdspec.remove('-t')
            if 'none' in cmdspec:
                cmdspec.remove('none')
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
                output = queue.get(timeout=300)
                percentage_found = re.search(r'(\d+\.\d+)', output)
                percentage = percentage_found.group() if percentage_found else None
                if percentage:
                    percentage = float(percentage)
                    if percentage != 0.0:
                        print(("copying from %(backup_path)s to "
                               "%(volume_path)s %(percentage)s %% completed\n") %
                              {'backup_path': backup_image_file_path,
                               'volume_path': target,
                               'percentage': str(percentage)})

                        self.update_state(state='PENDING',
                                          meta={'percentage': percentage,
                                                'disk_id': basepath,
                                                'ticket_id': ticket_id})

        _returncode = process.returncode  # pylint: disable=E1101
        if _returncode:
            print(('Result was %s' % _returncode))
            stderr = process.stderr.read()
            error = "Execution error %(exit_code)d (%(stderr)s). cmd %(cmd)s" % {'exit_code': _returncode,
                                                                                 'stderr': stderr,
                                                                                 'cmd': cmd}
            self.update_state(state='EXCEPTION',
                              meta={'exception': error,
                                    'disk_id': basepath})
            raise QemuImageConvertError(stderr)
        else:
            percentage = 100
            self.update_state(state='PENDING',
                              meta={'percentage': percentage,
                                    'disk_id': basepath,
                                    'ticket_id': ticket_id})

        process.stdin.close()

        if backing_file:
            try:
                self.update_state(state='PENDING',
                                  meta={'status': 'Performing Rebase operation to point disk to its backing file',
                                        'ticket_id': ticket_id})
                print 'Rebasing volume: [{0}] to backing file: [{1}]. ' \
                      'Volume format: [{2}]'.format(volume_path, backing_file, disk_format)

                basedir = os.path.dirname(volume_path)
                process = subprocess.Popen('qemu-img rebase -u -b ' + backing_file + ' ' + target,
                                           stdout=subprocess.PIPE,
                                           cwd=basedir, shell=True)
                stdout, stderr = process.communicate()
                if stderr:
                    error = "Unable to change the backing file " + volume_path + " " + stderr
                    log.error(error)
                    raise Exception(error)

            except IOError as ex:
                print ex
                err = "Unable to move temp file as temp file was never created. Exception: [{0}]".format(ex)
                log.error(err)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': err})
                raise Exception(err)

            except Exception as ex:
                print ex
                error = 'Error occurred: [{0}]'.format(ex)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': error})
                raise Exception(error)

    # Determine right format and send it accordingly
    if disk_format == "cow":
        disk_format = "qcow2"
    else:
        disk_format = "raw"

    def __get_lvm_size_in_gb(stdout):
        try:
            block_size = stdout.split('SIZE="')[1].split("\"")[0]
        except Exception as ex:
            match_found = re.search("SIZE=\"([A-Z, 0-9]+)\"", stdout)
            if match_found:
                block_size = match_found.group(1)

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

    if is_blk_device(volume_path):
        lvm_info = subprocess.Popen('lsblk -P ' + volume_path, stdout=subprocess.PIPE, shell=True)
        stdout, stderr = lvm_info.communicate()
        if not stderr:
            log.info("STDOUT: {}".format(stdout))
            lvm_size_in_gb = __get_lvm_size_in_gb(stdout)
            if restore_size and actual_size:
                if lvm_size_in_gb < restore_size:
                    log.info("LVM size before extend: {}".format(lvm_size_in_gb))
                    lvm_path = os.readlink(volume_path)
                    extend_by = restore_size - lvm_size_in_gb
                    if extend_by + lvm_size_in_gb > actual_size:
                        log.info(
                            "No more space remained for doing the disk restore. Disk is already being extended to actual size")
   
                    lvm_extend_cmd = "sudo -u root lvextend -L +{}G {}".format(extend_by, lvm_path)

                    lvm_extend = subprocess.Popen(lvm_extend_cmd, stdout=subprocess.PIPE, shell=True)
                    stdout, stderr = lvm_extend.communicate()
                    if stderr:
                        log.error("Failed to extend LVM disk Exception:" + stderr)
                    else:
                        lvm_info = subprocess.Popen('lsblk -P ' + volume_path, stdout=subprocess.PIPE, shell=True)
                        stdout, stderr = lvm_info.communicate()
                        if not stderr:
                            lvm_size = __get_lvm_size_in_gb(stdout)
                            log.info("LVM size after extend: {}".format(lvm_size))
                else:
                    log.info("LVM size is already larger than restore size. No need to extend the disk")
            else:
                log.info("Snapshot restore size or actual size of VM is None. Skipping LVM block extend..")
        else:
            log.error("error getting actual size of the lvm block")

    transfer_qemu_image_to_volume(volume_path, backup_image_file_path, disk_format)


def generate_random_string(string_length=5):
    """Returns a random string of length string_length."""

    random = str(uuid.uuid4())
    random = random.upper()
    random = random.replace("-","")
    return random[0:string_length]
