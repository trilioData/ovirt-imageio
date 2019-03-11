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
                                  meta={'percentage': percentage})

            except Exception as ex:
                pass

        '''qemu_cmd = ["qemu-img", "info", "--output", "json", dest]
        temp_process = subprocess.Popen(qemu_cmd, stdout=subprocess.PIPE)
        data, err = temp_process.communicate()
        data = json.loads(data)
        size = data["actual-size"]
        process.stdin.close()
        self.update_state(state='PENDING',
                          meta={'actual-size': size})'''

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
            op = directio.Send(path,
                               None,
                               size,
                               buffersize=buffer_size)
            total = 0
            print('Executing task id {0.id}, args: {0.args!r} kwargs: {0.kwargs!r}'.format(
                self.request))
            gigs = 0
            with open(dest, "w+") as f:
                for data in op:
                    total += len(data)
                    f.write(data)
                    if total/1024/1024/1024 > gigs:
                        gigs = total/1024/1024/1024
                        percentage = (total/size) * 100
                        self.update_state(state='PENDING',
                                          meta={'percentage': percentage})
            process = subprocess.Popen('qemu-img rebase -u -b ' + recent_snap_path + ' ' + dest, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            if stderr:
                log.error("Unable to change the backing file", dest, stderr)
        else:
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
                        shutil.copy(path, tempdir)
                        backing_file = os.path.basename(str(record.get('backing-filename', None)))

                        command = 'qemu-img rebase -u -b ' + backing_file + ' ' + filename
                        commands.append(command)
                    except IOError as e:
                        print("Unable to copy file. %s" % e)
                        raise e
                    except:
                        error = "Unexpected error : [{0}]".format(sys.exc_info)
                        print(error)
                        raise Exception(error)
                else:
                    try:
                        shutil.copy(path, tempdir)
                        command = 'qemu-img rebase -u ' + filename
                        commands.append(command)
                    except IOError as e:
                        print("Unable to copy file. %s" % e)
                        raise e
                    except:
                        print("Unexpected error:", sys.exc_info())
                        raise Exception(sys.exc_info())
                    break
                path = str(record.get('full-backing-filename'))
            string_commands = ";".join(str(x) for x in commands)
            process = subprocess.Popen(string_commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE
                                       , cwd=tempdir, shell=True)
            stdout, stderr = process.communicate()
            if stderr:
                shutil.rmtree(tempdir)
                raise Exception(stdout)
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
                                      meta={'percentage': percentage})

                except Exception as ex:
                    pass
            if recent_snap_path:
                process = subprocess.Popen('qemu-img rebase -u -b ' + recent_snap_path + ' ' + dest, stdout=subprocess.PIPE, shell=True)
                stdout, stderr = process.communicate()
                if stderr:
                    log.error("Unable to change the backing file", dest, stderr)
            del_command = 'rm -rf ' + tempdir
            delete_process = subprocess.Popen(del_command, shell=True, stdout=subprocess.PIPE)
            delete_process.communicate()


@app.task(bind=True, name="ovirt_imageio_daemon.celery_tasks.restore")
def restore(self, ticket_id, volume_path, backup_image_file_path, disk_format, size, buffer_size):

    def transfer_qemu_image_to_volume(
            volume_path,
            backup_image_file_path,
            disk_format):

        # Get base path and base volume name for creating tmp dir
        filename = os.path.basename(volume_path)
        temp_dir = os.path.dirname(volume_path) + "/tmp"

        # Get Backing file if present for current disk.
        qemu_cmd = ["qemu-img", "info", "--output", "json", volume_path]
        qemu_process = subprocess.Popen(qemu_cmd, stdout=subprocess.PIPE)
        data, err = qemu_process.communicate()
        data = json.loads(data)
        backing_file = data.get("backing-filename", None)

        log_msg = 'Qemu info for [{0}] : [{1}]. Backing Path: [{2}]'.format(volume_path, data, backing_file)
        print log_msg

        target = volume_path

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

        percentage = 0.0
        while percentage < 100:
            try:
                try:
                    output = queue.get(timeout=300)
                except Empty:
                    continue
                except Exception as ex:
                    print(ex)

                try:
                    percentage = re.search(r'\d+\.\d+', output).group(0)
                except AttributeError as ex:
                    pass

                try:
                    percentage = float(percentage)
                except Exception as ex:
                    print ex
                    raise ex

                print(("copying from %(backup_path)s to "
                           "%(volume_path)s %(percentage)s %% completed\n") %
                          {'backup_path': backup_image_file_path,
                           'volume_path': target,
                           'percentage': str(percentage)})

                percentage = float(percentage)

                self.update_state(state='PENDING',
                                  meta={'percentage': percentage})
            except Exception as ex:
                raise ex

        process.stdin.close()

        _returncode = process.returncode  # pylint: disable=E1101
        if _returncode:
            print(('Result was %s' % _returncode))
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            error = "Execution error %(exit_code)d (%(stderr)s). cmd %(cmd)s" % {'exit_code': _returncode,
                     'stderr': process.stderr.read(),
                     'cmd': cmd}
            self.update_state(state='EXCEPTION',
                              meta={'exception': error})
            raise Exception(error)


        if backing_file:
            try:
                self.update_state(state='PENDING',
                                  meta={'status': 'Performing Rebase operation to point disk to its backing file'})
                print 'Rebasing volume: [{0}] to backing file: [{1}]. ' \
                      'Volume format: [{2}]'.format(volume_path, backing_file, disk_format)

                basedir = os.path.dirname(volume_path)
                process = subprocess.Popen('qemu-img rebase -u -b ' + backing_file + ' ' + filename, stdout=subprocess.PIPE,
                                            cwd=basedir, shell=True)
                stdout, stderr = process.communicate()
                if stderr:
                    error = "Unable to change the backing file "+ volume_path + " " + stderr
                    log.error(error)
                    raise Exception(error)

            except IOError as ex:
                print ex
                log.error("Unable to move temp file as temp file was never created. Exception: ", ex)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': ex})
                raise Exception(ex)

            except Exception as ex:
                print ex
                error = 'Error occurred: [{0}]'.format(ex)
                self.update_state(state='EXCEPTION',
                                  meta={'exception': error})
                raise Exception(error)

            finally:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

        # Clean the temp files created in restore process if any
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    # Determine right format and send it accordingly
    if disk_format == "cow":
        disk_format = "qcow2"
    else:
        disk_format = "raw"

    transfer_qemu_image_to_volume(volume_path, backup_image_file_path, disk_format)

def generate_random_string(string_length=5):
    """Returns a random string of length string_length."""

    random = str(uuid.uuid4())
    random = random.upper()
    random = random.replace("-","")
    return random[0:string_length]
