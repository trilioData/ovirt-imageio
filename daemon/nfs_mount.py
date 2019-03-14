import base64
import os
import subprocess
from subprocess import check_output
import logging
import logging.config

log = logging.getLogger("server")

def is_online(nfsshare):
    status = False
    try:
        nfsserver = nfsshare.split(":")[0]
        cmd = subprocess.Popen("rpcinfo -s " + nfsserver, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        rpcinfo, error = cmd.communicate()
        if not error:
          for i in rpcinfo.split("\n")[1:]:
              if len(i.split()) and i.split()[3] == 'mountd':
                  status = True
                  break
        else:
          log.exception("Nfs Server " + nfsserver + " is not Online")
    except Exception as ex:
        log.exception(ex)

    return status

def is_mounted(nfsshare, mountpath):
    '''Make sure backup endpoint is mounted at mount_path'''
  
    if not os.path.ismount(mountpath):
        return False

    with open('/proc/mounts', 'r') as f:
        mounts = [{line.split()[1]:line.split()[0]}
                  for line in f.readlines() if line.split()[1] == mountpath]

    return len(mounts) and mounts[0].get(mountpath, None) == nfsshare

def mount_backup_target(nfsshare, mountpath):
    if is_online(nfsshare):
        command = ['sudo', 'mount', nfsshare, mountpath]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        stdout, stderr = process.communicate()
        if stderr:
            log.exception(stderr)
            return False
    else:
        log.exception("NFS Server is Offline")
        return False
    return True