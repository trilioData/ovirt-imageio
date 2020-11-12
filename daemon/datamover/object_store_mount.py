import s3fuse # This is the name of the module created by our pip install command

s3fuse.mount()
#
# # umount and clean all the files and folder generated from the mount
# s3fuse.umount()
#
# """
# Fixed Required
#     There is a bug that build one additional mount points.
#     For now, this function will remove the files and folders from this mountpoint.
# """
# s3fuse.clean()