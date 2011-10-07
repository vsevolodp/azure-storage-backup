def show_usage():
    print r"""
Downloads all blobs or uploading them back.

Usage:
  backup_bs b[ackup] <path> [account_name secret_key]
  backup_bs r[estore] <path> [account_name secret_key]"""

import sys, os, errno
from winazurestorage import *

def read_args(argv):
    if (not (len(sys.argv) == 3 or len(sys.argv) == 5)):
        show_usage()
        sys.exit(1)

    args = {}

    args['command'] = argv[1]

    if (args['command'] == 'b'):
        args['command'] = 'backup'
    if (args['command'] == 'r'):
        args['command'] = 'restore'

    if (args['command'] != 'backup' and args['command'] != 'restore'):
        show_usage()
        sys.exit(1)

    args['base_path'] = argv[2]

    if (len(sys.argv) == 3):
        args['host'] = DEVSTORE_BLOB_HOST
        args['account_name'] = DEVSTORE_ACCOUNT
        args['secret_key'] = DEVSTORE_SECRET_KEY
    else:
        args['host'] = CLOUD_BLOB_HOST
        args['account_name'] = argv[3]
        args['secret_key'] = argv[4]

    return args

args = read_args(sys.argv)

client = BlobStorage(host = args['host'], account_name = args['account_name'], secret_key = args['secret_key'])

if (args['command'] == 'backup'):
    containers = client.list_containers()
    if containers is None:
        raise IOError

    for container_name, etag, last_modified in containers:
        blobs = client.list_blobs(container_name)
        for blob_name, etag, last_modified in blobs:
            full_name = os.path.join(args['base_path'], container_name, blob_name)
            print os.path.join(container_name, blob_name)

            content = client.get_blob(container_name, blob_name)
            if content is None:
                raise IOError

            path = os.path.dirname(full_name)

            try:
                os.makedirs(path)
            except OSError as exc: # Python >2.5
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    print 'Can not create directory ' + path
                    raise

            f = open(full_name, 'wb')
            f.write(content)
            f.close
else:
    for container in os.listdir(args['base_path']):
        dir_path = os.path.join(args['base_path'], container)
        code = client.create_container(container)
        if (not code in [201, 409]):
            raise IOError(code, 'Error creating container \'{0}\''.format(container))

        for subdir, containers, file_names in os.walk(dir_path):
            for file_name in file_names:
                file_path = os.path.join(subdir, file_name)
                blob = file_path[len(dir_path) + 1:].replace('\\','/')

                blob_path = '{0}/{1}'.format(container, blob)
                if (os.stat(file_path).st_size > 64 * 1024 * 1024):
                    print 'ERROR: {0} - blobs bigger than 64 Mb is not supported'.format(blob_path)
                    continue
                else:
                    print blob_path

                f = open(file_path, 'rb')
                content = f.read()
                f.close

                code = client.put_blob(container, blob, content)
                if (code != 201):
                    raise IOError(code, 'Error creating blob \'{0}/{1}\''.format(container, blob))
