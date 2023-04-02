import logging

#logger = logging.getLogger(__name__)
logger = logging.getLogger('entsoe')
FORMAT = "%(asctime)s [%(filename)s:%(lineno)s, %(funcName)s()] %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, 
                    datefmt='%Y-%m-%d %H:%M:%S', 
                    level=logging.INFO)

#######################################################################################################
def sftp_sync_dirs(
    hostname = None,
    username = None,
    password = None,

    remote_dir = None,
    local_dir = None,
):
    import os
    import pysftp
    import datetime
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    
    list_modified_files = list()
    
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        
        if not sftp.exists(remote_dir):
           logger.error("Remote folder '{}' does not exist".format(remote_dir))
           return -1
        
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir, exist_ok = True) 
                logger.info("Directory {} created successfully".format(local_dir)) 
            except OSError as error: 
                logger.error("Directory '{}' cannot be created".format(local_dir))
                return -1
        
        sftp.cwd(remote_dir)
        for f in sftp.listdir_attr():
            filename = f.filename
            local_filename = os.path.join(local_dir, filename)
            remote_filename = os.path.join(remote_dir, filename)
            
            if sftp.isfile(filename):
                if not os.path.exists(local_filename):
                    logger.info("Downloading new file %s..." % filename)
                    sftp.get(filename, local_filename, preserve_mtime = True)
                    list_modified_files.append(filename)
                else:
                    remote_file_mtime = f.st_mtime
                    local_file_mtime = os.path.getmtime(local_filename)
                     
                    if remote_file_mtime > local_file_mtime:
                        # import datetime
                        # print(
                        #     filename,
                        #     datetime.datetime.fromtimestamp(remote_file_mtime), 
                        #     datetime.datetime.fromtimestamp(local_file_mtime)
                        # )
                        logger.info("Downloading %s..." % filename)
                        logger.info(
                            "File modified on remote server. Remote mtime: {}, Local mtime: {}"
                            .format(
                                datetime.datetime.fromtimestamp(remote_file_mtime).strftime('%Y-%m-%d-%H:%M:%S'),
                                datetime.datetime.fromtimestamp(local_file_mtime).strftime('%Y-%m-%d-%H:%M:%S')
                            )
                        )
                        sftp.get(filename, local_filename, preserve_mtime = True)
                        list_modified_files.append(filename)

    return list_modified_files
                    
#######################################################################################################
def download_remote_file(
    hostname = None,
    username = None,
    password = None,
    
    filename = None,
    remote_dir = None,
    local_dir = None,
):
    """
    
    """
    import pysftp
    import os

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")
        
        remote_path = os.path.join(remote_dir, filename)
        local_path = os.path.join(local_dir, filename)
        
        if not sftp.exists(remote_path):
            print("Remote file '{}' does not exist".format(remote_path))
            return -1
        
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir, exist_ok = True) 
                print("Directory {} created successfully".format(local_dir)) 
            except OSError as error: 
                print("Directory '{}' cannot be created".format(local_dir))
                return -1
        
        sftp.get(remote_path, local_path, preserve_mtime = True)
        
    # connection closed automatically at the end of the with-block

#######################################################################################################
def download_remote_folder(
    hostname = None,
    username = None,
    password = None,
  
    
    remote_path = None,
    local_path = None,
):
    """
    
    """
    import pysftp
    import os

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("Connection succesfully stablished ... ")
        
        if not sftp.exists(remote_path):
            print("Remote Directory '{}' does not exist".format(remote_path))
            return -1
        
        if not os.path.exists(local_path):
            try:
                os.makedirs(local_path, exist_ok = True) 
                print("Directory {} created successfully".format(local_path)) 
            except OSError as error: 
                print("Directory '{}' cannot be created".format(local_path))
                return -1
        
        sftp.get_d(remote_path, local_path, preserve_mtime = True)
        
    # connection closed automatically at the end of the with-block
