import hashlib 
from os import system as os_system
from os import listdir
from os.path import join as os_join
from os.path import isdir
from os import makedirs
from os.path import isfile, basename, getsize
from os.path import splitext
from os import getenv
from os import rename as file_rename
from time import sleep
from zipfile import ZipFile
import ftplib
import time


from utils import convert_list_to_db_str
from db_query import db_query
from db_utils import submit_query
from error_lib import check_error

db_query=db_query()


def create_file_path(file_name, file_path):
    if not isdir(file_path):
        makedirs(file_path)
    return os_join(file_path, file_name)



def md5_evaluate(f_name, block_size=65536):
    hash = hashlib.md5()    
    try:
        with open(f_name, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                hash.update(chunk)
        return hash.hexdigest()
    except:
        return None

def md5_check(f_name, md5):  
    md5_file = md5_evaluate(f_name)
    if md5_file is None:
        err_msg = 'file {0}: failed to calculate the md5'.format(f_name)
        return None, {'code': 501, 'msg':err_msg}
    if md5_file==md5:
        return md5_file, {'code': 0, 'msg':  'file {0}: md5 check success'.format(f_name)}
    else:
        return md5_file, {'code': 502, 'msg':'file {0}: md5 check failed'.format(f_name)}
    
def get_product_dir(f_path):
    tmp = f_path.split('/')[:-1]
    i = tmp.index('global')
    return '/'.join(tmp[i+1:])

def rsync(f_name, source_dir, target, max_attempts=3):
    '''
    function to sync file
    :param f_name: file name
    :param source source directory
    :param target target directory
    '''
    err = {'code': 0, 'msg':''}
    status=1
    attempt=0
    error=0
    source_file=os_join(source_dir, f_name)
    while (status != 0 and attempt < max_attempts ):
        attempt += 1
        status=os_system("rsync -auxvr '%s' '%s'" % (source_file,target))
        if not ( status == 0 ):
            sleep(5)
    if (attempt == max_attempts):
        err['code'] = status
        if err['code'] != 0:
            err['msg']='Error after {0} attempts'.format(attempt) 
    return err    


def rsync_file(source_file, target_file, max_attempts=3):
    '''
    function to sync file
    :param source_file source file
    :param target_file target file
    '''
    err = {'code': 0, 'msg':''}
    status=1
    attempt=0
    error=0 
    while (status != 0 and attempt < max_attempts ):
        attempt += 1
        status=os_system("rsync -auxvr '%s' '%s'" % (source_file,target_file))
        if not ( status == 0 ):
            sleep(5)
    if (attempt == max_attempts):
        err['code'] = status
        if err['code'] != 0:
            err['msg']='Error after {0} attempts'.format(attempt) 
    return err    

def get_product_md5(p_name, conn, cursor, log_file, proc_id='Unknown', pid=False):
    CMD = 'get-prod-md5' 
    query = db_query.get_md5_product(p_name)
    err   = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], CMD, log_file, arg_err=err['msg'])
    if err['code'] !=0:
        if pid:
            return err['code'], None, None
        else:
            return err['code'], None
    res = cursor.fetchone()
    if res is None:
        if pid:
            return 501, None, None
        else:
            return 501, None
    else:
        try:
            if pid:
                return 0, res['md5'], res['id']
            else:
                return 0, res['md5']
        except:
            if pid:
                return 0, res[0], res[1]
            else:
                return 0, res[0]

    
def get_product_md5_and_path(p_name,conn, cursor, log_file, 
                             proc_id='Unknown', p_id=None):
    CMD = 'get-prod-md5-path' 
    query = db_query.get_prod_path_md5(p_name)
    err   = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], CMD, log_file, arg_err=err['msg'])
    info = {
            'md5' : None, 
            'path': None, 
            }
     
    if err['code'] !=0:
        err_code=err['code']
    else:
        res = cursor.fetchone()
       
        if res:
            err_code=0
            if p_id:
                info['id']=None
            try: 
                for k in info.keys():
                    info[k]=res[k]
            except:
                for i,k in enumerate(info.keys()):
                    info[k]=res[i]
        else:
            err_code=501
        
    return err_code, info
    
       
def get_md5_path_from_db(f_lst,conn, cursor, log_file, 
                             proc_id='Unknown', 
                             hsm_root=False,
                             dms_root=False,
                             product_id=False,
                             cursor_factory=False):
    CMD = 'get-prod-md5-path'
    f_lst_str=convert_list_to_db_str(f_lst)
     
     
        
    query=db_query.get_prod_path_md5_products(p_name_lst=f_lst_str,
                                              hsm_root=hsm_root,
                                              dms_root=dms_root)
    err   = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], CMD, log_file, arg_err=err['msg'])
    keys=['product_file', 'md5', 'id', 'name']
    
    if product_id:
        keys.append('id')
    
    out={k:[] for k in keys}
        
    if err['code']==0:
        err_code = 0
        if cursor_factory:
            for res in cursor:
                for k in keys:
                    out[k].append(res[k])
        else:
            for res in cursor:
                for i,k in enumerate(keys):
                    out[k].append(res[i])
    return err['code'], out
    
  

def test_zip(zip_file):
    with ZipFile(zip_file, 'r', allowZip64=True) as f:
        test=f.testzip()
        f.close()
    if test:
        return {'code': 3, 'msg': 'corrupted zip'}
    else: 
        return {'code': 0, 'msg': 'ok'}
    
       

def zip_dir(zip_name, dir_to_zip):
    if isdir(dir_to_zip):
        with ZipFile(zip_name, 'w') as f:
            for fs in listdir(dir_to_zip):
                ff = os_join(dir_to_zip, fs)
                f.write(ff)
            f.close()
        return {'code' : 0, 'msg': '{} succesfully created'.format(zip_name)}
    else:
        return {'code' : 1021, 'msg': '{} is not a directory'.format(dir_to_zip)}

   
def check_if_file_exist(f):
    if isfile(f):
        return {'code': 0, 'msg': 'ok'}
    else:
        return {'code': 2, 
                'msg': 'file {0} does not exist'.format(f)}
        
    
def connect_ftp(host="131.176.196.42", user="ftp_adm", password='eodas%am3', passive=False, TLS=True):
    if TLS:
        ftp_conn = ftplib.FTP_TLS(host,user,password)
        ftp_conn.set_pasv(passive)
        ftp_conn.prot_p()
    else:
        ftp_conn = ftplib.FTP(host,user,password)
        ftp_conn.set_pasv(passive)
    return ftp_conn

def upload(ftp_conn, my_File, target_dir='/from_main/web/'):
    _ERROR=0
    attempt=0
    exit_code=1
    max_attempts = getenv('MAX_RETRIES')
    if max_attempts is None:
        max_attempts = 3
    else:
        max_attempts = int(max_attempts)
    sleep_retry=getenv('SLEEP_RETRY')
    if sleep_retry is None:
        sleep_retry = 5
    else:
        sleep_retry = int(sleep_retry)
    while (exit_code != 0 and attempt < max_attempts):
        attempt += 1
        try:
            exit_code=0
            ext = splitext(my_File)[1]
            f_name = basename(my_File)
            lock_file   = os_join(target_dir, '.'+f_name)
            unlock_file = os_join(target_dir, f_name)
            cmd = ''.join(['STOR ', lock_file])
            if ext in (".txt", ".htm", ".html", ".php", ".csv"):
                # ACII mode
                ftp_conn.storlines(cmd, open(my_File))
            else:
                #binary mode
                ftp_conn.storbinary(cmd, open(my_File))
            ftp_conn.rename((lock_file),(unlock_file))
        except ftplib.all_errors as e:
            exit_code=e
        if not (exit_code == 0):
            time.sleep(sleep_retry)
    if attempt == max_attempts:
        _ERROR=exit_code
    return _ERROR


def download(ftp, remote_file, target_dir):
    error=0
    attempt=0
    status=1
    max_attempts = getenv('MAX_RETRIES')
    if max_attempts is None:
        max_attempts = 3
    else:
        max_attempts = int(max_attempts)
    sleep_retry=getenv('SLEEP_RETRY')
    if sleep_retry is None:
        sleep_retry = 5
    else:
        sleep_retry = int(sleep_retry)        
    ext        = splitext(remote_file)[1]
    f_name     = basename(remote_file)
    lock_file  = os_join(target_dir, '.'+f_name)
    local_file = os_join(target_dir, f_name)
    while (status != 0 and attempt < max_attempts):
        attempt += 1
        try:
            status=0
            with open(lock_file, 'wb') as f:
                 ftp.retrbinary('RETR %s' % remote_file, f.write)
            file_rename(lock_file,local_file)   
        except ftplib.all_errors as e:
            status=e
        if status != 0:
            time.sleep(sleep_retry)
    if attempt==max_attempts:
        error=status
    return error

def ftp_folder_list(remote_dir, ftp_conn=None, passive=False):
    if ftp_conn is None:
        try:
            ftp_conn = connect_ftp(passive=passive)
        except:
            return 520, []
    try:
        file_lst=ftp_conn.nlst(remote_dir)
    except:
        print( ftplib.all_errors)
        return 524, []
    
    return 0, [f for f in file_lst ]
            
            
        
    

def ftp_dir_download(local_dir, remote_dir, passive=False):
    exit_code=513
    downloaded_lst = []
    failed_lst     = []
    try:
        conn_ftp = connect_ftp(passive=passive)
        file_lst=conn_ftp.nlst(remote_dir)
    except ftplib.all_errors as e:
        exit_code = e
        return exit_code, [], []
    
    file_lst=[f for f in file_lst if f[0]!='.' ]
    err = [None for f in file_lst]
    for i,f_remote in enumerate(file_lst):
        err[i]=download(conn_ftp, f_remote, local_dir)
        if err[i] == 0:
            downloaded_lst.append(f)
        else:
            failed_lst.append([512, f])
    conn_ftp.close()
    if len(failed_lst)==0:
        exit_code=0
        
    return exit_code, downloaded_lst, failed_lst
    

def ftp_upload(myfile, target_dir='/from_main/web/', host="131.176.196.42", 
               user="ftp_adm", password='eodas%am3', passive=False):
    try:
        conn_ftp = connect_ftp(passive=passive, host=host, user=user, password=password)
        STATUS   = upload(conn_ftp, myfile, target_dir=target_dir)
        conn_ftp.quit()
    except ftplib.all_errors as e:
        STATUS = e
    return STATUS
   
def ftp_delete_file(target_file, passive=False):
    exit_code=514
    try:
        conn_ftp = connect_ftp(passive=passive)
        conn_ftp.delete(target_file)
        exit_code = 0
        msg=''
        conn_ftp.close()
    except ftplib.all_errors as e:
        msg = e
    return exit_code, msg
            
'''           
def extract_checksum_file(zip_file, outfile=None):
    if outfile is None:
        outfile='./checksum.txt'
    with ZipFile(zip_file, 'r', allowZip64=True) as f_zip:
        for f in f_zip.namelist():
            if 'checksum.txt' in f:
                out=open(outfile,'wb')
                out.write(f_zip.read(f))
                out.close()
                return 0
    return 521
'''
def extract_checksum_file(zip_file, outfile=None):
    if outfile is None:
        outfile='./checksum.txt'
    err=extract_file_from_zip(zip_file, 'checksum.txt' , outfile=outfile)
    return err

def extract_file_from_zip(zip_file, source_file, outfile=None):
    if outfile is None:
        outfile='./'+source_file
    with ZipFile(zip_file, 'r', allowZip64=True) as f_zip:
        for f in f_zip.namelist():
            if source_file == f:
                out=open(outfile,'wb')
                out.write(f_zip.read(f))
                out.close()
                return 0
    return 521
    

def read_checksum_from_zip(zip_file):
    out=None
    with ZipFile(zip_file, 'r', allowZip64=True) as f_zip:
        for f in f_zip.namelist():
            if 'checksum.txt' in f:
                out=f_zip.read(f)
                break
    return out
            