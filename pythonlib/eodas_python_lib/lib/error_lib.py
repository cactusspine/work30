from os import getenv, getpid
from datetime import datetime
import sys
errors = {
            # error related to arg parsing
            100 : '{0} is not a valid ip',
            101 : '{0} is not a valid authentication string, username and password must be separated by "/"',
            1020: 'A file list must be provided',
            1021: '{0} does not exist',
            1022: 'Unable to read file {0}.',
            103: 'A batch id should be provided' ,
            1031: 'Product id must be provided',
            104: 'Media id must be provvided togheter with the related fsmedinfo file.',
            105: 'Only select and tape list options are incompatible.',
            1051: 'List and query options are not compatible ',
            106: 'media and list option are not compatible',  
            107: 'Media or list should be provided',
            108: 'One of the following options should be provvided: {}',
            1000: '{}',
            # error related to web services
            201: 'web services failed: {0}',
            202: 'web services failing reading wr_rsp',
            203: '{0}',
            204: 'file {} still on disk',
            #tape error
            301: 'no tape was found',
            302: 'No suitable tapes was found',
            303: 'Media in wrong archive: {0}',
            304: 'Error in webservice cmd: {0}',
            305: 'File not found on tape: {0}',
            306: 'Error in fsmedinfo for tape: {0}',
            307: 'No media was selected for checking',
            3071: 'No tape was found in the db',
            3072: 'No tape was found in the HSM',
            308: '{0}', # error retrieving product list from ws
            309: '{0}', # media in the wrong library
            310: 'Media {0}: error sorting files.',
            311: 'Media {0}: tomcat error raised during fsmedinfo command.',
            312: 'Media {0}: is not available',
            313: '{0}', # submitting move operation
            314: '{0}', # found product not in tape
            315: 'No valid tape was found in the list',
            316: 'Tape {0} not found in the db.',
            317: 'Tape {0} not found in the HSM.',
            318: '{} is a not valid status.',
            #db error
            400: 'Error while connecting to the db',
            431: 'db commit {0}',
            42:  'db query no commit: {0}',
            43:  'db query commit: {0}',
            44: 'ESA MD5 missing for product id {}',
            45: 'No records fetched',
            #file error
            500 : '{0}',  # generic
            501 : '{0}',  # md5 failed because md5 is not defined
            502 : '{0}',  # md5 failed because no match
            503 : 'file: {0} was not found neither in product table nor in document one',
            511 : 'ftp upload failed for file : {0}',
            512 : 'ftp download failed for file : {0}',
            513 : 'ftp download failure',
            514 : 'failure when deleting file: {0}',
            515 : 'failure renaming {0}',
            516 : 'file {0} does not exist.',
            517 : 'unable to read file {0}.',
            518 : 'unable to read file {0}.',
            519 : 'Unable to write file {}.',
            520 : 'Directory {} does not exist',
            521 : 'Error while extracting file {}',
            #batch submission
            601 : 'Error fetching next batch id',
            602: '{0} is not a valid batch id',
            #generic
            700: 'No file was found',
            #Ingestion
            800 : 'Product with id {} was already ingested',
            
    } 
def error_def(code, arg=None):
    if arg is None:
        arg=''
    if code in errors.keys():
        return {'code':code, 'msg': errors[code].format(arg)}
    else:
        return {'code':9999, 'msg': 'Unknown error'}

def check_error(proc_id, err_code, CMD, log_file_name, 
                arg_err=None, exit_on_error=False, log_msg=False, msg=None, warning=False):
    if err_code!=0:
        error=error_def(err_code, arg=arg_err)
        err_msg=error['msg']
        if warning:
            err_lev='warning'
        else:
            err_lev='error'
        report_error(proc_id, 'error', CMD, err_msg, log_file_name, arg_err=arg_err)
        if exit_on_error:
            sys.exit(err_code)
    else:
        if log_msg:
            if not msg:
                msg='ok'
            report_error(proc_id, 'message', CMD, msg, log_file_name)

    
def report_error(proc_id, err_lev,CMD, err_msg,  log_file_name, arg_err=None):
    '''
    log error
    :err_lev  error level ERROR, MESSAGE
    :CMD      command
    :err_msg  error message
    :arg_err  optional argument for error msg
    '''
    host_name = getenv('HOSTNAME')
    if not host_name:
        host_name='Unknown'
    
    log_file=open(log_file_name,'a')
        

    msg = ' - '.join([proc_id, host_name])
    msg = '{0} CMD: {1:>30} returns: {2}'.format(msg, CMD, err_msg)
    log_write(msg, err_lev.upper(),  log_file)
    log_file.close()
    

def get_time(format='%Y-%m-%d %H:%M:%S', str=True):
    if str:
        return datetime.now().strftime(format)
    else:
        return datetime.now()

    
def log_write(msg, msg_type, log_file):
    msg = ': '.join([get_time(), '{0:>7}'.format(msg_type), msg])+'\n'
    #print(msg)
    log_file.write(msg)
    
    