from utils import get_cur_time as get_time
from os import getenv, getpid 
import sys

def log_write(msg, msg_type, log_file):
    msg = ': '.join([get_time(), msg_type, msg])+'\n'
    #print(msg)
    log_file.write(msg)
    
 
def report_error(err_lev, proc_name, CMD, error_msg, log_file_name):
    '''
    log error:
    :param err_lev error level ERROR, MESSAGE, WARNING
    :param proc_name process name
    :param CMD command
    :param error_msg error message
    :param log_file_name log file name
    '''
    
    log_file=open(log_file_name,'a')
    host_name = getenv('HOSTNAME')
    if not host_name:
        host_name='unknown'
    
    msg = ' - '.join([host_name, proc_name])
    if err_lev == 'message':
        msg = '{0}'.format(msg)
    else:  
        msg = '{0} Cmd {1} returns {2}'.format(msg, CMD, error_msg)
    log_write(msg, err_lev.upper(),  log_file)
    log_file.close()
    
def check_error(err, proc_name, cmd, log_file,exit_if_error=True, write_msg=False):
    if err['code'] == 0:
        if write_msg:
            report_error('message', proc_name, cmd, err['msg'], log_file)
    else:
        report_error('error', proc_name, cmd, err['msg'], log_file)
        if exit_if_error:
            sys.exit(err['code'])
            
def write_report_msg(msg, log_file_name, header=False):
    log_file=open(log_file_name,'a')
    log_file.write(msg+'\n')
    log_file.close()
    
        