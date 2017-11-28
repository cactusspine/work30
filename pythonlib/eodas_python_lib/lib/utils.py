from datetime import datetime
from math import ceil
import argparse, re
from error_lib import check_error
from calendar import month_abbr, monthrange
import re
#==============================================================================
#--- Time and date utils
#==============================================================================
def get_dic_time(time_string, format=None):
    '''
    this function take a string of time in different format
    try guess the format and return a dictionary containing date info
    get_dic_time(time_string, format=None)->dictionary of year, month and day
    if cannot extract date return no-info 
    :param time_string:a string of the starting date time in various format
    :param format:format abbreviation: RFC, IBM, NORM, JUP
    
    Changes
    2017-10-30 First version : JW
    '''
    month_abbr2nr = {v.upper():k for k, v in enumerate(month_abbr)}
    dictime = { k : 'no-info' for k in ['year', 'month', 'day']}
    # the format of the time is RFC3339 string like "2008-09-03T20:56:35.450686Z"
    patternRFC   = "([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]"
    patternRFC  += "([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?"
    patternRFC  += "(([Zz])|([\+|\-]([01][0-9]|2[0-3]):[0-5][0-9]))"
    patternRFC  = re.compile(patternRFC)
    
    patternIBM  = "(([0-9])|([0-2][0-9])|([3][0-1]))\-"
    patternIBM += "((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))\-(\d{4})"
    patternIBM  = re.compile(patternIBM)
    patternNorm = re.compile("(\d{4})\-(0[1-9]|1[012])\-(([0-9])|([0-2][0-9])|([3][0-1]))")
    # jupiter year time 1996-021T07:29:15.907391 ,1996-267T20:16:22;
    patternJupiter = re.compile("(\d{4})\-([0123]\d{2})[Tt](([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60))")
    
    #--- format of the time is RFC3339 
    if format == 'RFC' or re.search(patternRFC, time_string) is not None:
        m = re.search(patternRFC, time_string)
        dictime['year'] = m.group(1)
        dictime['month'] = m.group(2)
        dictime['day'] = m.group(3)
    #--- IBM time format 'stop_date_time': u'26-MAR-1996 01:50:45.015'
    elif format == 'IBM' or re.search(patternIBM, time_string) is not None:
        m = re.search(patternIBM, time_string)
        print m.group(0)
        string_list = m.group(0).split('-')
        dictime['year'] = string_list[2]
        dictime['month'] = '{:02d}'.format(month_abbr2nr[string_list[1].upper()])
        dictime['day'] = string_list[0]
    #--- pattern normal year '2006-02-02 00:04:44+00'    
    elif format == 'NORM' or re.search(patternNorm, time_string) is not None:
        m = re.search(patternNorm, time_string)
        print m.group(0)
        string_list = m.group(0).split('-')
        dictime['year'] = string_list[0]
        dictime['month'] = string_list[1]
        dictime['day'] = string_list[2] 
    #--- pattern match jupiter year
    elif format == 'JUP' or re.search(patternJupiter, time_string) is not None:
        m = re.search(patternJupiter, time_string)
        y = int(m.group(1))
        jd = int(m.group(2))
        dictime = JulianDate_to_MMDDYYY(y, jd)     
    
    return dictime 

def get_cur_time(format="%Y-%m-%d %H:%M:%S"):
    '''
    this function get the currnt time and retrun a str with given format
    '''
    return datetime.now().strftime(format)

def JulianDate_to_MMDDYYY(y, jd):
    '''
    from Gaston , convert Julian Date to dictionary with normal date
    JulianDate_to_MMDDYYY(y,jd)->dictime 
    :output a dictionary containing the year, month, and day keys and their value
    :param y:interger of the year part 
    :param jd:interger of the Jupiter day
    '''
    month = 1
    day = 0
    d = {}

    while jd - monthrange(y, month)[1] > 0 and month <= 12:
            jd = jd - monthrange(y, month)[1]
            month = month + 1
    d['year'] = str(y)
    d['month'] = '{:02d}'.format(month)
    d['day'] = '{:02d}'.format(jd)
    #d_str = datetime.strptime('{}-{}-{}'.format(y,month,jd), '%Y-%m-%d')
    return d
#==============================================================================
#--- generic
#==============================================================================

def remove_duplicate(list):
    '''
    remove the duplicates in one list and keep its order
    :param list: a list object
    #solution from https://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-whilst-preserving-order
    '''
    seen = set()
    seen_add = seen.add
    return [x for x in list if not (x in seen or seen_add(x))]



def convert_list_to_db_str(l,str_format=True):
    if str_format:
        return "('"+"','".join(l)+"')"
    else:
        return "("+",".join(l)+")"


def list_op(set1, set2, op='diff', convert=True):
    if op == 'diff':
        res = set(set1)-set(set2)
    elif op == 'intersect':
        res = set(set1) & set(set2)
    else:
        print('Invalid op option')
    n = len(res)
    if convert:
        if n > 0:
            return n, convert_list_to_db_str(res)
        else:
            return 0, ''
    else:
         return n, list(res) 
     

def get_truncks_number(n_tot, n_sample):
    if n_sample==0:
        print('Sample size must be greather than 1')
        return None   
    return int(ceil(float(n_tot)/float(n_sample)))

def split_list_into_truncs(lst, n_sample):
    if n_sample==0:
        print('Sample size must be greather than 1')
        return None
    n_tot = len(lst)
    n_b = get_truncks_number(n_tot, n_sample)
    return [ lst[i*n_sample:(i+1)*n_sample] for i in range(n_b)]
    
#==============================================================================
#--- argument parsing
#==============================================================================

def process_args_run_job():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--batch-id", action='store', dest='batch_id', 
                        metavar='[batch id]', help=" batch id")
    args = parser.parse_args()
    if  args.batch_id is None:
        print('Error batch id must be provided')
        sys.exit()
        
    msg = 'batch id : {0}'.format(args.batch_id) 
    return args, {'msg': msg, 'code': 0}

def process_args_ingestion(log_file):
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--product-id", action='store', dest='product_id', 
                        metavar='[product id]', help=" product id to be ingested")
    parser.add_argument("--format", action='store', dest='format', 
                        metavar='[metadata format]', help=" format of metadata")
    parser.add_argument("--pattern", action='store', dest='pattern', 
                        metavar='[metadata pattern]', help=" pattern metadata source")
    args = parser.parse_args()
    if  args.product_id is None:
        check_error(proc_id, 1031, CMD, log_file, exit_on_error=True)
    return args, 0

def process_args_hsm_run_job(log_file, default_arg):
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action='store_true', dest='verbose', 
                        help=" Be verbose in output.")
    parser.add_argument("--batch-id", action='store', dest='batch_id', 
                        metavar='[batch id]', help=" batch id")
    
    parser.add_argument(
                        "--authenticate", action='store', dest='authenticate', 
                        default=default_arg['auth'], 
                        metavar='[authentication]', 
                        help=" Authenticate all WS calls, value is 'username/password'."
                        )
    parser.add_argument("--protocol", action='store', dest='protocol', 
                        metavar='[PROTOCOL]', choices=['http', 'https'], 
                        default=default_arg['protocol'], help=" The protocol to use.")
    parser.add_argument("--format", action='store', dest='format', 
                        choices=['text', 'json', 'xml'], default=default_arg['format'], 
                        help=" The format for response.")
    parser.add_argument("--ip", action='store', dest='ip', 
                        default=default_arg['ip'], help=" The ip of the MDC.")
    parser.add_argument("--policy", action='store', dest='policy', 
                        default=default_arg['policy'], help=" The policy for the media")
    
    args = parser.parse_args()
    if  args.batch_id is None:
        check_error(proc_id, 103 , 'parse-arg', log_file,exit_on_error=True)
    #--- check the ip, standard 4 numbers '.' separated
    m = re.compile( '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
    if not m.match( args.ip):
        err_code=100
        check_error(proc_id, 100, CMD, log_file, arg_err=args.ip, 
                    exit_on_error=True)

    #--- check authentication
    if '/' not in args.authenticate:
        check_error(proc_id, 101, CMD, log_file, arg_err=args.authenticate, 
                    exit_on_error=True)
    return args