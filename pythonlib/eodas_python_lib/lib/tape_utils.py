import requests
from json import loads as json_load
from json import load as json_f_load
import urllib
from error_lib import check_error
from random import sample
import sys
from copy import deepcopy
from db_utils import submit_query
from db_query import db_query
from utils import get_cur_time, get_truncks_number
from file_utils import get_md5_path_from_db
requests.packages.urllib3.disable_warnings()
    

def check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error, 
                  proc_id='Unknown', retrieval=False):
    err=201
    err_msg = ws_rsp['statuses'][0]['statusText']
    for status in ws_rsp['statuses']:
        if status['commandStatus'] == 'completed' or status['statusNumber'] in [355, 390]:
            err = 0
            err_msg = 'command completed' 
            break
    
    '''
    if ws_rsp['statuses'][0]['statusNumber'] == 0:
        err = 0
        err_msg=''
    elif 
    
    if ws_rsp['statuses'][0]['statusNumber'] != 0:
        err=201
        err_msg = ws_rsp['statuses'][0]['statusText']
        
    else: 
        err = 0
        err_msg=''
    '''
    check_error(proc_id, err, CMD, log_file, arg_err=err_msg, 
                    exit_on_error=exit_on_error)
    return err
        

def check_if_suitable_for_sending(media,  arch, args):
    CMD = 'check-if-suitable'
    cmd = '/media/fsmedinfo/'
    
    try:
        ws_rsp = do_webservices_cmd(cmd, args, {'media': media})
        if isinstance(ws_rsp, tuple):
            ws_rsp=ws_rsp[1]
        ws_rsp = json_load(ws_rsp)
        cur_arch = ws_rsp['medias'][0]['currentArchive']
        percentUsed = float(ws_rsp['medias'][0]['percentUsed'])
    except:
        cur_arch = 'N/A'
        percentUsed = 0
    return cur_arch==arch and percentUsed>=args.percentUsed 


def check_if_media_available(media,  arch, args, check_usage=False):
    CMD = 'check-if-media-available'
    cmd = '/media/fsmedinfo/'
    
    try:
        ws_rsp = do_webservices_cmd(cmd, args, {'media': media})
        if isinstance(ws_rsp, tuple):
            ws_rsp=ws_rsp[1]
        ws_rsp = json_load(ws_rsp)
        cur_arch = ws_rsp['medias'][0]['currentArchive']
        if check_usage:
            percentUsed = float(ws_rsp['medias'][0]['percentUsed'])
    except:
        cur_arch = 'N/A'
    if check_usage:
        test= (cur_arch==arch) and (percentUsed>=args.percentUsed) 
    else:
        test=cur_arch==arch
    return test


def check_available_media(tape_lst, args, proc_id, log_file):
    n_tape = len(tape_lst)
    CMD='check-tapes-availability'
    cmd='/media/fsmedinfo/'
    arch=args.arch
    
    err_code, ws_rsp=do_webservices_cmd(cmd, args, params={'media': tape_lst})
    if err_code != 0:
        check_error(proc_id, 201, CMD, log_file, arg_err=ws_rsp, exit_on_error=True)
    else:
        try:
            ws_rsp = json_load(ws_rsp)
            av_tape = [ a['mediaId'] for a in ws_rsp['medias'] if a['currentArchive']==arch]
        except:
            check_error(proc_id, 202, CMD, log_file,exit_on_error=True)
    n_av_tape=len(av_tape)
    if n_av_tape ==0:
        check_error(proc_id, 302, CMD, log_file,exit_on_error=True)
    else:
        if n_tape==n_av_tape:
            msg = 'all identified tapes are available'.format(n_av_tape, n_tape)
        else:
            msg = '{0} over {2} available tapes were identified: {1}'.format(n_av_tape, ','.join(av_tape), n_tape)
        check_error(proc_id, 0, CMD, log_file, log_msg=True, msg=msg)    
    
    return av_tape



def do_webservices_cmd( cmd, args, params={}):
    #--- set up the URL prefix
    if 'wsconfig' in cmd:
        prefix = "http://%s:81/sws/v2" % (args.ip,)
    elif 'https' in args.protocol:
        prefix = "https://%s:443/sws/v2" % (args.ip,)
    else:
        prefix = "http://%s:81/sws/v2" % (args.ip,)
    ws = prefix + cmd
    # if we do not already have a format parameter
    if 'format' not in params:
        params['format'] = args.format

    # add login parameters if we need to authenticate
    if args.authenticate != '':
        params['username'], params['password'] = args.authenticate.split('/')
    # all web-service URLs are lower case
    ws = ws.lower()

    response = requests.get(ws, params, verify=False)
   
    if args.verbose:
        print('''
                WS: {0}
              '''.format(urllib.unquote(response.url)))
    if response.status_code == 200:
        return 0, response.text
    else:
        return 201, response.reason


def fix_stored_name(f_name):
    file_name=f_name.split('/')[-1]
    if file_name[0]=='.':
        file_name = file_name[1:]
    ext=file_name.split('.')[-1]
    if ext != 'zip':
        file_name = '.'.join(file_name.split('.')[:-1])
    return f_name.replace(f_name.split('/')[-1], file_name)    


def get_prod_name(f_name):
    file_name=f_name.split('/')[-1]
    if file_name[0] =='.':
        file_name=file_name[1:]
    ext=file_name.split('.')[-1]
    if ext == 'zip':
        p_name = '.'.join(file_name.split('.')[:-1])
    else:
        p_name = '.'.join(file_name.split('.')[:-2])
    return p_name


def get_available_media_list(args, log_file, exit_on_error=True, arch=None, 
                  proc_id='Unknown', check_usage=False):
    CMD    = 'get-available-media-list'
    ws_cmd = '/media/fsmedlist/state'
    tape_list = []
    
    param = {'available':'true'}
    if arch:
        if arch=='vault':
            param['available'] = 'false' 
    
    if args.policy =='blank':
        param['blank']='true'
    else:
        param['policy']=args.policy 
    
    err, ws_rsp = do_webservices_cmd(ws_cmd, args, param)    
    check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
    ws_rsp = json_load(ws_rsp)
    
    err=check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error, proc_id=proc_id)
    if err == 0:
        med_lst =ws_rsp['classes'][0]['available']['medias']
        if arch is not None:
            tape_list = [ 
                         m['mediaId'] for m in  med_lst 
                         if check_if_media_available(m['mediaId'],arch, 
                                                     args, check_usage=check_usage)
                         ]
        else:
            tape_list = [ m['mediaId'] for m in  med_lst]
    return tape_list


def get_full_media_list(args, log_file, policy, exit_on_error=True,  
                        proc_id='Unknown'):
    CMD       = 'get-full-media-list'
    ws_cmd    = '/media/fsmedlist/state'
    param = {
             'protect'     :'true', 
             'unformatted' : 'true'
             }
    err, ws_rsp = do_webservices_cmd(ws_cmd, args, param)    
    check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
    ws_rsp = json_load(ws_rsp)
    err=check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error, proc_id=proc_id)
    
    tape_report = { p: { 'suspect'     : [],
                         'error'       : [],
                         'available'   : [],
                         'unavailable' : [],
                         'vault'       : [],
                         'tot'         : 0
                        } for p in policy }
    tape_report['blank'] = {
                            'write-unprotect': 0,
                            'tot'            : 0
                            }
    '''
    if err==0:
        classes = [ c for c in ws_rsp['classes'] if c['classId'] in policy] 
        for c in classes: 
        
            p=c['classId']
            tape_report[p][] = []
    '''     
               
    
def get_import_date(args, media, log_file, exit_on_error=False, 
                  proc_id='Unknown'):
    CMD='get-tape-importdate'
    ws_cmd = '/media/fsmedinfo'
    err, ws_rsp = do_webservices_cmd(ws_cmd, args, {'media': media})
    check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
    
    import_date = None
    
    if err==0:
        ws_rsp = json_load(ws_rsp)
        err=check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error, proc_id=proc_id)
        if err==0:
            import_date = ws_rsp['medias'][0]["importDate"]
    return import_date
       

def get_file_sample(media, args,sample_size, log_file, 
                  max_attempt=3,exit_on_error=False, 
                  proc_id='Unknown', info=None, perc=True):
    CMD='ws-get-file-sample'
    ws_cmd     = '/media/fsmedinfo'
     # --- query prod list from stornext
    attempt = 0
    check   = True
    products = []
    while check and attempt<max_attempt: 
        attempt += 1
        err, ws_rsp = do_webservices_cmd(ws_cmd, args, {'media': media, 
                                                        'verbose': 'true'})
        check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
        if err != 0:
            continue
        try:
            ws_rsp = json_load(ws_rsp)
        except:
            check_error(proc_id, 311 , CMD, log_file, arg_err=media, 
                    exit_on_error=exit_on_error)
            attempt=max_attempt
            break
            
        err=check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error, proc_id=proc_id)
        
        if err != 0:
            continue
        products = [ fix_stored_name(prod['path']) for prod in ws_rsp['medias'][0]['files']]
        n_file = len(products)
        if perc:
            if sample_size<100:
                n_sel  = min(int(sample_size*n_file), n_file)
            else:
                n_sel =n_file
        else: 
            n_sel = min(n_file, sample_size)
        if n_sel < n_file:
            msg = 'Media {0}: {2} out of {1} files selected'.format(media, n_file, n_sel)
            check_error(proc_id, err, CMD, log_file, msg=msg, log_msg=True)
            products = sample(products, n_sel)
        
        check = False
        
    if attempt==max_attempt and check:
        err_msg='Media {0}: failed to get product list after {1} attempts'.format(media, 
                                                                                  attempt)
        err_code= 308
    else:        
        err_code= 0
        err_msg=''
    check_error(proc_id, err_code, CMD, log_file,  
                arg_err=err_msg, exit_on_error=exit_on_error)
    if info is not None:
        if err_code ==0:
            return products, err_code, {k: ws_rsp['medias'][0][k] for k in info if k in ws_rsp['medias'][0].keys()}
        else:
            return products, err_code, {}
    else:
        return products, err_code


def get_file_sample_from_file(media, sample_size, log_file,
                              fsmedinfofile,exit_on_error=False, 
                              proc_id='Unknown', info=None):
    CMD='fs-get-file-sample'
    with open(fsmedinfofile,'r') as f:
        try:
            ws_rsp = json_f_load(f)
        except:
            msg= 'Media {0} - file {1}: unable to parse the fsmedinfo out'.format(media, 
                                                                                  fsmedinfofile)
            check_error(proc_id, 500, CMD, log_file, arg_err=msg, 
                    exit_on_error=exit_on_error)
            if info is None:
                return [], 517
            else:
                return [], 517, {}
    
    products = [ fix_stored_name(prod['path']) for prod in ws_rsp['medias'][0]['files']]
    n_file = len(products)
    n_sel  = int(sample_size*n_file)
    msg = 'Media {0}: {2} out of {1} files selected'.format(media, n_file, n_sel)
    check_error(proc_id, 0, CMD, log_file, msg=msg, log_msg=True)
    products = sample(products, n_sel)
    if info is not None:
        info_out = {k: ws_rsp['medias'][0][k] for k in info if k in ws_rsp['medias'][0].keys()}
        return products,0, info_out
    return products, 0


def get_prod_info(media, args, cursor, query, sample_size, log_file, 
                  max_attempt=3, exit_on_error=False, conn=None, 
                  proc_id='Unknown'):
    
    CMD='get-prod_info'
    products, err = get_file_sample(media, args,sample_size, log_file, 
                  max_attempt=max_attempt,exit_on_error=exit_on_error, 
                  proc_id=proc_id)
    n_sel = len(products)
    if err==0:
        product_names = {get_prod_name(prod):prod  for  prod in products}
        prod_names = "('"+"','".join(list(product_names.keys()))+"')"
        err   = submit_query(query(prod_names), cursor, conn=conn)
        check_error(proc_id, err['code'], CMD, log_file, arg_err=err['msg'])
        if err['code']==0:
            prod_info = { product_names[prod[0]]:{'md5': prod[2], 
                                   'path': prod[1]} for prod in cursor }
            
        products = list(prod_info.keys())
        n_prod   = len(products)
        msg = 'Media {0}: {1} out of {2} of selected files were products'.format(media,
                                                                                  n_prod, 
                                                                                  n_sel)
        check_error(proc_id, 0, CMD, log_file, msg=msg, log_msg=True)
        return prod_info, n_prod, products
    else:
        return None, 0, None


def get_tape_info(media, args, info):
    CMD = 'get-tape-info'
    cmd = '/media/fsmedinfo/'
    try:
        ws_rsp = do_webservices_cmd(cmd, args, {'media': media})
        if isinstance(ws_rsp, tuple):
            ws_rsp=ws_rsp[1]
        ws_rsp = json_load(ws_rsp)
        info_out = {k: ws_rsp['medias'][0][k] for k in info if k in ws_rsp['medias'][0].keys()}
    except:
        info_out = {}
    return info_out 

    
def get_md5_file(file_lst,args,log_file, proc_id='Unknown'):
    CMD = 'get-file-info'
    cmd = '/file/fsfileinfo/'   
    ws_rsp = do_webservices_cmd(cmd, args, {'file': file_lst,'checksum' : 'true' })
    try:
        ws_rsp = do_webservices_cmd(cmd, args, {'file': file_lst,'checksum' : 'true' })
        
        if isinstance(ws_rsp, tuple):
            ws_rsp=ws_rsp[1]
        ws_rsp = json_load(ws_rsp)
    except:
        check_error(proc_id, 202, CMD, log_file)
        return 202, []
    
    # check if commamnd aws succefull
    
                
    if len(ws_rsp['fileInfos'])<len(file_lst):
        for m in ws_rsp['statuses'][:-1]:
            #print m['statusText']
            check_error(proc_id, 203, CMD, log_file, arg_err=m['statusText'])
    
    
    md5_lst=[]
    
    for i,f in enumerate(ws_rsp['fileInfos']):
        if f['location'] == 'DISK':
            check_error(proc_id, 204, CMD, log_file, arg_err=f['fileName'])
        else:
            md5_lst.append({
                            'i': file_lst.index(f['fileName']), 
                            'md5':f['checksums'][0]['checksumValue'] 
                            })
    return 0, md5_lst
 
def update_tape_status(media_lst, args, status, log_file,proc_id='Unknown'):
    '''
    this will change the status of a list of tapes
    
    '''
    CMD='update-tape-status'
    suitable_status =['unsusp', 'protect', 'unprotect', 'avail', 'unavail', 'unmark'] 
    if status not in suitable_status:
        check_error(proc_id, 318, CMD, log_file, arg_err=status)
        return {'code':err_code}
    
    ws_cmd = '/media/fschmedstate'
    err_code, ws_rsp = do_webservices_cmd(ws_cmd, args, params={'media': media_lst, 
                                                                'state': status})
    check_error(proc_id, err_code, CMD, log_file, arg_err=ws_rsp)
    
    return {'code':err_code}
    
    
    

def move_tape(media_lst, args, log_file, destination='vault', proc_id='Unknown'):
    '''
    this will move the tapes in media_lst to an archive destination
    by default the operation vault the tapes
    '''
    CMD='move-tape'
    ws_cmd = '/media/vsmove'
    err_code, ws_rsp = do_webservices_cmd(ws_cmd, args, params={'media': media_lst, 
                                                                'archive': destination})
    check_error(proc_id, err_code, CMD, log_file, arg_err=ws_rsp)
    if err_code !=0:
        return {'code':err_code}
    #print ws_rsp
    ws_rsp = json_load(ws_rsp)
    # check operation
    check_for_err = True
    if 'vsmoveSuccess' in ws_rsp['vsmoveOutput'].keys():
        check_for_err = not int(ws_rsp['vsmoveOutput']['vsmoveSuccess']['vsmoveSuccessCompleted']) == len(media_lst)
        return {'code':0,  'msg': 'All {0} media(s) are moving to {1}'.format(len(media_lst), 
                                                                               destination)}
    if check_for_err:    
        error = 0
        msgs = []
        for err_tmp in ws_rsp['vsmoveOutput']['mediaErrors']:
            if err_tmp['mediaErrorText'] !='no err':
                error += 1
                msgs.append( { 'media' : err_tmp['mediaErrorMedium'],
                               'error' : err_tmp['mediaErrorText']})
                
        msg = '{0} out of {2} error(s) moving media(s) to {1}'.format(error, destination, len(media_lst))
        return {'code': 313, 'msg':msg, 'errors':msgs}
    

def retrieve_file_from_tape(f_lst, log_file, args, proc_id='Unknown'):
    CMD='retrieve-file-from-tape'
    ws_cmd='/file/fsretrieve'
    err_code, ws_rsp = do_webservices_cmd(ws_cmd, args, params={'file': f_lst})
    check_error(proc_id, err_code, CMD, log_file, arg_err=ws_rsp)
    if err_code !=0:
        return err_code
    ws_rsp = json_load(ws_rsp)
    err=check_ws_rsp_status(ws_rsp, log_file, CMD, exit_on_error=False, proc_id=proc_id)
    
    return err
    
def retrieve_tape_lst(f_lst, log_file, proc_id, args, f_sample=40):
    CMD='retrieve-tape-list'
    cmd='/file/fsfileinfo'
    n_files= len(f_lst)
    file_by_tape= {}
    n_truncks = get_truncks_number(n_files, f_sample)
    for i in range(n_truncks):
        i0 = i*f_sample
        i1 = min((i+1)*f_sample, n_files+1)
        err_code, ws_rsp = do_webservices_cmd(cmd, args, params={'file': f_lst[i0:i1]})
        if err_code != 0:
            check_error(proc_id, 201, CMD, log_file, arg_err=ws_rsp)
            continue
        else:
            try:
                ws_rsp = json_load(ws_rsp)
                for item in ws_rsp['fileInfos']:
                    if int(item['medias'][0]['copy'])==1:
                        media_id=item['medias'][0]['mediaId']
                    else:
                        media_id=item['medias'][1]['mediaId']
                    if media_id not in file_by_tape.keys():
                        file_by_tape[media_id] = []
                    file_by_tape[media_id].append(item['fileName'])
                
                '''                
                tape_lst = tape_lst |  { a['medias'][0]['mediaId'] if int(a['medias'][0]['copy'])==1 
                                                                   else a['medias'][1]['mediaId'] 
                                                                   for a in ws_rsp['fileInfos'] }
                '''
            except:
                check_error(proc_id, 202, CMD, log_file)
                continue
            
    tape_lst = list(file_by_tape.keys())
    n_tape = len(tape_lst)
    if n_tape == 0:
        check_error(proc_id, 301, CMD, log_file, exit_on_error=True)
    else:
        msg = '{0} tapes were identified: {1}'.format(len(tape_lst), ','.join(tape_lst))
        check_error(proc_id, 0, CMD, log_file, log_msg=True, msg=msg)
    return file_by_tape, tape_lst

def sort_prod_on_tape_db(file_list, args, media ,log_file, conn, cursor, arch=None, exit_on_error=False, 
                  proc_id='Unknown', get_prod=False, cursor_factory=False):
    '''
    Sort the products on the base of starting block
    :param: prod_list, list with full path of product in stornext
    '''
    CMD = 'sort-products'
    ws_cmd = '/file/fsfiletapeloc'
    block_id = []
    block_id_app = block_id.append
    
    prod_lst = [get_prod_name(f) for f in file_list]
    # --- retrieve prod path and md5
    err, out = get_md5_path_from_db(prod_lst,conn, cursor, log_file,
                         proc_id=proc_id, hsm_root=True,
                         cursor_factory=cursor_factory)

    
    for prod_fixed in out['product_file']:
        err, ws_rsp = do_webservices_cmd(ws_cmd, args, {'file':prod_fixed})
        check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
        if err != 0:
            continue
        ws_rsp = json_load(ws_rsp)
        if ws_rsp['statuses'][0]["statusText"] == "Command Successful.":
            block_id_app(ws_rsp["fileInfo"]["startBlock"])
            if arch is not None:
                if ws_rsp["fileInfo"]["libraryId"]!=arch:
                    err_msg = 'media {0} not in {1} but in {2}'.format(
                                                                       media,
                                                                       arch,
                                                                       ws_rsp["fileInfo"]["libraryId"]
                                                                       )
                    check_error(proc_id, 309, CMD, log_file, arg_err=err_msg, 
                                exit_on_error=exit_on_error)
                    return [], 310
        else:
            block_id_app(None)
            print prod_fixed
            err_msg = 'media: {} {} '.format(media, prod_fixed)
            err_msg += ws_rsp['statuses'][0]["statusText"]
            print err_msg
            check_error(proc_id, 304, CMD, log_file, arg_err=err_msg, 
                                exit_on_error=exit_on_error)
            
    product_sorted = [p for _,p in sorted(zip(block_id,out['product_file']))]
    if not get_prod:
        pTmp = [fix_stored_name(p) for p in product_sorted]
        product_sorted = pTmp
    err_msg = 'Media {0}: {1} out of {2} products were sorted'.format(media, 
                                                                    len(product_sorted),
                                                                    len(out['product_file']))
    check_error(proc_id, 0, CMD, log_file, log_msg=True, msg=err_msg)
    return product_sorted, err
  
  
def sort_prod_on_tape(prod_list, args, media ,log_file, arch=None, exit_on_error=False, 
                  proc_id='Unknown', get_prod=False):
    '''
    Sort the products on the base of starting block
    :param: prod_list, list with full path of product in stornext
    '''
    CMD = 'sort-products'
    ws_cmd = '/file/fsfiletapeloc'
    block_id = []
    block_id_app = block_id.append
    
    for prod in prod_list:
        prod_fixed = fix_stored_name(prod)
        err, ws_rsp = do_webservices_cmd(ws_cmd, args, {'file':prod_fixed})
        check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
        if err != 0:
            continue
        ws_rsp = json_load(ws_rsp)
        if ws_rsp['statuses'][0]["statusText"] == "Command Successful.":
            block_id_app(ws_rsp["fileInfo"]["startBlock"])
            if arch is not None:
                if ws_rsp["fileInfo"]["libraryId"]!=arch:
                    err_msg = 'media {0} not in {1} but in {2}'.format(
                                                                       media,
                                                                       arch,
                                                                       ws_rsp["fileInfo"]["libraryId"]
                                                                       )
                    check_error(proc_id, 309, CMD, log_file, arg_err=err_msg, 
                                exit_on_error=exit_on_error)
                    return [], 310
        else:
            block_id_app(None) 
            err_msg = 'media: '+media
            err_msg += prod + ws_rsp['statuses'][0]["statusText"]
            check_error(proc_id, 304, CMD, log_file, arg_err=err_msg, 
                                exit_on_error=exit_on_error)
            
    product_sorted = [p for _,p in sorted(zip(block_id,prod_list))]
    if not get_prod:
        pTmp = [fix_stored_name(p) for p in product_sorted]
        product_sorted = pTmp
    ''' 
    block_id_sorted = deepcopy(block_id)
    block_id_sorted.sort()
    
    ind_block = [block_id.index(id) for id in block_id_sorted if id is not None]
    
    if get_prod:
        product_sorted = [get_prod_name(prod_list[id]) for id in ind_block]
    else:
        product_sorted = [fix_stored_name(prod_list[id]) for id in ind_block]
    '''
    err_msg = 'Media {0}: {1} out of {2} products were sorted'.format(media, 
                                                                    len(product_sorted),
                                                                    len(prod_list))
    check_error(proc_id, 0, CMD, log_file, log_msg=True, msg=err_msg)
    return product_sorted, 0
   

def update_prod_info(media, args, cursor,conn, log_file,
                     max_attempt=3, exit_on_error=False, proc_id='Unknown', 
                     info=None):
    err_msg = 'Media {0}: '.format(media)
    err_msg += '{}' 
    
    CMD='update-prod-info-list'
    print '{0} - Media {1}: start query hsm'.format(get_cur_time(), media)
    if info is None:
        products, err = get_file_sample(media, args,100, log_file, 
                                        max_attempt=max_attempt,
                                        exit_on_error=exit_on_error, 
                                        proc_id=proc_id)
    else:
        products, err, info_out = get_file_sample(media, args,100, log_file, 
                                        max_attempt=max_attempt,
                                        exit_on_error=exit_on_error, 
                                        proc_id=proc_id, info=info)
    
    print '{0} - Media {1}: end query hsm'.format(get_cur_time(), media)
    if err!=0:
        if info is None:
            return {'code' : err}
        else:
            return {'code' : err}, {}
    product_names = [get_prod_name(prod)  for  prod in products]
     
    prod_names = "('"+"','".join(product_names)+"')"
    
    CMD='update-prod-info-check'
#    print '{0} - Media {1}: start status check'.format(get_cur_time(), media)
    query = db_query().count_prod_not_in_tape(prod_names)
    err   = submit_query(query, cursor, conn=conn)
    
    check_error(proc_id, err['code'], CMD, log_file, arg_err=err_msg.format(err['msg']))
    print '{0} - Media {1}: end status check'.format(get_cur_time(), media)
    if err['code']!=0:
        if info:
            return err, {}
        else:
            return err     
    n_not_tape =cursor.fetchone()[0]
    print '{0} - Media {1}: {2} files do not have tape status'.format(get_cur_time(), media, n_not_tape) 
    if  n_not_tape > 0:
        err = {'code':314, 'msg': '{} out of {} products are not in tape status'.format(n_not_tape, 
                                                                                        len(product_names))}
        check_error(proc_id, err['code'], CMD, log_file, arg_err=err_msg.format(err['msg']))
        
        query = db_query().get_prod_not_in_tape(prod_names)
        err_0   = submit_query(query, cursor, conn=conn)
        check_error(proc_id, err_0['code'], CMD, log_file, arg_err=err_msg.format(err['msg']))
        
        for c in cursor:
            msg ='{0} - Media {1}: {2}, {3}, {4}'.format(' '*19,media, c[0], c[1], c[2] ) 
            print  msg
            check_error(proc_id, 314, CMD, log_file, arg_err=msg)
            
        if info:
            return err, {}
        else:
            return err   
        
    CMD='update-prod-info'
    print '{0} - Media {1}: start status update'.format(get_cur_time(), media)
    query = db_query().update_prod_status(prod_names)
    err   = submit_query(query, cursor, conn=conn, commit=True)
    check_error(proc_id, err['code'], CMD, log_file, arg_err=err_msg.format(err['msg']))
    print '{0} - Media {1}: end status update'.format(get_cur_time(), media)
        
    if info is None:
        return err
    else:
        return err, info_out
            

def write_fsmedinfo_on_file(media, args,log_file, proc_id, f_name=None,
                      max_attempt=3,exit_on_error=False, format='text' ):
    CMD='ws-get-fsmedinfo-out'
    ws_cmd  = '/media/fsmedinfo'
     # --- query prod list from stornext
    attempt = 0
    check   = True
    products = []
    while check and attempt<max_attempt: 
        attempt += 1
        err, ws_rsp = do_webservices_cmd(ws_cmd, args, {
                                                        'media': media, 
                                                        'verbose': 'true', 
                                                        'format':format
                                                        })
        check_error(proc_id, err, CMD, log_file, arg_err=ws_rsp, 
                    exit_on_error=exit_on_error)
        if err != 0:
            continue
        err = int(ws_rsp.split('\n')[0].split(':')[-1].strip())
        if err==0:
            check=False
            
            try:
                with open(f_name, 'w') as f:
                    f.write(ws_rsp)
                    f.close()
                err_msg = 'Media {0}: fsmedinfo succefully written in {1}'.format(media, f_name)
                err_code = 0
                
            except:
                err_code = 518
                err_msg  = ''
                
            check_error(proc_id, err_code, CMD, log_file,  
                            arg_err=err_msg, exit_on_error=exit_on_error)
            return err_code
       
    if attempt==max_attempt and check:
        err_msg='Media {0}: failed to get fsmedinfo {1} attempts'.format(media, 
                                                                                  attempt)
        err_code= 308
    else:        
        err_code= 0
        err_msg=''
    check_error(proc_id, err_code, CMD, log_file,  
                arg_err=err_msg, exit_on_error=exit_on_error)
    return err_code
   
    
    