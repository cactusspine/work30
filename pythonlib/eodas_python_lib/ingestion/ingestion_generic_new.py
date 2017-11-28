'''
The script perform the ingestion of a generic product

'''
'''
===============================================================================
extractMetada functions for the metadata extraction from the raw metadata files
AUTHOR: Jiali WANG
===============================================================================
Changes:
===============================================================================
   Date          Author(s)                     Comment
2017-10-29 : JW+GB                     First dev version
2017-11-03 : JW+GC                     First prod version
===============================================================================
'''
#===============================================================================
#--- Dependencies
#===============================================================================
#------ Python Modules
#===============================================================================
import sys
import os.getenv as os_getenv
from os import makedirs, getpid, chdir
from os import remove as remove_file
from os.path import join as os_join
from os.path import isdir, isfile

 
#===============================================================================
#------ Custom Modules
#===============================================================================
from db_query_ingestion import db_query_ingestion as db_query
sys.path.append('/exports/eodas/scripts/scripts/eodas_python_lib/lib')
from db_utils import connect as db_connect
from db_utils import submit_query, check_query_res
from error_lib import check_error
from util import process_args_ingestion_pid as process_args
#===============================================================================
#------ Internal function
#===============================================================================
        
db_query=db_query()
proc_id='ingestion'

def main():
    
    log_dir = str(os_getenv('SYSTEM_LOG'))
    log_dir += '/DL197' 
    if not isdir(log_dir):
        makedirs(log_dir)
    # log file
    log_global  = os_join(log_dir, 'global_ingestion.log')
    log_zip_chk = os_join(log_dir, 'check_zip_content.log')
    
    args=process_args(log_global, default_arg)
    product_id=args.product_id
    proc_id += '_'+str(product_id)
    if args.format is None:
        metadata_fromat='xml'
    else:
        metadata_fromat=args.format
    if args.pattern is None:
        metadata_pattern='/*/*.metadata'
    else:
        metadata_pattern=args.pattern
    
    #--- check processing directories
    pid = str(getpid())
    processing_dir = '{}/{}'.format(os_getenv('PROCESSING_DIR'),pid)
    processing_dir = '{}/{}'.format('/tmp',getpid())
    if not isdir(processing_dir):
        try:
            makedirs(processing_dir)
        except:
           msg = 'Unable to create the directory {}'.format(processing_dir)
           check_error(proc_id, 500, 'create-processing-dir', log_global, 
                       exit_on_error=True, arg_err=msg)
           
    dir_lst=[pid, 'testzipdir', 'testzipdir2']
    for d in dir_lst:
        dTmp = '{}/{}'.format(processing_dir,d)
        if not isdir(dTmp):
            try:
                makedirs(dTmp)
            except:
               msg = 'Unable to create the directory {}'.format(dTmp)
               check_error(proc_id, 500, 'create-processing-dir', log_global, 
                           exit_on_error=True, arg_err=msg)
    #--- go to local working directory
    chdir(processing_dir)
    
    #-- db connection 
    conn, cursor, err = db_connect()
    check_error(proc_id, err['code'], 'db-connect', log_global, exit_on_error=True)
    
    #--- Getting the product status
    query=db_query.get_product_status(product_id)
    err = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], 'get-product-status', log_global, 
                exit_on_error=True, arg_err=err['msg'])
    check_query_res(cursor, 'get-product-status', log_global, 
                    conn=conn, exit_on_error=True)
    
    product_status=cursor.fetchone()[0]
    
    # check if this is a new attempt to ingest a previously ARCHIVED product
    print ('PRODUCT_STATUS : '+product_status)
    if product_status != 'NEW':
        conn.close()
        check_error(proc_id, 800, 'get-product-status', log_global, 
                exit_on_error=True, arg_err=product_id)
        
    # update the product status to ACTIVE
    query = db_query.update_product_status(product_id, 'ACTIVE')
    err = submit_query(query, cursor, conn=conn, commit=True)
    check_error(proc_id, err['code'], 'upd-product-status', log_global, 
                exit_on_error=True, arg_err=err['msg'])
    
    # retrieve the ingestion parameters
    query = db_query.get_product_info(product_id)
    err = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], 'get-product-info', log_global, 
                exit_on_error=True, arg_err=err['msg'])
    
    check_query_res(cursor, 'get-product-info', log_global, 
                    conn=conn, exit_on_error=True)
    dTmp = cursor.fetchone()
    product_name=dTmp[0]
    product_type=dTmp[1]
    print 'Product Name: {}'.format(product_name)
    print 'Product Type: {}'.format(product_type)
    
    query = db_query.get_initial_path(product_id)
    err = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], 'get-product-info', log_global, 
                exit_on_error=True, arg_err=err['msg'])
    
    check_query_res(cursor, 'get-product-info', log_global, 
                    conn=conn, exit_on_error=True)
    
    query=db_query.get_duplicated_prod(product_id)
    err = submit_query(query, cursor, conn=conn)
    check_error(proc_id, err['code'], 'get-product-info', log_global, 
                exit_on_error=True, arg_err=err['msg'])
    
    
    


if __name__ == "__main__":
        main()