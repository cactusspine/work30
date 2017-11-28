import os
import psycopg2
import psycopg2.extras
from   utils import get_cur_time
from error_lib import check_error

def connection(server="eodas_db", db="remote_sensing",
            user="srv_dpmc",passwd="acri%dc4"):
    conn_str = "dbname={0} user={1} password={2} host={3} port=5432".format(db, 
                                                                            user, 
                                                                            passwd, 
                                                                            server)  
    return psycopg2.connect(conn_str)

def connect(server="eodas_db", db="remote_sensing",
            user="srv_dpmc",passwd="acri%dc4", cursor_factory=False ):
    try:
        conn = connection(server=server, db=db,user=user,passwd=passwd)
        if cursor_factory:
            return conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor), {'code': 0, 
                                                                                      'msg': get_cur_time() + ': connected to db'}
        else:
            return conn, conn.cursor(), {'code': 0, 'msg': get_cur_time() + ': connected to db'}
    except psycopg2.Error:
        err = {'code':400}
        err['msg']  = get_cur_time()+' Error while connecting to the db'
        return None, None,err
    
def submit_query(query, cursor, commit=False, conn=None):
    if commit:
        err_code = 43 
        if conn is None:
            return {'code': 431, 'msg':'No db connection available'}
    else:
        err_code = 42
    try:
        cursor.execute(query)
        if commit:
            conn.commit()
        return {'code': 0, 'msg':'ok'}
    except psycopg2.Error as e:
        print e.pgerror
        print e.diag.message_primary
        if conn:
            conn.rollback()
        return {'code': err_code, 'msg':e.diag.message_primary}

def check_query_res(cursor, cmd, log_file, 
                    conn=None, proc_id='Unknown', exit_on_error=False):  
    if cursor.rowcount == 0:
        if conn and not exit_on_error:
            conn.close()
        check_error(proc_id, 45, cmd, log_file, exit_on_error=exit_on_error)


def convert_list_to_db_str(l):
    return "('"+"','".join(l)+"')"
        