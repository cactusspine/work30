#! /exports/eodas/software/bin/python

# -------------------------------------------------------------------
#
# This script perform a direct ingestion of products from the DAS server
#
# History:
#
# 2016-07-21 :  gbr : initial version
# 2017-10-24 :  jw  : editing for test data
#
# -------------------------------------------------------------------

# -------------------------------------------------------------------
## Error codes
#  1 : syntax error
#  2 : failure during insertion of product_type
#  3 : failure during insertion of product
#  4 : failure during update of dataset_info
#  5 : failure during creation of processing directory
#  6 : failure during move to processing directory
#  7 : failed to connect to database
#  8 : failure during selection of product_status
#  9 : product already archived
# 10 : database update failure
# 11 : failure during selection of ingestion parameters
# 12 : failure during rsync between nas and node
# 13 : failed to select the status of product in the DMS
# 14 : wrong product status in the DB
# 15 : error during unzip received product in processing directory
# 16 : error during metadata extraction in processing directory
# 17 : error during EODAS container integrity check
# 18 : error during EODAS container content check
# 19 : error during EODAS container hashcode generation
# 20 : error during creation of path in primary archive
# 21 : error during creation of path in secondary archive
# 22 : error during rsync to primary archive
# 23 : error during check EODAS container in primary archive
# 24 : error during rsync to secondary archive
# 25 : error during check EODAS container in secondary archive
# 26 : error during archiving center record in DMS
# 27 : error during sensor record in DMS
# 28 : error during platform record in DMS
# 29 : error during product_info record in DMS
# 30 : product archiving path record failed
# 31 : product path is not correctly referenced in DB
# 99 : new attempt to archive a product still archived
# -------------------------------------------------------------------

import os
import sys
import re
import datetime
import shutil
import time
import zipfile
import hashlib
import psycopg2
import subprocess
import sys
from xml.etree import ElementTree 
from cStringIO import StringIO
import imp
import glob

#-- custom modules
#sys.path.append('/exports/eodas/dev/jwang/parser')
sys.path.append('/exports/eodas/scripts/scripts/tape_tools/lib')#this is  the final
import parserjw

from db_utils import connect as db_connectfun
from db_utils import submit_query as submit_query
#--- TODO set the database to the remote_sensing_test_1
#db_connect = imp.load_source('db_connect','/exports/eodas/scripts/db_connect_test_remote_sensing_luxembourg.py')	# TEST DATABASE
#db_connect = imp.load_source('db_connect','/exports/eodas/scripts/db_connect_remote_sensing.py')	# REAL DATABASE
db_connect= db_connectfun( db='remote_sensing_test_1')
def date_utc():
	return datetime.datetime.now()

def log_message(msg):
	LOG_FILE=open(LOG_FILENAME,'a')
	LOG_FILE.write(str(date_utc())+" : "+msg+'\n')
	LOG_FILE.close()

def log_message2(msg):
	LOG_FILE=open(LOG_FILENAME2,'a')
	LOG_FILE.write(str(date_utc())+" : "+msg+'\n')
	LOG_FILE.close()	
	
def log_warning(msg):
	LOG_FILE=open(LOG_FILENAME,'a')
	LOG_FILE.write(str(date_utc())+" : WARNING : "+msg+'\n')
	LOG_FILE.close()

def log_error(msg):
	LOG_FILE=open(LOG_FILENAME,'a')
	LOG_FILE.write(str(date_utc())+" : ERROR : "+msg+'\n')
	LOG_FILE.close()

def log_fatal(msg):
	LOG_FILE=open(LOG_FILENAME,'a')
	LOG_FILE.write(str(date_utc())+" : FATAL : "+msg+'\n')
	LOG_FILE.close()
	
def report_error(a, b, c, d, e):
	# get arguments   
	REP_PROC_NAME=a
	REP_ERR_LEVEL=b
	REP_CMD=c
	REP_CMD_STATUS=d
	REP_TXT=e
	# record error and alert the operator
	if (REP_ERR_LEVEL=="warning"):
		log_warning(REP_PROC_NAME+" - "+os.getenv('HOSTNAME')+" - "+REP_TXT+" - Cmd "+REP_CMD+" returns "+REP_CMD_STATUS)
	elif (REP_ERR_LEVEL=="message"):
		log_message(REP_PROC_NAME+" - "+os.getenv('HOSTNAME')+" - "+REP_TXT+" - Cmd "+REP_CMD+" returns "+REP_CMD_STATUS)
	elif (REP_ERR_LEVEL=="error"):
		log_error(REP_PROC_NAME+" - "+os.getenv('HOSTNAME')+" - "+REP_TXT+" - Cmd "+REP_CMD+" returns "+REP_CMD_STATUS)
	elif (REP_ERR_LEVEL=="fatal"):
		log_fatal(REP_PROC_NAME+" - "+os.getenv('HOSTNAME')+" - "+REP_TXT+" - Cmd "+REP_CMD+" returns "+REP_CMD_STATUS)
	
def process_error(a, b, c):
	# get arguments
	P_ID=a
	P_ST=b
	P_RS=c
	# update the product status
	CURRENT_DATE=date_utc()
	conn, cursor, err_test=db_connectfun()
	cursor.execute("UPDATE eodas.product_status SET status='%s', comment='%s' WHERE product_status.product_id='%s'" % (P_ST, P_RS, P_ID))
	conn.commit()	
	conn.close()

def check_zip_file(myzip):
	_FILE=zipfile.ZipFile(myzip,'r')
	_STATUS=_FILE.testzip()
	_FILE.close()
	return _STATUS
  
def rm_file(a):
	_ERROR=0
	_FILE=a
	_NB=0
	_STATUS=1
	while (_STATUS != 0 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		_STATUS=os.system("rm -f '%s'" % _FILE)
		if not (_STATUS == 0):
			time.sleep(int(os.getenv('SLEEP_RETRY')))
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	return _ERROR

def rm_dir(folder):
	_ERROR=0
	_FILE=folder
	_NB=0
	_STATUS=1
	while (_STATUS != 0 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		_STATUS=os.system("rm -rf '%s'" % _FILE)
		if not (_STATUS == 0):
			time.sleep(int(os.getenv('SLEEP_RETRY')))
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	return _ERROR

def create_dir(path):
	_ERROR=0
	_DIR=path
	_NB=0
	_STATUS=1
	SLEEP_RETRY=5.0
	while (_STATUS != 0 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		_STATUS=os.system("mkdir -p '%s'" % path)
		if not (_STATUS == 0):
			time.sleep(int(os.getenv('SLEEP_RETRY')))
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	return _ERROR

def rsync_file(_file,source,target):
	_ERROR=0
	_FILE=_file
	_SOURCE=source
	_TARGET=target
	_NB=0
	_STATUS=1
	_PATH=_SOURCE+'/'+_FILE
	while (_STATUS != 0 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		print ("RSYNC : rsync -lptoDuxvr %s %s" % (_PATH, _TARGET))
		_STATUS=os.system("rsync -lptoDuxvr %s %s" % (_PATH, _TARGET))
		if not ( _STATUS == 0 ):
			time.sleep(int(os.getenv('SLEEP_RETRY')))
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	return _ERROR

def getmd5(filepath):
	return str(hashlib.md5(open(filepath,'rb').read()).hexdigest())

def compareMD5(md5,MD5):
	compare=False
	if md5 == MD5:
		compare=True
	return compare

def check_zip_content(myzipfile):
	_ERROR=0
	_FILE=myzipfile
	_NB=0
	_STATUS=1
	# We try to unzip the product.zip file 3 times before failure
	while (_STATUS != 0 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		_STATUS=os.system("unzip %s -d %s " % (_FILE, PROCESSING_DIR+'/testzipdir'))
		if not (_STATUS == 0):
			time.sleep(int(os.getenv('SLEEP_RETRY')))
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	for files in os.listdir(PROCESSING_DIR+'/testzipdir'):
		if (str(files) == 'checksum.txt') or (str(files) == 'info.txt'):
			pass
		else:
			md5_file = getmd5(PROCESSING_DIR+'/testzipdir/'+files)
			cmd = "grep '%s' '%s'" % (str(files),(PROCESSING_DIR+'/testzipdir/checksum.txt'))
			sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			initial_md5 = sp.communicate()[0].split(' ')[0]
			if compareMD5(initial_md5,md5_file):
				log_message2("CHECK : "+str(os.path.basename(myzipfile))+" : "+str(files)+" : "+"OK")
			else:
				log_message2("CHECK : "+str(os.path.basename(myzipfile))+" : "+str(files)+" : "+"KO")
	
	return _ERROR
	
def check_zip_content2(myzipfile):
	_ERROR=0
	_FILE=myzipfile
	_NB=0
	_STATUS=1
	# We try to get the listing of zipfile 3 times before failure
	while (_STATUS != 3 and _NB < int(os.getenv('MAX_RETRIES'))):
		_NB=_NB+1
		myzip = zipfile.ZipFile(myzipfile,'r')
		_STATUS=len(myzip.namelist())
		if not _STATUS == 3:
			time.sleep(int(os.getenv('SLEEP_RETRY')))
		myzip.close()
	if (_NB == int(os.getenv('MAX_RETRIES'))):
		_ERROR=_STATUS
	myzip = zipfile.ZipFile(myzipfile,'r')
	myzip.extract('checksum.txt',PROCESSING_DIR+'/testzipdir/')
	checksum_file=open(PROCESSING_DIR+'/testzipdir/checksum.txt','r')
	list_checksum_file=checksum_file.readlines()
	list_product=[]
	for files in list_checksum_file:
		list_product.append(files.rstrip().split(' ')[-1].split('/')[-1])
	for files in myzip.namelist():
		if (str(files) == 'checksum.txt') or (str(files) == 'info.txt'):
			pass
		else:
			if files in list_product:
				log_message2("CHECK : "+str(os.path.basename(myzipfile))+" : "+str(files)+" : "+"OK")
			else:
				log_message2("CHECK : "+str(os.path.basename(myzipfile))+" : "+str(files)+" : "+"KO")
				_ERROR=1
	checksum_file.close()
	myzip.close()
	return _ERROR	

def main():

	conn, cursor, err_test=db_connectfun()
	if err_test['code']!=0:
		#report_error("direct","fatal","product",str(PRODUCT_ID),
		#			"Failed to connect database,{}".format(err_test['msg']),file=LOG_FILENAME)
		sys.exit(30)
	
	# default value of the exit code
	# ----------------------------------------------------------------------------------
	ERROR=0
	#print sys.version_info
	
	# Creation global variable for ingestion
	# ----------------------------------------------------------------------------------
	global LOG_FILENAME
	global LOG_FILENAME2
	global PROCESSING_DIR
	global PRODUCT_NAME

	
	# Creation of a log file: 1 for global log + 1 for zip content check
	# ----------------------------------------------------------------------------------
	LOG_FILENAME=str(os.getenv('SYSTEM_LOG'))+'/DL197/global_ingestion.log'  	# global log file for the dataset ingestion
	LOG_FILENAME2=str(os.getenv('SYSTEM_LOG'))+'/DL197/check_zip_content.log'  # log file to control the content of eodas container
	PATTERN = ['/*/*.metadata']
	FORMAT  = 'xml'
	# check syntax and read command-line parameters
	# ----------------------------------------------------------------------------------
	
	if (sys.argv[1] == ''):# or sys.argv[2]=='' or sys.argv[3]==''):
		print 
		print " Syntax: $0 product_id  $1 format $2 search_pattern_for_metadata"
		print
		sys.exit(1)
	else:
		PRODUCT_ID=sys.argv[1]
	#	FORMAT=sys.argv[2]
	#	PATTERN=sys.argv[3]
		print ('-------------------------------------------------------------')
		print 'productID=',(PRODUCT_ID),' format=',(FORMAT),' pattern=',(PATTERN)
		
		

	# Creation of a processing directory for working
	# ----------------------------------------------------------------------------------
	tmp=str(os.getpid())
	#PROCESSING_DIR=str(os.getenv('PROCESSING_DIR'))+'/'+tmp
	PROCESSING_DIR='/work/scratch/'+tmp
	print('PROCESSING_DIR: '+PROCESSING_DIR)
	#PROCESSING_DIR='/tmp/'+tmp

	try:
		os.makedirs(PROCESSING_DIR+'/'+tmp)		   					# the processing directory
		os.makedirs(PROCESSING_DIR+'/testzipdir') 				 	# folder for zip integrity check in case of zip/tar/tgz... product
		os.makedirs(PROCESSING_DIR+'/testzipdir2') 					# folder to control the content of the EODAS package
		#os.makedirs(str(os.getenv('SYSTEM_LOG'))+'/DL197') # folder for the log file
	except OSError as e:
		print e
		report_error("direct","error","mkdir_failure","","error during creation of processing directory")
		sys.exit(5)

	
	# go to local working directoryl
	# ----------------------------------------------------------------------------------
	try:
		os.chdir(PROCESSING_DIR)
		print ('os has changed to processing dir')
	except OSError:
		report_error("direct","error","chdir_failure","","error during move to processing directory")
		sys.exit(6)
		
		
	# Getting the product status
	# ----------------------------------------------------------------------------------
	try:
		cursor.execute("SELECT status FROM eodas.product_status WHERE product_id = '%s' ORDER BY product_id DESC LIMIT 1" % PRODUCT_ID)
		PRODUCT_STATUS=cursor.fetchone()[0]
	except psycopg2.Error:
		report_error("direct","error","product_status",str(PRODUCT_ID),"failed to select the status of product in the DMS")
		conn.close()
		sys.exit(8)

	# check if this is a new attempt to ingest a previously ARCHIVED product
	# ----------------------------------------------------------------------------------
	print ('PRODUCT_STATUS : '+PRODUCT_STATUS)
	if (PRODUCT_STATUS!="NEW"):		
		report_error("direct","error","check_product_status",PRODUCT_STATUS,"product with id:"+str(PRODUCT_ID)+"has already been archived")
		conn.close()
		sys.exit(9)

	# update the product status to ACTIVE
	# ----------------------------------------------------------------------------------
	try:
		cursor.execute("UPDATE eodas.product_status SET status = 'ACTIVE' WHERE product_id = '%s'" % PRODUCT_ID)
		conn.commit()
	except psycopg2.Error:
		report_error("direct","warning","product_status",str(PRODUCT_ID),"the update of product status to : ACTIVE has failed")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during update of product_status to ACTIVE)")
		conn.close()
		sys.exit(10)
	
	# retrieve the ingestion parameters 
	# ----------------------------------------------------------------------------------
	try:	
		cursor.execute("SELECT name, product_type FROM internal.product WHERE product.id = '%s'" % PRODUCT_ID)
		data=cursor.fetchone()	
		PRODUCT_NAME='/'+data[0]
		PRODUCT_TYPE=data[1]
		print ('PRODUCT_NAME: '+str(PRODUCT_NAME))
		print ('PRODUCT_TYPE: '+str(PRODUCT_TYPE))
	except psycopg2.Error:
		report_error("direct","error","product",str(PRODUCT_ID),"failed with select name and product_type")
		sys.exit(25)
	
	try:
		cursor.execute("SELECT initial_path FROM eodas.product_status WHERE product_status.product_id='%s'" % PRODUCT_ID)
		PRODUCT_INITIAL_PATH=cursor.fetchone()[0]
		print ('PRODUCT_INITIAL_PATH: '+str(PRODUCT_INITIAL_PATH))
		cursor.execute("SELECT duplicate_from FROM eodas.product_x_flag WHERE product_id='%s' and flag_type_id =2 " % PRODUCT_ID)
		duplicate=cursor.fetchone()
		print duplicate
		if not (str(duplicate) =='(None,)' or duplicate == None):
			print ('the product is a duplicate from : '+str(duplicate[0]))
			PRODUCT_NAME=PRODUCT_NAME[0:len(PRODUCT_NAME)-2]  # in case the product is a duplicate, we remove the _X at the end of its name to retrieve it on the media
	except psycopg2.Error:
		report_error("direct","error","product_status",str(PRODUCT_ID),"failed with select initial_path")
		sys.exit(25)

	# copy the file to the processing directory
	# ----------------------------------------------------------------------------------
	STATUS=rsync_file(PRODUCT_NAME, PRODUCT_INITIAL_PATH, PROCESSING_DIR+'/'+tmp)
	if not ( STATUS == 0 ):
		report_error("direct","error","rsync_file",str(STATUS),"error during rsync between media and processing directory")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during copy to primary archive)")
		conn.close()
		sys.exit(24)
	
	cursor.execute("UPDATE eodas.product_status SET comment='product has been copied in processing_dir' WHERE product_id ='%s'" % PRODUCT_ID)
	conn.commit()
	

	print ('-------------------------------------------------------------')
	print ('generation of info.txt + checksum.txt')
	# creation of and checksum.txt info.txt which contains respectivly checksum and the initial path of the product 
	# ----------------------------------------------------------------------------------	 	
	checksum=open(PROCESSING_DIR+'/'+tmp+'/checksum.txt','w')
	ESA_MD5=str(getmd5(PROCESSING_DIR+'/'+tmp+PRODUCT_NAME))
	checksum.write(ESA_MD5+'  '+PRODUCT_NAME+'\n')
	checksum.close()
	info=open(PROCESSING_DIR+'/'+tmp+'/info.txt','w')	
	info.write(PRODUCT_INITIAL_PATH)
	info.close()

	try:
		cursor.execute("SELECT status FROM eodas.product_status WHERE product_id = '%s' ORDER BY product_id DESC LIMIT 1" % PRODUCT_ID)
		data=cursor.fetchone()
		PRODUCT_STATUS=data[0]
	except psycopg2.Error:
		report_error("direct","error","product_status",str(PRODUCT_ID),"failed to select the status of product in the DMS")
	
	if not (PRODUCT_STATUS=='ACTIVE'):
		report_error("direct","error","product_status",str(PRODUCT_ID),"wrong product status in the DMS")
		sys.exit(29)

	
	# ----------------------------------------------------------------------------------
	# product is valid
	# ----------------------------------------------------------------------------------
	
	# create/update the product status
	# ----------------------------------------------------------------------------------
	cursor.execute("UPDATE eodas.product_status SET status='IN_TRANSFER' WHERE product_status.product_id='%s'" % PRODUCT_ID) 
	conn.commit()	
	
	# Retrieve dataset_name from the database
	# ----------------------------------------------------------------------------------
	try:
		cursor.execute("SELECT dataset_id FROM internal.dataset_x_product WHERE dataset_x_product.product_id = '%s'" % PRODUCT_ID)
		data=cursor.fetchone()
		DTS_ID=data[0]
	except psycopg2.Error:
		report_error("direct","error","dataset_x_product",str(PRODUCT_ID),"dataset id not found")
		sys.exit(29)
	try:
		cursor.execute("SELECT name FROM internal.dataset WHERE dataset.id = '%s'" % DTS_ID)
		data=cursor.fetchone()
		DTS_NAME=data[0]
	except psycopg2.Error:
		report_error("direct","error","dataset",str(PRODUCT_ID),"dataset name not found")
		sys.exit(29)
	
	# In case the product is a duplicate, we retreive its name in the database to build the eodas container
	# ----------------------------------------------------------------------------------
	if not (duplicate == None):	
		try:	
			cursor.execute("SELECT name FROM internal.product WHERE product.id = '%s'" % PRODUCT_ID)
			temp=cursor.fetchone()[0]	
		except psycopg2.Error:
			report_error("direct","error","product",str(PRODUCT_ID),"failed with select name for creation of zip")
			sys.exit(25)
		PRODUCT_ZIPNAME='/'+temp+'.zip'
	else:
		PRODUCT_ZIPNAME=PRODUCT_NAME+'.zip'
	PRODUCT_NAME=PRODUCT_NAME.split('/')[-1]
	print ('PRODUCT_ZIPNAME : ' +PRODUCT_ZIPNAME)

	
	# check integrity of received container
	# -- TODO more general extractor?--------------------------------------------------------------------------------
	#generate the directory testzipdir2 first
	#os.system("mkdir {}".format(PROCESSING_DIR+'/testzipdir2') )
	STATUS=os.system("tar xvf %s -C %s " % (PROCESSING_DIR+'/'+tmp+'/'+PRODUCT_NAME, PROCESSING_DIR+'/testzipdir2'))#extract to directory
	if not ( STATUS == 0 ):
		report_error("direct","error","untar_file",str(STATUS),"error during untar received product in processing directory")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during unzip received product in processing directory)")
		conn.close()
		sys.exit(15)
		
	# Extraction of metadata 
	# ------  ----------------------------------------------------------------------------
	#--- TODO call the method you built to extract the metadata in a dictionnary
	# read the metadata file found in PROCESSING_DIR+'/testzipdir2'+'/*/*.metadata'
	print 'before metadata--'
	
	for pattern in PATTERN:
		file_metadata_list=glob.glob(PROCESSING_DIR+'/testzipdir2/'+pattern)
		if file_metadata_list:
			break
	print file_metadata_list
	if not file_metadata_list:
		report_error("direct","error","finding_metadata",str(PROCESSING_DIR+'/testzipdir2/'+pattern),"error during looking for metadata")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during extracting metadata in processing directory)")
		conn.close()
		sys.exit(15)
	print 'before metadata'
		
	#path_to_meta_product=PROCESSING_DIR+'/testzipdir2/'+PRODUCT_NAME.replace('.tar','/')+PRODUCT_NAME+'_L0.....')
	try:
		
		t=parserjw.extractMetada(file_metadata_list[0],FORMAT)#initial class with file 
        
		#meta,err=t.xmlmetadata(file_metadata_list[0])
		meta,err=t.getraw()
		mydic,error =t.injectionDic(meta, cursor)
	
		PRD_PLATFORM_NAME			= mydic['platform_name']
		PRD_PLATFORM_CODE			= mydic['platform_code']
		PRD_PLATFORM_SHORTNAME		= mydic['platform_short_name']
		PRD_PLATFORM_ID				= mydic['platform_platform_id']
		
		PRD_SENSOR_NAME 			= mydic['sensor_name']
		PRD_SENSOR_MODE	 			= mydic['sensor_operational_mode']
		PRD_SENSOR_TYPE 			= mydic['sensor_type']
		PRD_SENSOR_RESOLUTION 		= mydic['sensor_resolution']
		PRD_RESOLUTION_UNIT 		= mydic['sensor_resolution_unit']
		
		PRD_CENTER_CODE 			= mydic['center_code']
		PRD_CENTER_NAME 			= mydic['center_name']
		PRD_CENTER_CODE_IN_PRD_NAME = mydic['center_id']

		PRD_PROCESSING 				= str(mydic['processing']).replace("'",'"')
		PRD_GENERATION_DATE 		= mydic['product_creation_date']

		PRD_ACQUISITION_START_TIME 	= mydic['start_date_time']
		PRD_ACQUISITION_STOP_TIME  	= mydic['stop_date_time']

		PRD_MISSION_PHASE 			= mydic['mission_phase']
		PRD_IDENTIFIER 				= mydic['product_identifier']
		PRD_SWATH 					= mydic['swath_identifier']
		PRD_FILE_CLASS 				= mydic['file_class']

		PRD_VERSION 				= mydic['product_version']
		PRD_SUBDIR 					= mydic['product_subdir']

		PRD_ORBIT 					= mydic['orbit_number']
		PRD_ORBIT_DIRECTION 		= mydic['orbit_direction']
		
		PRD_FOOTPRINT 				= str(mydic['footprint']).replace("'",'"')
		PRD_INFO     				= str(meta).replace("'",'"')
		PRD_QUALITY					= str(mydic['product_quality']).replace("'",'"')
		
		PRD_TRACK_MIN 				= mydic['track_min']
		PRD_TRACK_MAX 				= mydic['track_max']
		PRD_FRAME_MIN 				= mydic['frame_min']
		PRD_FRAME_MAX 				= mydic['frame_max']

	
		PRD_YEAR 					= mydic['year']
		PRD_MONTH 					= mydic['month']
		PRD_DAY 					= mydic['day']

		PRD_TYPE					= 'NULL'
		
		# Retreive the ESA_MEDIA using the product_path
		# ----------------------------------------------------------------------------------
		DAS_LABEL=PRODUCT_INITIAL_PATH.split('/')[2]  # DAS_LABEL = 'DAS_XXXX'
		cursor.execute("select id from internal.media where comment = '%s'" % DAS_LABEL)
		ESA_MEDIA = cursor.fetchone()[0]
		
		#ESA_MEDIA=999
		
		print PRD_PLATFORM_NAME
		print PRD_PLATFORM_CODE
		print PRD_PLATFORM_SHORTNAME
		print PRD_PLATFORM_ID
		
		print PRD_SENSOR_NAME
		print PRD_SENSOR_MODE
		
		print PRD_CENTER_CODE
		print PRD_CENTER_NAME
		print PRD_CENTER_CODE_IN_PRD_NAME
		print PRD_PROCESSING
		print PRD_GENERATION_DATE
		
		print PRD_ACQUISITION_START_TIME
		print PRD_ACQUISITION_STOP_TIME
		
		print PRD_MISSION_PHASE
		
		print PRD_ORBIT
		#print PRD_FOOTPRINT
		#print PRD_INFO
		
		print PRD_QUALITY
		
		print PRD_TRACK_MIN
		print PRD_TRACK_MAX
		
		print PRD_FRAME_MIN
		print PRD_FRAME_MAX
		
		print ESA_MEDIA
		
		print PRD_IDENTIFIER	
		print PRD_TYPE
		
		print PRD_YEAR
		print PRD_MONTH
		print PRD_DAY

	except : 
		report_error("direct","error","extractMetadata","ERROR PARSING METADATA","error during metadata extraction in processing directory")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during metadata extraction in processing directory)")
		conn.close()
		rm_dir(PROCESSING_DIR)
		sys.exit(16)
		
	# cleaning of the processing_dir
	# ----------------------------------------------------------------------------------
	rm_dir(PROCESSING_DIR+'/testzipdir2')	
	
	# Creation of path for product archiving
	# ----------------------------------------------------------------------------------
	PRD_DIR='/'+DTS_NAME+'/'+PRD_YEAR+'/'+PRD_MONTH+'/'+PRD_DAY

	print ('-------------------------------------------------------------')	
	print ('Metadata have been extracted from product')

	# creation of a .zip for product in /PROCESSING_DIR
	# ----------------------------------------------------------------------------------
	myzip = zipfile.ZipFile(PROCESSING_DIR+PRODUCT_ZIPNAME, "a", compression=zipfile.ZIP_STORED,allowZip64="True")
	for files in os.listdir(PROCESSING_DIR+'/'+tmp):
		myzip.write(PROCESSING_DIR+'/'+tmp+'/'+files, files)
	myzip.close()

	# check the zip product file integrity
	# ----------------------------------------------------------------------------------
	STATUS=check_zip_file(PROCESSING_DIR+PRODUCT_ZIPNAME)
	
	if not (STATUS==None):
		# remove the downloaded file
		rm_file(PROCESSING_DIR+PRODUCT_ZIPNAME)
		report_error("direct","error","check_zip_file",str(STATUS),"corrupted zip file on node")
		process_error(PRODUCT_ID, "FAILED", "Corrupted ZIP file")
		sys.exit(22)
	
	# check the zip product file content integrity
	# ----------------------------------------------------------------------------------
	STATUS=check_zip_content2(PROCESSING_DIR+PRODUCT_ZIPNAME)
	if not (STATUS==0):
		# remove the downloaded file
		rm_file(PROCESSING_DIR+PRODUCT_ZIPNAME)
		report_error("direct","error","check_zip_file",str(STATUS),"corrupted zip file on node")
		process_error(PRODUCT_ID, "FAILED", "Corrupted ZIP file")
		sys.exit(22)

	print ('-------------------------------------------------------------')
	print ('The product.zip in PROCESSING_DIRECTORY has been checked and is OK')
	

	# get the size of EODAS container created
	# ----------------------------------------------------------------------------------
	PRODUCT_SIZE=os.path.getsize(PROCESSING_DIR+PRODUCT_ZIPNAME)
	print ('PRODUCT_SIZE : '+str(PRODUCT_SIZE))


	# generation of checksum for EODAS container created
	# ----------------------------------------------------------------------------------
	if os.access(PROCESSING_DIR+PRODUCT_ZIPNAME, os.F_OK):
		print ('-------------------------------------------------------------')
		print ('Generation of MD5 for prod.zip')
		PRD_MD5=getmd5(PROCESSING_DIR+PRODUCT_ZIPNAME)
	else:
		report_error("direct","error","generation of MD5",PROCESSING_DIR+PRODUCT_ZIPNAME,"file not found")
		sys.exit(25)


	# build primary target directory from filename information
	# ----------------------------------------------------------------------------------
	
	# /!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\
	# CALL ME WHEN YOU'RE HERE because this are the real path, but for your test, we will change it
	# /!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\/!\
	PRIMARY_DIR='/mount/eodas/data'+PRD_DIR
	SECONDARY_DIR='/mount/eodas/sophia'+PRD_DIR
	#PRIMARY_DIR='/mount/restoresophia/folder_for_jiali1'+PRD_DIR
	#SECONDARY_DIR='/mount/restoresophia/folder_for_jiali2'+PRD_DIR
	
	print ('-------------------------------------------------------------')
	print('PRIMARY_DIR: '+str(PRIMARY_DIR))
	print('SECONDARY_DIR: '+str(SECONDARY_DIR))

	
	STATUS=create_dir(PRIMARY_DIR)
	if not ((STATUS == 0) or (os.access(PRIMARY_DIR, os.F_OK))):
		report_error("direct","error","create_dir",str(STATUS),"error during directory creation on primary archive directory")
		process_error(PRODUCT_ID, "NEW", "Reset status (mkdir failure on primary archive directory)")
		conn.close()
		sys.exit(23)
	
	print ('-------------------------------------------------------------')
	print ('Path has been created for primary archive')

	STATUS=create_dir(SECONDARY_DIR)
	if not ((STATUS == 0) or (os.access(SECONDARY_DIR, os.F_OK))):
		report_error("direct","error","create_dir",str(STATUS),"error during directory creation on secondary archive directory")
		process_error(PRODUCT_ID, "NEW", "Reset status (mkdir failure on secondary archive directory)")
		conn.close()
		sys.exit(25)
	print ('-------------------------------------------------------------')
	print ('Path has been created for secondary archive')
	

	# copy the file to the primary archive
	# ----------------------------------------------------------------------------------
	STATUS=rsync_file(PRODUCT_ZIPNAME, PROCESSING_DIR, PRIMARY_DIR)
	if not ( STATUS == 0 ):
		report_error("direct","error","rsync_file","$STATUS","error during rsync between node and primary media")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during copy to primary archive)")
		conn.close()
		sys.exit(24)
	print ('-------------------------------------------------------------')
	print ('Product: '+str(PRODUCT_ID)+' has been copying to the primary archive')

	# check the copy integrity
	# ----------------------------------------------------------------------------------
	STATUS=check_zip_file(PRIMARY_DIR+PRODUCT_ZIPNAME)
	if not ( STATUS == None ):
		# remove the copy
		rm_file(PRIMARY_DIR+'/'+PRODUCT_ZIPNAME)
		report_error("direct", "error", "check_zip_file", "$STATUS", "corrupted zip file on primary media")
		process_error(PRODUCT_ID, "NEW", "Reset status (invalid zip file on primary archive)")
		conn.close()
		sys.exit(22)
	
	print ('-------------------------------------------------------------')
	print ('The product.zip in PRIMARY_ARCHIVE has been checked and is OK')
	
	# copy the file to the secondary archive
	# ----------------------------------------------------------------------------------
	STATUS=rsync_file(PRODUCT_ZIPNAME, PROCESSING_DIR, SECONDARY_DIR)
	if not ( STATUS == 0 ):
		report_error("direct","error","rsync_file","$STATUS","error during rsync between node and secondary media")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during copy to secondary archive)")
		conn.close()
		sys.exit(24)
	print
	print ('-------------------------------------------------------------')
	print ('Product: '+str(PRODUCT_ID)+' has been copying to the secondary archive')

	# check the copy integrity
	# ----------------------------------------------------------------------------------
	STATUS=check_zip_file(SECONDARY_DIR+PRODUCT_ZIPNAME)
	if not ( STATUS == None ):
		# remove the copy
		rm_file(PRIMARY_DIR+'/'+PRODUCT_ZIPNAME)
		report_error("direct", "error", "check_zip_file", "$STATUS", "corrupted zip file on secondary media")
		process_error(PRODUCT_ID, "NEW", "Reset status (invalid zip file on secondary archive)")
		conn.close()
		sys.exit(22)
	
	print
	print ('-------------------------------------------------------------')
	print ('The product.zip in SECONDARY_ARCHIVE has been checked and is OK')
	
	
	PRODUCT_STATUS='DISK'
	
	
	# end of the process... update the database
	# ----------------------------------------------------------------------------------

	# Insertion of acquisition station in the DMS
	# ----------------------------------------------------------------------------------	
	
	try:
		# WE CHECK IF  THE ACQUISITION STATION IS ALREADY KNOWN :
		REQ="SELECT id FROM internal.center WHERE code = '%s' AND name = '%s' AND code_in_product_name = '%s'" % (PRD_CENTER_CODE, PRD_CENTER_NAME, PRD_CENTER_CODE_IN_PRD_NAME)
		print REQ
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')
		cursor.execute(REQUEST)
		test=cursor.rowcount
	except psycopg2.Error as e:
		print e
		report_error("direct","error","center",str(PRODUCT_ID),"failed to log to database for counting center")
		sys.exit(29)		
	if ( test == 0 ): # THE ACQUISITION STATION IS NOT KNOWN, THE ID IN THIS TABLE IN NOT A SERIAL SO WE HAVE TO FIND THE NEXT ID ....
		i=0
		while (test != None):
			try:
				cursor.execute("SELECT id FROM internal.center WHERE center.id='%s'" % i)
				test=cursor.fetchone()
				i=i+1
			except psycopg2.Error as e:
				print e
				report_error("direct","error","center",str(PRODUCT_ID),"failed to log to database for selecting id of center")
				sys.exit(29)
		try:
			#we have find an available id, we add the acquisition station
			REQ = "INSERT INTO internal.center (id, code, name, code_in_product_name) VALUES ('%s', '%s', '%s', '%s')" % ((i-1), PRD_CENTER_CODE, PRD_CENTER_NAME, PRD_CENTER_CODE_IN_PRD_NAME)
			REQUEST = REQ.replace("'NULL'","NULL").replace("''","'")
			print REQUEST
			cursor.execute(REQUEST)
			conn.commit()
		except psycopg2.Error as e:
			print e
			report_error("direct","error","center",str(PRODUCT_ID),"failed to log to database for inserting center")
			sys.exit(29)
		print ('-------------------------------------------------------------')
		print ('Acquisition station has been created for this type of product')
	
	try:
		# Finally we ask for the acquisition station id 
		REQ="SELECT id FROM internal.center WHERE code = '%s' AND name = '%s' AND code_in_product_name = '%s'" % (PRD_CENTER_CODE, PRD_CENTER_NAME, PRD_CENTER_CODE_IN_PRD_NAME)
		print REQ
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')
		cursor.execute(REQUEST)
		stationId=cursor.fetchone()[0]
	except psycopg2.Error as e:
		print e
		report_error("direct","error","center",str(PRODUCT_ID),"failed to log to database for selecting id of center")
		sys.exit(29)


	# Insertion of sensor in the DMS
	# ----------------------------------------------------------------------------------
	try:
		# WE CHECK IF THE SENSOR IS ALREADY KNOWN :
		REQ="SELECT id FROM eodas.sensor WHERE name = '%s' AND operational_mode = '%s'  AND type = '%s' AND resolution = '%s' AND resolution_unit = '%s'" % (PRD_SENSOR_NAME, PRD_SENSOR_MODE, PRD_SENSOR_TYPE, PRD_SENSOR_RESOLUTION, PRD_RESOLUTION_UNIT)
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')
		cursor.execute(REQUEST)
		test=cursor.rowcount
	except psycopg2.Error:
		report_error("direct","error","sensor",str(PRODUCT_ID),"failed to log to database for selecting id of sensor")
		sys.exit(29)

	if ( test == 0 ):
		try:
			REQ="INSERT INTO eodas.sensor (name, operational_mode, type, resolution, resolution_unit) VALUES ('%s', '%s', '%s', '%s', '%s')" % (PRD_SENSOR_NAME, PRD_SENSOR_MODE, PRD_SENSOR_TYPE, PRD_SENSOR_RESOLUTION, PRD_RESOLUTION_UNIT)
			REQUEST = REQ.replace("'NULL'","NULL").replace("''","'")
			cursor.execute(REQUEST)
			conn.commit()
			print ('-------------------------------------------------------------')
			print ('Sensor has been created for this type of product')
		except psycopg2.Error:
			report_error("direct","error","sensor",str(PRODUCT_ID),"failed to inserting sensor")
			sys.exit(29)
	try:
		# FINALLY WE RETRIEVE ITS ID
		REQ="SELECT id FROM eodas.sensor WHERE name = '%s' AND operational_mode = '%s'  AND type = '%s' AND resolution = '%s' AND resolution_unit = '%s'" % (PRD_SENSOR_NAME, PRD_SENSOR_MODE, PRD_SENSOR_TYPE, PRD_SENSOR_RESOLUTION, PRD_RESOLUTION_UNIT)
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')
		cursor.execute(REQUEST)	
		sensorId=cursor.fetchone()[0]
	except psycopg2.Error:
		report_error("direct","error","sensor",str(PRODUCT_ID),"failed to log to database for selecting id of sensor")
		sys.exit(29)
	
	"""
	# Insertion of Platform in the DMS
	# ----------------------------------------------------------------------------------
	try:
		# WE CHECK IF THE PLATFORM IS ALREADY KNOWN :
		REQ="SELECT id FROM eodas.platform WHERE name = '%s' AND code = '%s'  AND short_name = '%s' AND platform_id = '%s'" % (PRD_PLATFORM_NAME, PRD_PLATFORM_CODE, PRD_PLATFORM_SHORTNAME, PRD_PLATFORM_ID)
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')
		cursor.execute(REQUEST)
		test=cursor.rowcount
	except psycopg2.Error:
		report_error("direct","error","platform",str(PRODUCT_ID),"failed to log to database for selecting id of platform")
		sys.exit(29)

	if ( test == 0 ):
		try:
			# PLATFORM ISN'T KNOWN, WE ADD IT
			REQ="INSERT INTO eodas.platform (name, code, short_name, platform_id) VALUES ('%s', '%s', '%s', '%s')" % (PRD_PLATFORM_NAME, PRD_PLATFORM_CODE, PRD_PLATFORM_SHORTNAME, PRD_PLATFORM_ID)
			REQUEST = REQ.replace("'NULL'","NULL").replace("''","'")
			cursor.execute(REQUEST)
			conn.commit()
			print ('-------------------------------------------------------------')
			print ('Platform has been created for this type of product')
		except psycopg2.Error:
			report_error("direct","error","platform",str(PRODUCT_ID),"failed to inserting platform")
			sys.exit(29)
	try:
		# FINALLY WE RETRIEVE ITS ID
		REQ="SELECT id FROM eodas.platform WHERE name = '%s' AND code = '%s'  AND short_name = '%s' AND platform_id = '%s'" % (PRD_PLATFORM_NAME, PRD_PLATFORM_CODE, PRD_PLATFORM_SHORTNAME, PRD_PLATFORM_ID)
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'").replace('= NULL','is NULL')		
		cursor.execute(REQUEST)	
		platformId=cursor.fetchone()[0]
	except psycopg2.Error:
		report_error("direct","error","platform",str(PRODUCT_ID),"failed to log to database for selecting id of platform")
		sys.exit(29)	
	"""
	platformId=4511 
	# Insertion of product_info in DMS
	# ----------------------------------------------------------------------------------	
	try:
		REQ="""INSERT INTO eodas.product_info (product_id, 
												platform, 
												info, 
												validity, 
												cdate, 
												sensor, 
												start_date_time, 
												stop_date_time, 
												footprint, 
												processing, 
												orbit_number, 
												orbit_direction, 
												track_min, 
												track_max, 
												frame_min, 
												frame_max, 
												product_identifier, 
												swath_identifier, 
												file_class, 
												md5, 
												acquisition_station_id, 
												mission_phase, 
												product_version, 
												subdir, 
												esa_media, 
												product_quality, 
												esa_md5) 
										VALUES ('%s','%s','%s',NULL,NOW(),'%s','%s','%s',
												'%s',%s,'%s','%s','%s','%s','%s',
												'%s','%s','%s','%s','%s','%s','%s',												
												'%s','%s','%s','%s','%s')"""  %(PRODUCT_ID,platformId,PRD_INFO,sensorId,PRD_ACQUISITION_START_TIME,PRD_ACQUISITION_STOP_TIME,PRD_FOOTPRINT,
																						PRD_PROCESSING,PRD_ORBIT,PRD_ORBIT_DIRECTION,PRD_TRACK_MIN,PRD_TRACK_MAX,PRD_FRAME_MIN,PRD_FRAME_MAX,PRD_IDENTIFIER,
																						PRD_SWATH,PRD_FILE_CLASS,PRD_MD5,stationId,PRD_MISSION_PHASE,PRD_VERSION,PRD_SUBDIR,ESA_MEDIA,
																						PRD_QUALITY,ESA_MD5)
											
		REQUEST = REQ.replace("'NULL'","NULL")
		cursor.execute(REQUEST)
		conn.commit()
		# UPDATE OF internal.product to add the creation_date and the eodas_container size 
		REQ="""UPDATE internal.product 
				SET generation_date_time='%s', 
					size ='%s' 
				WHERE id = '%s' """ % (PRD_GENERATION_DATE,PRODUCT_SIZE,PRODUCT_ID)
		REQUEST = REQ.replace("'NULL'","NULL").replace("''","'")
		cursor.execute(REQUEST)
		conn.commit()	
		print ('-------------------------------------------------------------')
		print ('Metadata + checksum have been registered in eodas.product_info')
		
	except psycopg2.Error as e:
		report_error("direct","error","SQL_REQUEST",str(e),("failed during insertion of metadata in DB for product : "+str(PRODUCT_ID)))
		process_error(PRODUCT_ID, "NEW", "Reset status (failed during insertion of metadata in DB)")
		conn.close()
		sys.exit(29)
		

	# reference the primary copy 
	# ----------------------------------------------------------------------------------
	os.chdir(PRIMARY_DIR)

	# Getting the name of product in case product is a duplicate 
	# ----------------------------------------------------------------------------------
	if not (duplicate == None):
		try:
			cursor.execute("SELECT name FROM internal.product WHERE id = '%s'" % PRODUCT_ID)
			PRODUCT_NAME=cursor.fetchone()[0]
		except:
			report_error("direct","error","product",str(PRODUCT_ID),"failed to select the name of product in the DMS")
			process_error(PRODUCT_ID, "NEW", "Reset status (error during product recording)")
			conn.close()
			sys.exit(30)

	try:
		REQ= "SELECT internal.new_disk_location_create('%s', '%s')" % (PRODUCT_NAME, (PRIMARY_DIR+PRODUCT_ZIPNAME))
		print REQ
		cursor.execute(REQ)
		conn.commit()
		data=cursor.fetchone()
		STATUS=data[0]
	except psycopg2.Error:
		report_error("direct","error","new_disk_location_create",str(PRODUCT_ID),"failed to log to database for selecting new_disk_location_create")
	
	if not (STATUS == None):
		report_error("direct", "error", "new_disk_location_create", str(STATUS), "product has been incorrectly referenced in the DIS (primary archive)")
		process_error(PRODUCT_ID, "NEW", "Reset status (error during product recording)")
		conn.close()
		sys.exit(29)
	
	print ('-------------------------------------------------------------')
	print ('media, media_catalog, media_catalog_entry have been added in DMS')
	
	# go back to the working directory
	# ----------------------------------------------------------------------------------
	os.chdir(PROCESSING_DIR)
	
	# check if the copie is correctly referenced in the DIS
	# ----------------------------------------------------------------------------------
	try:
		cursor.execute("SELECT count(*) FROM internal.product AS p, internal.media_catalog_entry AS mce, internal.product_x_media_catalog_entry as pmce WHERE p.id = pmce.product AND pmce.media_catalog_entry = mce.id AND p.name='%s'" % PRODUCT_NAME)
		COUNT=cursor.fetchone()[0]
	except psycopg2.Error:
		report_error("direct","error","product, media_catalog_entry, product_x_media_catalog_entry",str(PRODUCT_ID),"failed to log to database for counting product ")
		sys.exit(29)

	if ( COUNT == 1 ):
		print ('-------------------------------------------------------------')
		print ('Product (which id: '+str(PRODUCT_ID)+') is ref in the DMS! Well Job')
	
	if (COUNT < 1):
		report_error("direct", "error", "check_nb_of_references", str(STATUS), "too few product references (nb="+str(COUNT)+"<1)")
		process_error(PRODUCT_ID, "NEW", "Reset status (product referenced "+str(COUNT)+" times)")
		conn.close()
		sys.exit(25)
	elif (COUNT > 1):	
		report_error("direct", "error", "check_nb_of_references", str(STATUS), "too many product references (nb="+str(COUNT)+">2")
		process_error(PRODUCT_ID, "NEW", "Reset status (product referenced "+str(COUNT)+" times)")
		sys.exit(25)

	# create/update the product status
	# ----------------------------------------------------------------------------------
	try:
		cursor.execute("UPDATE eodas.product_status SET status='DISK', comment='Direct ingestion process completed', cdate=NOW() WHERE product_id = '%s'" % PRODUCT_ID)
		conn.commit()
	except psycopg2.Error:
		report_error("direct","error","product_status",str(PRODUCT_ID),"failed to log to database for updating product status")
		sys.exit(29)
	
	print ('-------------------------------------------------------------')
	print ('Product status has been updated: DISK')
	
	# end of test on product validity closing connexion to DMS
	# ----------------------------------------------------------------------------------
	conn.close()

	# cleaning of the processing_dir
	# ----------------------------------------------------------------------------------
	rm_dir(PROCESSING_DIR)
	
	# bye-bye script...
	# ----------------------------------------------------------------------------------
	return ERROR
	
if __name__ == "__main__":
    	main()
