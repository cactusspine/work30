#!/bin/bash

# -------------------------------------------------------------------
#
# This script perform a direct ingestion of products from the DAS server
#
# History:
#
# 2016-07-21 :  gbr : initial version
#
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Error codes
#  1 : syntax error
#  2 : LTA_HOME is not set
#  3 : batch does not exist
# 11 : primary archive disk not found
# 12 : secondary archive disk not found
# 21 : error during file copy
# 22 : error during zip file integrity check
# 23 : error during primary target directory creation
# 24 : error during product copy to primary directory
# 25 : product is not correctly referenced in the DIS
# 26 : error during add product_x_file in the database
# 27 : invalid product found in the LTA drop box
# 28 : product already archived
# 29 : product referencing failure
# 30 : failed to connect to database
# -------------------------------------------------------------------

# check if working environment variable is set
if [ -z ${LTA_HOME} ]; then
  log_error "LTA_HOME is not defined"
  exit 2 
fi

source ${LTA_HOME}/definitions.include
#take parameters for the python code
# $1 is the product_id, $2 is the type of metadata(xml, n1, asc, cdf) $3 is the patern to search for the metadata in product like '/*/*.metadata'
#/mount/restoresophia/sources/software/bin/python /exports/eodas/dev/jwang/ingestion_generic.py $1 $2 $3
/exports/eodas/software/python/bin/python /exports/eodas/scripts/scripts/eodas_python_lib/ingestion/ingestion_generic.py $1

