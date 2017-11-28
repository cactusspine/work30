#!/bin/bash
#########################################################################
#           Script Name : create_insert_listing.sh					    #
#           Author Name : Jiali Wang								    #
#           Email       : jialimwang@gmail.com						    #
#           Date	    : 11/24/2017								    #
#           Script usage: create listing of files for insertion			#
#########################################################################
set -e
set -x
#need a parameter indicating the file directory for the products
#need a input file ,has four column datasetid partid pattern number
#before lauching the file 
#============================================================
#display help info
#============================================================
if [ "$1" == "-h" ] ; then
    echo -e "Usage: ./try.sh searchdirectory  search_pattern_file"
    echo -e ""
    exit 0
fi
#============================================================
#readin the directory ,generate full file list
#============================================================
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

filedir='/mnt/DAS_1078'

if [ -d $1  ]; then 
	filedir=$1	
else
	echo "syntax $d is not a directory to search for products!"
	exit 1
fi

allfilelist=`echo $filedir|sed 's/\/mnt\///g'`
echo "save the listing of $filedir in ${allfilelist}.lst"

find $filedir -type f -ls |awk '{print $11}' > ${allfilelist}.all

#============================================================
#readin the input file and grep pattern
#============================================================
#: <<DOCMMENTATIONXX

input='input.txt'
while IFS=' ' read -r datasetid partid pattern number deliveryid
do

      #find /mnt/DAS_1078 -type f -ls |grep "${pattern}" |awk '{print $11}'> ds.${datasetid}_${partid}.das.delivery_14.lst
      cat ${allfilelist}|grep "${pattern}" > ds.${datasetid}_${partid}.das.${deliveryid}.lst
      real=`cat ./DAS_1078.all|grep "${pattern}" |wc -l`
      if [[ $real -eq $number ]] 
      then echo "precount number and ds.${datasetid}_${partid}.das.${deliveryid}.lst are the same "
      else echo 'precount number and ds.${datasetid}_${partid}.das.${deliveryid}.lst are different'	
      fi
  done < "$input"

#DOCMMENTATIONXX