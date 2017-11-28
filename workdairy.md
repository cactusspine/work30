# Working dairy 

### Jiali Wang

### Starting date: 11/22/2017

===

####insert esa delivery excel into the database
This is the first step of whole injesting. 
	```bash
	cd /exports/eodas/scripts/specific-batch/eodas
	./esa_delivery_excel_to_eodas_db_v2.sh 	/exports/eodas/data/das/delivery/DAS_Delivery_13_14_unref.xlsx
	
	``` 

use the

*** 
#### insert checked product list into the database
- create the list make sure all the named listing are saved at `cd /exports/eodas/data/das/listing/dataset` .
- find the listing with full path using `find `pwd ` -mtime 0`
	before lauching check the number of file by `wc -l /exports/eodas/data/das/listing/dataset/ds.DL269a7.das.delivery_13_14_part_1_2.lst` and check head.
- make sure to go into the 40 machine first before insertion sothat the insertion is safe . make sure even if quit the work will be continued.
	
	```bash 
	cdp #go to the path :/exports/eodas/scripts/specific-package/eodas
	./product_create.sh -h #display help info		 
	./product_create.sh --version 1.0.0 --listing /exports/eodas/data/das/listing/dataset/ds.DL269a7.das.delivery_13_14_part_1_2.l
	```	
	
put the full path of the product into the `cd /exports/eodas/data/das/listing/dataset` folder, and use product_create to insert the product.
* need to check the version of product
* if the product has been flagged to need rivision by the ESA
* this code seems only handle the ZIP insertion.
  
 afterwards use the following query to __check if the product has been inserted__.

	```sql  
  	SELECT count(p.id), d.name
		FROM internal.product AS p, 
	     eodas.product_status AS ps,
	     internal.dataset_x_product as dxp,
	     internal.dataset as d
		WHERE p.id=ps.product_id
	      and p.id=dxp.product_id
	      and d.id=dxp.dataset_id
	      and d.name IN ('DL268k')
	      AND ps.status NOT IN ('DISK','SENT','TAPE','ARCHIVED')
	 ---GROUP By d.name
	 ORDER by d.name;
	```	 

 before lauching ,check IF PRODUCT INSERTED `SELECT id FROM internal.product where name ='1385245.TAR_W099202700101.tar'`


	```sql	 
	SELECT delivery_id,medium_id,media_id
	FROM eodas.delivery_from_esa 
	WHERE media_id IN (
		---1292,1297,1304,1305,1303,1208,1298,
		1291)
	```
manually setting the value of the swquence after your last insert with provided values:`SELECT setval('test_id_seq', (SELECT MAX(id) from "test"));`
	```bash
	find . -size +1M |cat >>.gitignore
	find $filedir -type f -ls |awk '{print $11}' > ` echo $filedir|sed 's/\/mnt\///g'`.all1
	find . -name '*.lst'|xargs wc -l
	find . -type f -size 0 -delete
	cd .;for file in $(\ls *.lst); do newname=`\echo $file|\sed 's/Part/part/g'`; mv $file $newname; done
	for file in $(ls *.zip); do unzip $file;done
	for file in $(\ls *.lst); do newname=`\echo $file|\sed 's/Part/part/g'`; mv $file $newname; done
	for pattern in `cat toto`; do grep $pattern md.DAS_1078.das.delivery_14.lst| grep ZIP|wc -l; done	 
	  
	sed -i '1s/^/task goes here\n/' todo.txt
	echo 'task goes here' | cat - todo.txt > temp && mv temp todo.txt
	```
***
  
####11/23/2017
***
	```sql
	SELECT * FROM eodas.media_info
	WHERE original_name in ('GTDAS-053','GTDAS-012')
	```
***
####
***
