# Working dairy 

### Jiali Wang

### Starting date: 11/22/2017

***
#### insert checked product list into the database
-make sure to go into the 40 machine first before insertion sothat the insertion is safe .
	go through the machine usuially working the small remote is 
	```bash 
		cdp
		./product_create.sh --listing /exports/eodas/data/das/listing/dataset/ds.DL268q_part_5_zip.das.delivery_14.lst --version 1.0.0
		./product_create.sh --listing /exports/eodas/data/das/listing/dataset/ds.DL268u_part_5_zip.das.delivery_14.lst --version 1.0.0
		./product_create.sh --listing /exports/eodas/data/das/listing/dataset/ds.DL268g_part_5_6_zip.das.delivery_14_15.lst --version 1.0.0
	```	
put the full path of the product into the `/exports/eodas/data/das/listing/dataset` folder, and use product_create to insert the product.
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
#####learning of sed
```bash
sed  -e 'here is expression to apply' $filename
-n: only print the 
```
####bash history expansion
important cocept to accelerate history search
	```bash
	!!#excute last command
	#^original^replacement^ can revise the last command and excute
	!:1 !:0 can access the parameter list of last command
	!!:$ # last paramemter
	!!:^ #first augument
	!!:* #all the argument except the 0
	!!:0 #the initaial command 
	!-3 #the last but third command
	!ssh #execute the last command starting ssh
	!?xargs? #excute the last command containing xargs
	#after the history expansion we can add modifier to change the behavior of recall
	:p #instead of excute, print the command instead
	cat /usr/share/doc/manpages/copyright
	cd !!:$:h #will change dir to the copyright directory
	#:h #chopping of the filename at the end
	!cat:$:t  #get the tail of the last augument ,which is file name
	!!:$:r # strip of the trailing exttension which is the end
	#modifier can be appended one after another
	touch file1 file2 file3
	mkdir !!:*:gs/file/dir/#gs///modifier excute the replacement over all agument
	```
