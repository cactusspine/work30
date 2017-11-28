# learning dairy 

### Jiali Wang

### Starting date: 11/22/2017

***

#####learning of sed
```bash
sed  -e 'here is expression to apply' $filename
-n: only print the 
```
***
####bash history expansion
#####11/24/2017
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
***
####scrapy
#####11/24/2017

#####learning of sed
```bash
sed  -e 'here is expression to apply' $filename
-n: only print the 
```
