#header to build, author date and version

import os
import sys
import re
from lxml import etree as ET
from netCDF4 import Dataset
import json
from db_utils import connect as db_connect
from db_utils import submit_query as submit_query
import datetime
import calendar




class extractMetada(object):

    
    base_str = '''
        {0}
        {1:^80}
        {0}
        '''
    errors = {
                # error related to arg parsing
                100 : 'file does not exist',
                200: 'the stop line of metadata does not exist',
            
                # error related to database
                400: 'can not connect to DB',
                202: 'no retrieval from DB',
         }
    fields =['validity','start_date_time','stop_date_time','footprint',
             'processing','orbit_number','orbit_direction','track_min',
             'track_max','frame_min','frame_max','product_identifier',
             'swath_identifier','file_class','mission_phase','product_version',
             'product_quality','product_creation_date','product_type_name',
             'product_subdir',
             'center_id','center_name','center_code','platform_platform_id',
             'platform_short_name','platform_name','platform_code','sensor_type',
             'sensor_operational_mode','sensor_resolution','sensor_name',
             'sensor_resolution_unit','year','month','day']
    field_noInfo=['year','month','day']
    
    month_abbr2nr={v.upper():k for k,v in enumerate(calendar.month_abbr)}

    def error_def(code, arg=None):
        #--- aaa
        if arg is None:
            arg=''
        if code in errors.keys():
            return {'code':code, 'msg': errors[code].format(arg)}
        else:
            return {'code':9999, 'msg': 'Unknown error'}    
 
    def filter_not_needed(self,mylist):
        '''
        filter_not_needed(list)->list_shorted
        this function is to shorten the string of keys in the extracted Mettadata from XML file 
        this function is called by extractMetadata(MD.xml file)
        :param list:a list of extrated key string parts for each property 
        '''
        myfilter=['earthobservation','earthobservationequipment','procedure',
                'metadataproperty','result','earthobservationresult',
                'earthobservationmetadata','processing',
                'acquisitionparameters','featureofinterest',
                'phenomenontime','product','l0-dls-metadata']
        return [x for x in mylist if not x in myfilter]       
    def remove_duplicate(self,list):
        '''
        remove the duplicates in one list and keep its order
        :param list: a list object
        #solution from https://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-whilst-preserving-order
        '''
        seen = set()
        seen_add = seen.add
        return [x for x in list if not (x in seen or seen_add(x))]    
    def eosip(self, content_file):
        '''
        this function resorve the xml file and return the extracted metadata in dictionary
        extractMetada(xml_file)->dictionary
        :param content_file:a complete path file name towards a EOSIP format xml file of the metadata
        #example return value {'earthobservation_resulttime_timeinstant': '2011-10-16T20:52:46Z'}
        '''
        product=content_file
        if not os.path.isfile(content_file):
            return None,101
        #TODO 
        #check if file exist, if not ,error
        #return None, 101 
        try:
            tree = ET.parse(product)
        #except IOError:
           # print ("can not open filefile {}".format(content_file))
           # return None, 101     
        except ET.ParserError:
            return None, 102 
        except Exception, e:
            return None, 999 
        else:
            print ("the xml file has been successfully read in!")    
            
        root = tree.getroot()
        dic={}#Creation of a dictionnary to keep the values of the key and values
        avoid_string='vendorspecific'
    
        for ele in tree.iter():
            #--- check if element has text, or if text is of None type
            try:
               # print "element name", ele.tag
                #print "text :", ele.text
                len(ele.text)
            except:
                #print "this node is empty"
                continue
            if ele.text.isspace():
                #print "empty string"
                continue
            #all the node which reached here are node with content to extract
           
            val=ele.text
            property=[]
            
            #--- extract the tag of the current and its ancester in reverse order
            for arc in ele.iterancestors():#take the leaves ancesters
                property.insert(0,arc.tag)
                #insert an item at a given position, the first argument is the index of the element before which to insert
                #list can be append, extend(L), inset, remove(by value), pop/popleft, index, count(by value),reverse 
            property.append(ele.tag)#important !! add the tag of the current node 
               
            #--- convert the tags to the key for value, store
            #lower case, link with _, remove same value
            property=[re.sub(r'\{.*?\}',"",p).lower() for p in property]#list comprehensions, concise
            property=self.remove_duplicate(property)
            property=self.filter_not_needed(property)
            property='_'.join(property)
            
            if avoid_string in property:
                continue#if the current node is a node of specific structure
            #print property
            dic[property]=val
            
        #--- insert the specific case of vendorSpecific in dic
        try:
            for node in tree.findall('//eop:vendorSpecific//eop:localValue',namespaces=root.nsmap):
                if node.getprevious() is None:
                    # print "this local value have no attribute:", node.tag, node.text
                    continue
                else:
                    attribute = 'vendorspecific_'+node.getprevious().text.lower()
                    localValue = node.text
                    dic[attribute]=localValue 
        except Exception, e:
            print "can't find eop vendor"             
               
        return dic,0    
    #Creation of a dictionnary to keep the values
 
    def xmlmetadata(self, content_file):
        '''
        this function resorve the xml file and return the metadata in dictionary
        extractMetada(xml_file_metadata)->dictionary
        :param content_file:
        a complete path file name towards a xml file of the metadata,
         which contains multiple snaps and locations
        #example return value {'earthobservation_resulttime_timeinstant': '2011-10-16T20:52:46Z'}
        '''
        product=content_file
        if not os.path.isfile(content_file):
            return None,101
        #TODO 
        #check if file exist, if not ,error
        #return None, 101 
        try:
            tree = ET.parse(product)
        #except IOError:
           # print ("can not open filefile {}".format(content_file))
           # return None, 101     
        except ET.ParserError:
            return None, 102 
        except Exception, e:
            return None, 999 
        else:
            print ("the xml file has been successfully read in!")    
            
        root = tree.getroot()
        dic={}#Creation of a dictionnary to keep the values of info
        #dic_no_rep={}
        avoid_string='vendorspecific'
    
        for ele in tree.iter():#deepth first iteration
            #--- check if element has text, or if text is of None type
            try:
               
                len(ele.text)
            except:
                #print "this node is empty"
                continue
            if ele.text.isspace():
                #print "empty string"
                continue
            #all the node which reached here are node with content to extract
           
            val=ele.text
            property=[]
            
            #--- extract the tag of the current and its ancester in reverse order
            for arc in ele.iterancestors():#take the leaves ancesters
                property.insert(0,arc.tag)
            property.append(ele.tag)#important !! add the tag of the current node 
               
            #--- convert the tags to the key for value, store
            #lower case, link with _, remove same value, romove root
            property=[re.sub(r'\{.*?\}',"",p).lower() for p in property]
            property=self.remove_duplicate(property)
            property=self.filter_not_needed(property)
            property='_'.join(property)
            
            #-- current node is a node of specific structure,avoid
            if avoid_string in property:
                continue
            
            #--handle same property multiple value
#    </temporal-coverage>
#    <g-polygon>
# <polygon-corner><latitude>62.892</latitude>
# <longitude>-25.517</longitude></polygon-corner>
#     <polygon-corner>
#      <latitude>63.006</latitude>
#      <longitude>-24.246</longitude>
#   </g-polygon>
#    <scene-centre>
#     <latitude>62.702</latitude>
#     <longitude>-24.777</longitude>
            #check same level multiple nodes and same property arise again
            same_key_list=[k for k,v in dic.items()if k.startswith(property)]
            #rep_list=[k for k,v in dic.items()if (k.startswith(property) and v!=val)]
            if same_key_list:#this property has been documented before
                property=property+ '_'+str(len(same_key_list))
                
            #add the property and value to final dic directly
            dic[property]=val
            
        #--- insert the specific case of vendorSpecific in dic
        try:
            for node in tree.findall('//eop:vendorSpecific//eop:localValue',namespaces=root.nsmap):
                if node.getprevious() is None:
                    # print "this local value have no attribute:", node.tag, node.text
                    continue
                else:
                    attribute = 'vendorspecific_'+node.getprevious().text.lower()
                    localValue = node.text
                    dic[attribute]=localValue 
        except Exception, e:
            print "sth wrong with the nodes!"             
               
        return dic,0    
    #Creation of a dictionnary to keep the values
    def netcdf(self, nc_file):
        '''
        this function extract metaData from NC/netcdf format data
        extractMetaNc(nc_file)->dictionary
        this function is from Gaston
        need netcdf4 package
        :param a nc format file
        '''
        if not os.path.isfile(nc_file):
            return None,101
        dic={}
        try:     
            myDataset = Dataset(nc_file)#open dataset
        except Exception,e:
            return None,103
        else:
            print "netcdf file loaded!"     
        #print dir(myDataset)
        #myJson='{'
        # GET ALL DIMENSIONS
        dico_dimensions={}
        for cle in myDataset.dimensions.keys():
            dico_dimensions[cle]=myDataset.dimensions[cle].size
            #myJson+='"dimensions_%s" : "%s" , ' % (cle,myDataset.dimensions[cle].size)
            dic['dimentions']=dico_dimensions
        
        # GET ALL VARIABLES
        dico_vbles={}
        #print myDataset.variables
        for cle in myDataset.variables.keys():
            #print "\t\ttype:", repr(myDataset.variables[cle].dtype)
            temp_grp=myDataset.variables[cle]
            temp_dict={}
            #tempJson='{'
            for attr in temp_grp.ncattrs():
                #print '\t\tattr+value:', attr,'=',repr(temp_grp.getncattr(attr)) 
                temp_dict[attr.lower()]=temp_grp.getncattr(attr)
                #tempJson+='"%s" : "%s" , ' %(attr,temp_grp.getncattr(attr))
            #tempJson=tempJson[:-2]+'}'
            dico_vbles[cle]=temp_dict
            #myJson+='"variables_%s" : %s , ' % (cle,tempJson)
            dic['variables']=dico_vbles
       # print " All VARIABLES recorded"  
       
        # GET ALL ATTRIBUTES
        dico_attr={}    
        for attr in myDataset.ncattrs():
            dico_attr[attr.lower()]=getattr(myDataset, attr)
            dic[attr]=getattr(myDataset, attr)
            #myJson+='"attributes_%s" : "%s" , ' % (attr,getattr(myDataset, attr))
            
        # CHANGE the Month from string into integer =>  OCT -> 10 ....
        #dico_month=dict((v.lower(),str(k) if len(str(k))>1 else '0'+str(k)) for k,v in enumerate(calendar.month_abbr))
        #myJson=myJson[:-2].replace("\n",' ')+'}'
        #print myJson
        #dic=json.loads(myJson)
        # print json.dumps(data,indent=4, sort_keys=True)
    
        return dic,0

    def extractMetaAsc(self,asc_file):
        '''
        this fun resorve the asc file and return the extracted metadata as dictionary
        extractMetada(asc_file)->dictionary
        :param asc_file:
        '''
        dic={}
        if not os.path.isfile(asc_file):
                return None,100
        with open(asc_file,'r') as f:
            for line in f:
                if not line.strip():
                    #print "this is empty line, end reading"
                    break
                attribute,val=[p.strip().lower() for p in line.split('|')]
                dic[attribute]=val
                #print "attribute=",attribute,"value=", val
            f.close()           
        # Creation of a xmlTree in order to parse the .MD.XML file
        return dic, 0

    
    def extractMetaN1(self,product):
    
        '''
        This function extract metaData from the N1 file
        TLM_HK__0PNLRC20020407_142604_000060382004_00483_00537_1829.N1
        :param n1_file: meta+binary mix data type
        '''
        if not os.path.isfile(product):
                return None,100
        d={}
        found=False
        with open(product,'r') as f:
            #content = [next(f) for x in xrange(74)]
            for line in f:
                if 'DS_NAME' in line:#reach the end of the metadata
                    found=True
                    break
                elif not line.strip():
                    #print "this is a empty line"
                    continue
                else:#handle this line
                    attri,val = line.split('=')
                    #if val is of string type, unquote
                    val=val.strip().strip('"').strip()
                    #removed the \n# remove the ""#remove again possible space
                    #if val is with unit
                    val=re.sub(r'\<.*?\>',"",val)
                    d[attri]=val
        if not found:
            return None, 200 #no Ending of metadata           
    
        return d,0             


    def checkdic(self,keys_list, cursor, version=1,conn=None):
        '''
        This function take a list of the field and cursor connection as input
        and return the correspoinding keys lists if it exist in the database.
        checkdic(list,cursor,*version,*connection)->list,error_code
        '''
#         #get connection to database
#         try:
#             conn, cursor, err = db_connect(server='172.22.99.61', db='test')
#         except:
#             return None ,400 #error in connection to DB
        #query,check if the fields exist in the database
     
#         query = '''SELECT field 
#                     FROM  eodas.field_x_keys
#                     WHERE '{}' = ANY( keys) AND version = {}'''
#         #select * from table where key_string = ANY(array column name) 
#         for key_string in keys_list:# NOT EFFICIENT!!!
#             #check if key is in the array column keys,return corrispond field
#             submit_query(query.format(key_string,version), cursor, commit=False, conn=conn)
#             #print self.base_str.format('-' * 80, query.format(key_string))
#             if cursor.rowcount==0:
#                 fieldlist.append(None)
#                 continue
#             else:
#                 fieldlist.append(cursor.fetchone()[0])
        #return the list of the keys, and error_code =0
        query = '''SELECT field, keys
                    FROM  eodas.field_x_keys
                    WHERE version = {}'''
        submit_query(query.format(version), cursor, commit=False, conn=conn)
        dic_kf={}
        if cursor.rowcount==0:
            return None,401#empty reply from the DB
        else:
            for row in cursor:
                for kv in row[1]:
                    if kv in dic_kf:
                        #this kv already have an associated field
                        dic_kf[kv].append(row[0])
                    else:
                        dic_kf[kv]= [row[0]]
        
        fieldlist=[dic_kf.get(attri, None) for attri in keys_list]            
        return fieldlist,0

    def injectionDic(self,key_dict, cursor):
        '''
        This function take a dictionary of matadata extracted with original attributes
        and transform it into a dictionary using the standarised fields in DB as the key 
        injectionDic(key_dictionary)->field_dictionary
        :param key_dict: dictionary containing metadata before mapping to DB
        '''
        if key_dict is None:
            return None 
        ori_keys=key_dict.keys()#attris from the metadata may contains number
        
        keys_without_nr= [re.sub('_[0-9]+$','',k) for k in ori_keys]
        #listfield,error =self.checkdic(ori_keys,cursor)
        listfield,error =self.checkdic(keys_without_nr,cursor)
        dic_map=dict(zip(ori_keys,listfield))#mapping originalkey with nr ,list field
        dic={}

        for f in self.fields:
            dic[f]="NULL"

            # max([v for k,v in meta.items() if k.startswith('downlink-metadata_sensor-id')] )
        for ori_key, field_list in dic_map.items():
            if field_list is None:
                #print "can't find corresponding field for {}".format(ori_key)
                continue
            
            for field in field_list:
                #--- field which will pad a  dictionary
                if field =='footprint' or field =='product_quality' or field=='processing':         
                    if dic[field]=="NULL":
                        #print "enter first entry"
                        dic[field]={ori_key : key_dict[ori_key]}
                        #print dic[field], type(dic[field])
                    else:
                        dic[field].update({ori_key:key_dict[ori_key]})
                       
                #--- field need comparison from multiple original keys      
                elif field == 'track_min' or field == 'frame_min':#add this to field dic
                    #print 'enter branck 2'
                    if dic[field]=="NULL":
                        dic[field]=int(key_dict[ori_key] )
                    else:    
                        dic[field]=min (dic[field], int(key_dict[ori_key]))  
                        
                elif field == 'track_max' or field =='frame_max':#add this to field dic
                    #print 'enter branck 3'
                    if dic[field]=="NULL":
                        dic[field]=int(key_dict[ori_key] )
                    else:    
                        dic[field]=max (dic[field], int(key_dict[ori_key]))    
                                     
                else:
                    #print 'enter branck 4'
                    dic[field]=key_dict[ori_key]
        #--- time related field special            
        if  dic['year']=='NULL' or dic['year']=='NULL' or dic['year']=='NULL':
              dictime= self.get_dic_time(dic['start_date_time']) 
              dic['year']= dictime['year']
              dic['month']=dictime['month']
              dic['day']=dictime['day']
        return dic, 0    

    def get_dic_time(self,time_string, format=None):
        '''
        this function take a string of time in different format
        try guess the format and return a dictionary containing date info
        get_dic_time(self,time_string, format=None)->dictionary of year, month and day
        if cannot extract date return no-info 
        :param time_string:a string of the starting date time in various format
        :param format:format abbreviation: RFC, IBM, NORM, JUP
        '''
        dictime={ k : 'no-info' for k in ['year','month','day']}
        #the format of the time is RFC3339 string like "2008-09-03T20:56:35.450686Z"
        patternRFC = re.compile("([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(([Zz])|([\+|\-]([01][0-9]|2[0-3]):[0-5][0-9]))") 
        patternIBM =re.compile("(([0-9])|([0-2][0-9])|([3][0-1]))\-((JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))\-(\d{4})")
        patternNorm =re.compile("(\d{4})\-(0[1-9]|1[012])\-(([0-9])|([0-2][0-9])|([3][0-1]))")
         #jupiter year time 1996-021T07:29:15.907391 ,1996-267T20:16:22;
        patternJupiter =re.compile("(\d{4})\-([0123]\d{2})[Tt](([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60))")
        #--- format of the time is RFC3339 
        if format=='RFC' or re.search(patternRFC,time_string) is not None:
            m=re.search(patternRFC,time_string)
            #d= datetime.datetime.strptime(time_string.upper(),"%Y-%m-%dT%H:%M:%S.%fZ")
            #dictime['year']=d.year
            #dictime['month']=d.month
            #dictime['day']=d.day
            dictime['year']= m.group(1)
            dictime['month']= m.group(2)
            dictime['day']= m.group(3)
        #--- IBM time format 'stop_date_time': u'26-MAR-1996 01:50:45.015'
        elif format=='IBM' or re.search(patternIBM,time_string) is not None:
            m=re.search(patternIBM,time_string)
            print m.group(0)
            string_list=m.group(0).split('-')
            dictime['year']= string_list[2]
            dictime['month']= '{:02d}'.format(self.month_abbr2nr[string_list[1].upper()])
            dictime['day']= string_list[0]
        #--- pattern normal year '2006-02-02 00:04:44+00'    
        elif format=='NORM' or re.search(patternNorm,time_string) is not None:
            m=re.search(patternNorm,time_string)
            print m.group(0)
            string_list=m.group(0).split('-')
            dictime['year']= string_list[0]
            dictime['month']= string_list[1]
            dictime['day']= string_list[2] 
       
        #--- pattern match jupiter year
        elif format=='JUP' or re.search(patternJupiter,time_string) is not None:
            m=re.search(patternJupiter,time_string)
            y=int(m.group(1))
            jd=int(m.group(2))
            dictime=self.JulianDate_to_MMDDYYY(y,jd)       
        return dictime 
       
    def JulianDate_to_MMDDYYY(self,y,jd):
        '''
        from Gaston , convert Julian Date to dictionary with normal date
        /exports/eodas/scripts/specific-batch/eodas
        :param y:interger of the year part 
        :param jd:interger of the Jupiter day
        '''
        month = 1
        day = 0
        d={}

        while jd - calendar.monthrange(y,month)[1] > 0 and month <= 12:
                jd = jd - calendar.monthrange(y,month)[1]
                month = month + 1
        #print month,jd,y
        d['year']=str(y)
        d['month']='{:02d}'.format(month)
        d['day']='{:02d}'.format(jd)
        return d
            
    def initiate_filed_x_key(self):
        return 0
#                 input_db= '/exports/eodas/dev/jwang/parser/key_dic.txt'
#         query='''INSERT INTO eodas.field_x_keys
#          (field, keys, version) VALUES ({}, ARRAY{},1);
#         
#         '''
#         with open(input_db, 'r') as f:
#             lines =f.readlines()
#         for line in lines:
#             fi = line.strip().split(':') 
#             print query.format(fi[0],fi[1])
#             submit_query(query.format(fi[0],fi[1]), cursor, commit=True, conn=conn)
#     #ALTER TABLE public.accounts ALTER COLUMN pwd_history SET DEFAULT array[]::varchar[];

    dic={}
    err=0
 
 
    def __init__(self,file_path=None, input_format='xml'):
        '''
        initiate extractMetadata class by the input file and assign a format
        :param file_path:the full path to the metadata file which need extract
        :param input_format: string,can be xml, asc, n1,cdf
        '''
        if file_path is None:
            print "create empty extractMetadata class  "
            return
        self.file_format=input_format
        if input_format=='xml':
            self.dic,self.err=self.xmlmetadata(file_path)
        elif input_format=='asc':
            self.dic,self.err=self.extractMetaAsc(file_path)
        elif input_format=='n1':
            self.dic,self.err=self.extractMetaN1(file_path)
        elif input_format=='cdf':
            self.dic,self.err=self.netcdf(file_path)
        else:
            print "lacking method to handle {} type of data".format(input_format)           
        
    def getraw(self):
        return self.dic,self.err
 
             
        

def test():        
    #file_name='./cr2/CS_OPER_STR3DAT_0__20111016T190041_20111016T203618_0001.MD.XML'
    #file_name='./er1/SAR_IM__0PWDSI19910730_195717_00000469A000_00014_00197_0000.MD.XML'
    #file_name='ASPS20_H_960326001011.nc'#ncfile example
    #file_name='TLM_HK__0PNLRC20020407_142604_000060382004_00483_00537_1829.N1'
    file_name='./sr13323/sr13323imager.asc' ##strance 
    tmp_path='/exports/eodas/tmp/metadata_extraction_for_jiali/products'
   # tmp_path='/exports/eodas/dev/jwang/tarfile'
    #file_name='./1385245.TAR_W099202700101/1385245_L0-DLS.metadata'
    if not os.path.isfile( os.path.join(tmp_path, file_name)):
        print "{} does not exist, plese provide a valid file ".format(os.path.join(tmp_path, file_name))
        sys.exit() 
    # 
    # #meta=extractMetaAsc(os.path.join(tmp_path, file_name))
    #meta=extractMe(os.path.join(tmp_path, file_name))
     #meta=extractMetaN1(os.path.join(tmp_path, file_name))
    t=extractMetada()
    print t.getraw()
    time_string='1996-021T07:29:15.907391'
    print t.get_dic_time(time_string)
    #meta,err=t.xmlmetadata(os.path.join(tmp_path, file_name))
    meta,err=t.extractMetaAsc(os.path.join(tmp_path, file_name))
    #meta,err=t.netcdf(os.path.join(tmp_path, file_name))
    #meta,err=t.extractMetaN1(os.path.join(tmp_path, file_name))
    print meta
    
    #get connection to database
    try:
        conn, cursor, err = db_connect(server='172.22.99.61', db='test')
    except Exception, e:
        print "sth wrong with DB"
    #print t.checkdic1(['productinformation_version','random stuff','productqualitydegradation','identifier','productqualitystatus'],cursor)

         
        
    print t.injectionDic(meta, cursor)
    cursor.close()
    conn.close()
    

if __name__ == "__main__":
        test()
    
