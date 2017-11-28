'''
===============================================================================
extractMetada functions for the metadata extraction from the raw metadata files
AUTHOR: Jiali WANG
===============================================================================
Changes:
===============================================================================
   Date          Author(s)                     Comment
2017-10-31 : JW                        First dev version
2017-11-03 : JW+GC                     First prod version
===============================================================================
'''
#===============================================================================
#--- Dependencies
#===============================================================================
#------ Python Modules
#===============================================================================
import re
from lxml import etree as ET
from netCDF4 import Dataset
from os.path import isfile
#===============================================================================
#------ Custom Modules
#===============================================================================
from db_utils import connect as db_connect
from db_utils import submit_query as submit_query
from utils    import get_dic_time, remove_duplicate
#===============================================================================
#------ global var
#===============================================================================
class extractMetadata(object): 
    
    dic = {}
    err = 100
    
    # Raw metadata extraction
    
    def filter_not_needed(self, mylist):
        '''
        filter_not_needed(list)->list_shorted
        this function is to shorten the string of keys in the extracted Mettadata from XML file 
        this function is called by extractMetadata(MD.xml file)
        :param list:a list of extrated key string parts for each property 
        '''
        myfilter = ['earthobservation', 'earthobservationequipment', 'procedure',
                    'metadataproperty', 'result', 'earthobservationresult',
                    'earthobservationmetadata', 'processing',
                    'acquisitionparameters', 'featureofinterest',
                    'phenomenontime', 'product', 'l0-dls-metadata']
        return [x for x in mylist if not x in myfilter]   
    
    def xml(self, content_file,fromstring=None):
        '''
        this function resorve the xml file and return the metadata in dictionary
        extractMetada(xml_file_metadata)->dictionary
        :param content_file:
        a complete path file name towards a xml file of the metadata,
         which contains multiple snaps and locations
        #example return value {'earthobservation_resulttime_timeinstant': '2011-10-16T20:52:46Z'}
        '''
        product = content_file
        if fromstring:
            try:
                tree=ET.fromstring(content_file)
                print ("the xml file has been successfully read in!")
            except:
                return None,103
        else:    
            if not isfile(product):
                return None, 101
            try:
                tree = ET.parse(product)
            except ET.ParserError:
                return None, 102 
            except Exception, e:
                return None, 999 
            else:
                print ("the xml file has been successfully read in!")    
        dic = {}
        avoid_string = 'vendorspecific'
        for ele in tree.iter():  # deepth first iteration
            #--- check if element has text, or if text is of None type
            try:
                len(ele.text)
            except:
                # print "this node is empty"
                continue
            if ele.text.isspace():
                continue
            val = ele.text
            property = []
            #--- extract the tag of the current and its ancester in reverse order
            for arc in ele.iterancestors():
                property.insert(0, arc.tag)
            property.append(ele.tag)
            #--- convert the tags to the key for value, store
            property = [re.sub(r'\{.*?\}', "", p).lower() for p in property]
            property = remove_duplicate(property)
            property = self.filter_not_needed(property)
            property = '_'.join(property)
            # -- current node is a node of specific structure,avoid
            if avoid_string in property:
                continue
            # --handle same property multiple value
            same_key_list = [k for k, v in dic.items()if k.startswith(property)]
            if same_key_list:
                property = property + '_' + str(len(same_key_list))

            dic[property] = val
            
        #--- insert the specific case of vendorSpecific in dic
        try:
            for node in tree.findall('//eop:vendorSpecific//eop:localValue', namespaces=root.nsmap):
                if node.getprevious() is None:
                    # print "this local value have no attribute:", node.tag, node.text
                    continue
                else:
                    attribute = 'vendorspecific_' + node.getprevious().text.lower()
                    localValue = node.text
                    dic[attribute] = localValue 
        except Exception, e:
            print "sth wrong with the nodes!"   
               
        return dic, 0
    
    def eosip(self, content_file):
        '''
        this function resorve the xml file and return the extracted metadata in dictionary
        extractMetada(xml_file)->dictionary
        :param content_file:a complete path file name towards a EOSIP format xml file of the metadata
        #example return value {'earthobservation_resulttime_timeinstant': '2011-10-16T20:52:46Z'}
        '''
        product = content_file
        if not isfile(content_file):
            return None, 101
       
        try:
            tree = ET.parse(product)
        except ET.ParserError:
            return None, 102 
        except:
            return None, 999 
        else:
            print ("the xml file has been successfully read in!")    
            
        root = tree.getroot()
        dic = {}
        avoid_string = 'vendorspecific'
    
        for ele in tree.iter():
            try:
                len(ele.text)
            except:
                # print "this node is empty"
                continue
            if ele.text.isspace():
                # print "empty string"
                continue
            val = ele.text
            property = []
            
            #--- extract the tag of the current and its ancester in reverse order
            for arc in ele.iterancestors():
                property.insert(0, arc.tag)
            property.append(ele.tag)
               
            #--- convert the tags to the key for value, store
            property = [re.sub(r'\{.*?\}', "", p).lower() for p in property]
            property = remove_duplicate(property)
            property = self.filter_not_needed(property)
            property = '_'.join(property)
            
            if avoid_string in property:
                continue
            dic[property] = val
            
        #--- insert the specific case of vendorSpecific in dic
        try:
            for node in tree.findall('//eop:vendorSpecific//eop:localValue', namespaces=root.nsmap):
                if node.getprevious() is None:
                    # print "this local value have no attribute:", node.tag, node.text
                    continue
                else:
                    attribute = 'vendorspecific_' + node.getprevious().text.lower()
                    localValue = node.text
                    dic[attribute] = localValue 
        except:
            print "can't find eop vendor"             
               
        return dic, 0
    
    def netcdf(self, nc_file):
        '''
        this function extract metaData from NC/netcdf format data
        extractMetaNc(nc_file)->dictionary
        this function is from Gaston
        need netcdf4 package
        :param a nc format file
        '''
        if not isfile(nc_file):
            return None, 101
        dic = {}
        try:     
            myDataset = Dataset(nc_file)
        except Exception, e:
            return None, 103
        else:
            print "netcdf file loaded!"     
        dico_dimensions = {}
        for cle in myDataset.dimensions.keys():
            dico_dimensions[cle] = myDataset.dimensions[cle].size
            dic['dimentions'] = dico_dimensions
        
        # GET ALL VARIABLES
        dico_vbles = {}
        for cle in myDataset.variables.keys():
            temp_grp = myDataset.variables[cle]
            temp_dict = {}
            # tempJson='{'
            for attr in temp_grp.ncattrs():
                temp_dict[attr.lower()] = temp_grp.getncattr(attr)
                # tempJson+='"%s" : "%s" , ' %(attr,temp_grp.getncattr(attr))
            # tempJson=tempJson[:-2]+'}'
            dico_vbles[cle] = temp_dict
            # myJson+='"variables_%s" : %s , ' % (cle,tempJson)
            dic['variables'] = dico_vbles
       
        # GET ALL ATTRIBUTES
        dico_attr = {}    
        for attr in myDataset.ncattrs():
            dico_attr[attr.lower()] = getattr(myDataset, attr)
            dic[attr] = getattr(myDataset, attr)
            # myJson+='"attributes_%s" : "%s" , ' % (attr,getattr(myDataset, attr))
        return dic, 0
    
    
    def asc(self, asc_file):
        '''
        this fun resorve the asc file and return the extracted metadata as dictionary
        extractMetada(asc_file)->dictionary
        :param asc_file:
        '''
        dic = {}
        if not isfile(asc_file):
                return None, 100
        with open(asc_file, 'r') as f:
            for line in f:
                if not line.strip():
                    # print "this is empty line, end reading"
                    break
                attribute, val = [p.strip().lower() for p in line.split('|')]
                dic[attribute] = val
            f.close()           
        return dic, 0
    
    def n1(self, product):
        '''
        This function extract metaData from the N1 file
        TLM_HK__0PNLRC20020407_142604_000060382004_00483_00537_1829.N1
        :param n1_file: meta+binary mix data type
        '''
        if not isfile(product):
                return None, 100
        d = {}
        found = False
        with open(product, 'r') as f:
            for line in f:
                if 'DS_NAME' in line:
                    found = True
                    break
                elif not line.strip():
                    continue
                else:
                    attri, val = line.split('=')
                    val = val.strip().strip('"').strip()
                    # if val is with unit, remove
                    val = re.sub(r'\<.*?\>', "", val)
                    d[attri] = val
        if not found:
            return None, 200  # no Ending of metadata           
        return d, 0             


    def checkdic(self, keys_list, cursor, version=1, conn=None):
        '''
        This function take a list of the field and cursor connection as input
        and return the corresponding keys lists if it exist in the database.
        checkdic(list,cursor,*version,*connection)->list,error_code
        '''
        query = '''SELECT field, keys
                    FROM  eodas.field_x_keys
                    WHERE version = {}'''
        err=submit_query(query.format(version), cursor, commit=False, conn=conn)
        if err['code'] !=0:
            print err['ms']
            return [], err['code']
            
        dic_kf = {}
        if cursor.rowcount == 0:
            return None, 401  # empty reply from the DB
        else:
            for row in cursor:
                for kv in row[1]:
                    if kv in dic_kf:
                        # this kv already have an associated field
                        dic_kf[kv].append(row[0])
                    else:
                        dic_kf[kv] = [row[0]]
        #fieldlist = [dic_kf[attri] for attri in keys_list if attri in dic_kf.keys()]
        
        fieldlist = [dic_kf.get(attri, None) for attri in keys_list]            
        return fieldlist, 0

    def injectionDic(self, key_dict, cursor):
        '''
        This function take a dictionary of matadata extracted with original attributes
        and transform it into a dictionary using the standarised fields in DB as the key 
        injectionDic(key_dictionary)->field_dictionary
        :param key_dict: dictionary containing metadata before mapping to DB
        '''
        
        fields = ['validity', 'start_date_time', 'stop_date_time', 'footprint',
             'processing', 'orbit_number', 'orbit_direction', 'track_min',
             'track_max', 'frame_min', 'frame_max', 'product_identifier',
             'swath_identifier', 'file_class', 'mission_phase', 'product_version',
             'product_quality', 'product_creation_date', 'product_type_name',
             'product_subdir',
             'center_id', 'center_name', 'center_code', 'platform_platform_id',
             'platform_short_name', 'platform_name', 'platform_code', 'sensor_type',
             'sensor_operational_mode', 'sensor_resolution', 'sensor_name',
             'sensor_resolution_unit', 'year', 'month', 'day']
        
        
        
        ori_keys = key_dict.keys()  # attris from the metadata may contains number
        keys_without_nr = [re.sub('_[0-9]+$', '', k) for k in ori_keys]
        listfield, error = self.checkdic(keys_without_nr, cursor)
        dic_map = dict(zip(ori_keys, listfield))  # mapping originalkey with nr ,list field
        dic = {}
        for f in fields:
            dic[f] = "NULL"
        for ori_key, field_list in dic_map.items():
            if field_list is None:
                # print "can't find corresponding field for {}".format(ori_key)
                continue
            for field in field_list:
                #--- field which will pad a  dictionary
                if field in ['footprint', 'product_quality', 'processing']:         
                    if dic[field] == "NULL":
                        # print "enter first entry"
                        dic[field] = {ori_key : key_dict[ori_key]}
                    else:
                        dic[field].update({ori_key:key_dict[ori_key]})
                       
                #--- field need comparison from multiple original keys      
                elif field == 'track_min' or field == 'frame_min':
                    # print 'enter branck 2'
                    if dic[field] == "NULL":
                        dic[field] = int(key_dict[ori_key])
                    else:    
                        dic[field] = min (dic[field], int(key_dict[ori_key]))  
                        
                elif field == 'track_max' or field == 'frame_max':
                    # print 'enter branck 3'
                    if dic[field] == "NULL":
                        dic[field] = int(key_dict[ori_key])
                    else:    
                        dic[field] = max (dic[field], int(key_dict[ori_key]))    
                else:
                    dic[field] = key_dict[ori_key]
        #--- time related field special            
        if  dic['year'] == 'NULL' or dic['month'] == 'NULL' or dic['day'] == 'NULL':
              dictime = get_dic_time(dic['start_date_time']) 
              dic['year']  = dictime['year']
              dic['month'] = dictime['month']
              dic['day']   = dictime['day']
        return dic, 0
    
    def error(self):
        return  {
                # error related to arg parsing
                100 : "lacking method to handle {} type of data".format(self.input_format),
                101 : 'file does not exist',
                102 : 'xml Parser error',
                103 : 'netcdf parsing error',
                200: 'the stop line of N1 file metadata does not exist',
                # error related to database
                400: 'can not connect to DB',
                401: 'no retrieval from DB',
                # unknow error
                999: 'unknown error'
            }
 
        
    def __init__(self, file_path=None, input_format='xml'):
        '''
        initiate extractMetadata class by the input file and assign a format
        :param file_path:the full path to the metadata file which need extract
        :param input_format: string,can be xml, asc, n1,cdf
        '''
        if file_path is None:
            print "creat empty extractMetadata class "
            return
        self.file_format = input_format
        
        if input_format == 'xml':
            self.dic, self.err = self.xml(file_path)
        elif input_format == 'asc':
            self.dic, self.err = self.asc(file_path)
        elif input_format == 'n1':
            self.dic, self.err = self.n1(file_path)
        elif input_format == 'cdf':
            self.dic, self.err = self.netcdf(file_path)
        else:
            print "lacking method to handle {} type of data".format(input_format)           
        
    def getraw(self):
        '''
        get method of the raw dictionary extracted from various format of metadata
        '''
        return self.dic, self.err
 
