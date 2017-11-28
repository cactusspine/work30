from utils import get_cur_time as get_time

class db_query():
    # =========================================================================
    # generic queries
    # =========================================================================
    #--- get
    def get_next_batch_id(self):
        return '''
                SELECT nextval('processing.processing_batch_batch_id')
               '''
        
    def get_md5_product(self, product_name):
        return '''
                SELECT pi.md5 as md5, 
                       p.id as id
                FROM eodas.product_info as pi,
                     internal.product as p
                WHERE pi.product_id=p.id 
                  AND p.name='{0}'
        '''.format(product_name)
        
    def get_if_imported(self, product_id):
        return '''
               SELECT product_id
               FROM eodas.product_status
               WHERE product_id={0}
                 AND (status='ARCHIVED' 
                 OR comment='sophia product copy on disk')
               '''.format(product_id)
    def get_document_id(self,doc):
        return '''
                    SELECT id 
                    FROM internal.document
                    WHERE name='{0}'
        '''.format(doc)
        
    
    
    def get_n_product_in_dms(self):
        return '''
            SELECT count(P.id) as nb_prod_total 
            FROM  internal.product P, 
                  eodas.product_status PS
            WHERE  PS.product_id=P.id 
               AND PS.status IN ('DISK','TAPE','SENT','ARCHIVED');
            '''
    
    
    def check_if_batch_exists(self, batch_id):
        return '''
                SELECT count(*) as n
                FROM processing.batch 
                WHERE batch_id = '{0}'
             '''.format(batch_id)
    
    def get_processing_set_id(self, batch_id):
        return '''
                SELECT processing_set_id
                FROM processing.batch 
                WHERE batch_id = '{0}'
             '''.format(batch_id)
             
             
    def get_path_md5_imported_products(self, dms_root=False, hsm_root=False):
        return et_prod_path_md5_products(self, 
                                         dms_root=dms_root,hsm_root=hsm_root, 
                                         imported=True)
    
    
    def get_prod_path_md5_products(self, dms_root=False, 
                                         hsm_root=False, 
                                         imported=False,
                                         p_name_lst=None):
        if dms_root:
            sel= "m.name || '/' ||"
        elif hsm_root:
            sel = "'/stornext/ham-fs2/global/' || '/' ||"
        else:
            sel = ''
        if imported:
            constrain = "AND ps.comment = 'sophia product copy on disk'"
        else:
            constrain = ""
        if p_name_lst:
            constrain += ' AND p.name in {}'.format(p_name_lst)
        
        return '''
               SELECT {0} mc.name || '/' || p.name || '.zip' as product_file, 
                      pi.md5 as md5, 
                      p.id as id,
                      p.name as name
               FROM
                    internal.product p,
                    eodas.product_info as pi,
                    internal.product_x_media_catalog_entry as pxmce,
                    internal.media_catalog_entry as mce,
                    internal.media_catalog as mc,
                    internal.media as m,
                    eodas.product_status as ps
                WHERE
                    pxmce.product = p.id
                    AND pi.product_id=p.id
                    AND ps.product_id=pi.product_id
                    AND ps.product_id=p.id
                    AND mce.id = pxmce.media_catalog_entry
                    AND mc.id = mce.media_catalog
                    AND m.id = mc.media 
                    {1}
               '''.format(sel, constrain)    
        
    def get_product_flag(self, product_id):
        return '''
            SELECT flag_type_id as flag
            FROM eodas.product_x_flag 
            WHERE product_id = {}
            '''.format(product_id)
    
    def get_duplicate_master_prod(self, product_id):
        return '''
            SELECT duplicate_from as p_id
            FROM eodas.product_x_flag 
            WHERE product_id = {}
        '''.format(product_id)
    
    def get_distinct_datset_id(self, product_id_lst):
        return '''
        SELECT distinct dataset_id 
        FROM internal.dataset_x_product 
        WHERE product_id in ({})
        '''.format(product_id_lst)
    
        
    def get_mc_path(self, product_id):
        return '''
                SELECT mc.name as path
                FROM  internal.media_catalog as mc,
                      internal.product_x_media_catalog_entry as pxmce,
                      internal.media_catalog_entry as mce
                WHERE pxmce.product = {}
                  AND mce.id = pxmce.media_catalog_entry
                  AND mc.id = mce.media_catalog 
                  
                '''.format(product_id)
        
    def get_product_instrement_path(self, product_id, dataset=False):
        if dataset:
            return '''
            SELECT d.name || '/' ||
                  COALESCE(pf.name,'NO_INFO') || '/' ||
                  COALESCE(s.name,'NO_INFO')  || '/' ||
              COALESCE(pt.name,'NO_INFO') as path
            FROM internal.product AS P JOIN eodas.product_info AS pi ON pi.product_id = p.id,
              internal.dataset_x_product AS dxp,
              internal.dataset AS d,
              internal.product_type AS pt,
              eodas.platform AS pf,
              eodas.sensor AS s
             WHERE 
              pt.id = COALESCE(p.product_type,'0') AND
              pf.id = COALESCE(pi.platform,'0') AND
              s.id = COALESCE(pi.sensor,'0') AND
              dxp.dataset_id = d.id AND
              dxp.product_id = p.id AND
              p.id = {};
            
            '''.format(product_id)
        else:
            return '''
            SELECT COALESCE(pf.name,'NO_INFO') || '/' ||
                   COALESCE(s.name,'NO_INFO')  || '/' ||
                   COALESCE(pt.name,'NO_INFO') as path
            FROM internal.product AS P JOIN eodas.product_info AS pi ON pi.product_id = p.id,
                 internal.product_type AS pt,
                 eodas.platform AS pf,
                 eodas.sensor AS s
            WHERE 
                  pt.id = COALESCE(p.product_type,'540') AND
                  pf.id = COALESCE(pi.platform,'0') AND
                  s.id = COALESCE(pi.sensor,'0') AND
                  p.id = {};
            '''.format(product_id)
     
        
    
    def get_proc_paramset(self, batch_id):
        return '''
                SELECT value 
                FROM processing.parameters_set 
                WHERE id = '{0}' 
                ORDER BY keyword_index
        '''.format(batch_id)
        
    def get_prod_path_md5(self, prod):
        return '''
               SELECT m.name || '/' || mc.name || '/' || p.name || '.zip' as path, 
                      pi.md5 as md5, 
                      p.id as id
               FROM
                    internal.product p,
                    eodas.product_info as pi,
                    internal.product_x_media_catalog_entry as pxmce,
                    internal.media_catalog_entry as mce,
                    internal.media_catalog as mc,
                    internal.media as m
                WHERE
                    pxmce.product = p.id
                    AND pi.product_id=p.id
                    AND mce.id = pxmce.media_catalog_entry
                    AND mc.id = mce.media_catalog
                    AND m.id = mc.media 
                    AND p.name = '{0}'
               '''.format(prod)
    
    def get_prod_path(self, p_id):
        return '''
               SELECT m.name || '/' || mc.name || '/' || p.name || '.zip' as product_path 
               FROM
                    internal.product p,
                    internal.product_x_media_catalog_entry as pxmce,
                    internal.media_catalog_entry as mce,
                    internal.media_catalog as mc,
                    internal.media as m
                WHERE
                    pxmce.product = p.id
                    AND mce.id = pxmce.media_catalog_entry
                    AND mc.id = mce.media_catalog
                    AND m.id = mc.media 
                    AND p.id = {0}
               '''.format(p_id)    




    def get_imported_tapes(self):
        return'''
            SELECT media_name
            FROM eodas.media_recovery_status 
            WHERE esa_md5 is NULL
        '''
    def get_imported_w_tapes(self):
        return '''
                SELECT media_name
                FROM eodas.media_recovery_status
                WHERE NOT is_available_for_write 
                  AND is_imported
                ORDER BY media_name
        '''
    
    def get_product_id_from_lst(self, f_list):
        return '''
            SELECT pi.product_id
            FROM eodas.product_info pi,
                 internal.product p
            WHERE p.id=pi.product_id
              AND p.name in {}
        '''.format(f_lst)
    def get_prod_id_simple_prod_no_esa_md5(self, pid_lst):
        return '''
            SELECT pi.product_id 
            FROM eodas.product_info pi, 
                 internal.dataset_x_product dxp, 
                 internal.dataset d,
                 eodas.dataset_info di
            WHERE pi.esa_md5 is NULL
              AND dxp.product_id=pi.product_id
              AND dxp.dataset_id=d.id
              AND di.dataset_name=d.name
              AND di.structure='SIMPLE'
              AND pi.product_id in {}
          '''.format(pid_lst)
    
    def get_esa_md5_simple_prod(self,product_id):
        return '''
                SELECT esa_md5
                FROM eodas.product_info
                WHERE product_id={}
        '''.format(product_id)
    
    def get_file_esa_md5(self, product_id):
        return '''
        SELECT filename, esa_md5
        FROM eodas.product_x_file
        WHERE product_id={}
        '''.format(product_id)
    
    
    def get_product_path(self, name=None, id=None, id_lst=None, 
                         only_path=True, extract_id=False):
        if id:
            constrain='p.id = {}'.format(id)
        elif name:
            constrain="p.name = '{}'".format(name)
        elif id_lst:
            constrain='p.id IN {}'.format(id_lst)
        else:
            return ''
        if only_path:
            sel = ''
        else:
            sel = "|| '/' || p.name || '.zip'"
        if extract_id:
            sel2 = ', p.id as id'
        else:
            sel2 = ''
        
        return '''
                    SELECT '/mount/' || mc.name {0} as product_path 
                           {2}  
                    FROM internal.product p, 
                         eodas.product_status ps, 
                         internal.product_x_media_catalog_entry as pxmce, 
                         internal.media_catalog_entry as mce, 
                         internal.media_catalog as mc 
                    WHERE {1}
                      AND pxmce.product = p.id 
                      AND ps.product_id= p.id 
                      AND mce.id = pxmce.media_catalog_entry 
                      AND mc.id = mce.media_catalog;
                '''.format(sel, constrain,sel2)
     
    def get_simple_prod_no_esa_md5(self, f_lst):
        return '''
                SELECT p.product_id 
                FROM ( eodas.product_info pi 
                        INNER JOIN internal.product 
                        ON internal.product.id = pi.product_id) as p, 
                     internal.dataset_x_product dxp, 
                     internal.dataset d,
                     eodas.dataset_info di
                WHERE p.esa_md5 is NULL
                  AND dxp.product_id=p.product_id
                  AND dxp.product_id=p.product_id
                  AND dxp.dataset_id=d.id
                  AND di.dataset_name=d.name
                  AND di.structure='SIMPLE'
                  AND p.name in {}
                  '''.format(f_lst)
    
    def get_complex_prod_no_esa_md5(self, f_lst):
        return '''
                SELECT distinct p.id
                FROM eodas.product_x_file pxf,
                     internal.product p
                WHERE pxf.product_id=p.id
                  AND pxf.esa_md5 IS NULL
                  AND p.name in {}
        '''.format(f_lst)
    
    #--- update
    def update_comment(self, product_id):
        return '''
                UPDATE eodas.product_status 
                SET comment='sophia product copy on disk'
                WHERE product_id={0}
        '''.format(product_id)
         
    def update_esa_md5_simple_prod(self, prod_id, esa_md5):
        return '''UPDATE eodas.product_info 
                  SET esa_md5='{0}' 
                  WHERE product_id={1}'''.format(esa_md5, prod_id)
        
    def update_esa_md5_prod(self, prod_id, f_name ,esa_md5): 
        return '''UPDATE eodas.product_x_file
                  SET esa_md5='{0}'
                  WHERE product_id={1} AND filename LIKE '%{2}' 
               '''.format(esa_md5, prod_id, f_name)
     
    def get_file_name_esa_md5(self, product_id):
        return '''
            SELECT filename, esa_md5
            FROM eodas.product_x_file
            WHERE product_id={} 
        '''.format(product_id)
    
    #--- insert
    def insert_proc_bathc(self,BATCH_ID, PRODUCT_ID, PROCESSING_SET_ID, 
                          BATCH_STATE, OUTPUT_DIR, REQUEST_ID, 
                          OUTPUT_MEDIA_CATALOG):
        return '''
                    INSERT INTO processing.batch (batch_id, file_input_id, 
                                                  processing_set_id, state, 
                                                  output_dir, request_id, 
                                                  output_media_catalog) 
                    VALUES ( {0}, {1}, {2}, '{3}', '{4}', {5}, {6})
        '''.format(BATCH_ID, PRODUCT_ID, PROCESSING_SET_ID, BATCH_STATE, 
                   OUTPUT_DIR, REQUEST_ID, OUTPUT_MEDIA_CATALOG)
    
    def insert_proc_paramset(self,  BATCH_ID, ORDER, PARAM_NAME, VALUE):
        return '''
                    INSERT INTO processing.parameters_set (id, keyword_index, 
                                                            keyword, value) 
                    VALUES ({0}, {1}, '{2}', '{3}')
        '''.format(BATCH_ID, ORDER, PARAM_NAME, VALUE) 
    
    # ========================================================================= 
    # queries for tapes
    # =========================================================================
    def select_medias_in_db(self, all=True, policy=None):
        if policy:
            constrain = "policy='{}'".format(policy)
        if all:
            constrain = " WHERE {}".format(constrain)
            return 'SELECT tape_id FROM eodas.tape_info {}'.format(constrain)
        else:
            constrain = " AND {}".format(constrain)
            return 'SELECT tape_id FROM eodas.tape_info WHERE NOT removed {}'.fomat(constrain)
        
    def select_removed_medias_in_db(self):
                return 'select tape_id from eodas.tape_info WHERE removed' 
    
    def insert_media(self, tape_id, import_date):
        return '''
                INSERT INTO eodas.tape_info (tape_id, import_date) 
                VALUES ('{0}','{1}')'''.format(tape_id, import_date)
                
    def update_removed_media(self, tape_id_str):
        return '''
               UPDATE eodas.tape_info SET removed=FALSE  
               WHERE tape_id IN {0} and removed'''.format(tape_id_str)

    def flag_removed_media(self, tape_ids):
        return '''
               UPDATE eodas.tape_info SET removed=TRUE  
               WHERE tape_id IN {0} and NOT removed'''.format(tape_ids)
       
    def update_media_status(self, media_id):
        d=get_time()
        return '''
                UPDATE eodas.tape_info 
                SET checked=TRUE, verification_date='{1}', rank='{1}' 
                WHERE tape_id='{0}' 
                '''.format(media_id, d)
                
    def get_media_to_check(self, sample_size, list_av):
                return """
                        SELECT tape_id FROM eodas.tape_info 
                        WHERE NOT removed AND tape_id in {1}
                        ORDER BY checked, verification_date, import_date 
                        LIMIT (
                                SELECT COUNT(*) FROM eodas.tape_info
                                WHERE NOT removed
                            )*{0}""".format(sample_size, list_av)
                            
    def count_available_media(self):
                return 'SELECT COUNT(*) FROM eodas.tape_info WHERE NOT removed'
    def update_media_status(self, media_id):
        d=get_time()
        return '''
                UPDATE eodas.tape_info 
                SET checked=TRUE, verification_date='{1}', rank='{1}'  
                WHERE tape_id='{0}' 
                '''.format(media_id, d)
                
    def get_path_and_md5_list(self, list_prod):
        return '''
                SELECT p.name as name, mc.name || '/' || mce.name as local_path, pi.md5 as md5
                FROM internal.media_catalog_entry as mce,
                     internal.media_catalog as mc,
                     internal.product as p,
                     eodas.product_info as pi
                WHERE p.name IN {0}
                AND mce.media_catalog=mc.id
                AND p.id=pi.product_id
                AND mce.name=p.name||'.zip' '''.format(list_prod)   
    def get_tape_from_sophia(self, tape_lst):
        return '''
                SELECT media_name as tape_id
                FROM eodas.media_recovery_status 
                WHERE media_name in {0}
               '''.format(tape_lst)
    def update_media_recovery(self, media):
        return '''
               UPDATE eodas.media_recovery_status 
               SET sent_back_to_sophia=TRUE
               WHERE media_name='{0}'
               '''.format(media)
    def update_media_recovery_all(self, tape_lst):
        return '''
               UPDATE eodas.media_recovery_status 
               SET sent_back_to_sophia=TRUE
               WHERE media_name IN {0}
               '''.format(tape_lst)
    def insert_in_media_transfer(self, media, requester=2):
        return '''
                INSERT INTO eodas.media_transfer 
                (media_name, action, action_date, requester)
                VALUES ('{0}', 'SENT', NOW(), {1} )
               '''.format(media, requester)

    def insert_media_into_media(self, media, capacity, upsert=False):
        if upsert:
            return '''INSERT INTO internal.media 
                      (media_type,name,current_physical_capacity,recipient) 
                      VALUES (10,'{0}',{1},2)
                      ON CONFLICT (name)
                          DO UPDATE 
                              SET current_physical_capacity={1} 
                              WHERE internal.media.name='{0}'
                    '''.format(media, capacity)
        else:
            return '''INSERT INTO internal.media 
                      (media_type,name,current_physical_capacity,recipient) 
                      VALUES (10,'{}',{},2)
                    '''.format(media, capacity)
    
    def insert_media_into_media_info(self, media, used_capacity, upsert=False):
        if upsert:
            return '''
                    INSERT INTO eodas.media_info 
                    (media_name,creation_date,used_capacity,sent_by,expedition_date,sent_to) 
                    VALUES ('{0}',NOW(),{1},2,date_trunc('hour',NOW()),0)
                    ON CONFLICT (media_name)
                        DO UPDATE 
                              SET used_capacity={1}, 
                                  expedition_date=NOW(),
                                  sent_to=0,
                                  sent_by=2
                              WHERE eodas.media_info.media_name='{0}'
                    '''.format(media, used_capacity)
        else:
            return '''
                    INSERT INTO eodas.media_info 
                    (media_name,creation_date,used_capacity,sent_by,expedition_date,sent_to) 
                    VALUES ('{0}',NOW(),{1},0,date_trunc('hour',NOW()),2)
                    '''.format(media, used_capacity)
                    
    def update_prod_status(self, prod_lst):
        
        return '''
                UPDATE eodas.product_status PS SET status='SENT', 
                       comment='', cdate=date_trunc('hour',NOW()) 
                FROM internal.product P 
                WHERE PS.product_id=P.id 
                  AND P.name IN {0}
        '''.format(prod_lst)
    def update_prod_status_to_arch(self, prod_id):
        
        return '''
                UPDATE eodas.product_status PS SET status='ARCHIVED', 
                       comment='', cdate=date_trunc('hour',NOW()) 
                FROM internal.product P 
                WHERE PS.product_id={} 
        '''.format(prod_lst)
    def update_esa_md5_status(self, tape, status):
        if status:
            status='TRUE'
        else:
            status='FALSE'
        return '''
                UPDATE eodas.media_recovery_status 
                SET esa_md5={0}
                WHERE media_name = '{1}'  
        '''.format(status,tape)
    
    def update_tape_status_mrs(self, tape_lst):
        
        return '''
        UPDATE eodas.media_recovery_status
        SET is_available_for_write=TRUE,
            cdate=NOW()
        WHERE media_name IN {}
        '''.format(tape_lst)
    
    
    def count_prod_not_in_tape(self, prod_lst):
        return '''
                SELECT COUNT(*) as n
                FROM eodas.product_status PS, 
                     internal.product P 
                WHERE  PS.product_id=P.id
                    AND NOT (PS.status='TAPE' OR PS.status='SENT')
                    AND P.name IN {0}
               '''.format(prod_lst)
        
    def get_prod_not_in_tape(self, prod_lst):
        return '''
                SELECT P.id, P.name, PS.status
                FROM eodas.product_status PS, 
                     internal.product P 
                WHERE  PS.product_id=P.id
                    AND NOT (PS.status='TAPE' OR PS.status='SENT')
                    AND P.name IN {0}
               '''.format(prod_lst)
    
    def insert_tape_info(self, info):
        str_field = ['tape_id', 'import_date', 'policy']
        ex = ['tape_id', 'import_date']
        
        fields = ','.join(info.keys())
        values = ["'{}'".format(info[k]) if k in str_field 
                      else "{}".format(info[k]) 
                      for k in info.keys() ]
        values  = ','.join(values)        
        
        set_str = [
                   "{}='{}'".format(k,info[k]) if k in str_field 
                      else "{}={}".format(k,info[k]) 
                      for k in info.keys() 
                      if k not in ex  
                ]
        set_str  = ','.join(set_str)
        return '''
        INSERT INTO eodas.tape_info
            ({0}) VALUES ({1})
            ON CONFLICT (tape_id)
                DO UPDATE
                    SET {2} 
                    WHERE eodas.tape_info.tape_id='{3}' 
        '''.format(fields, values, set_str, info['tape_id'])
     
    def insert_tape_recovery_status(self, media):
        return '''
        INSERT INTO eodas.media_recovery_status 
        (media_name, cdate, is_added_to_mailbox) VALUES ('{}', NOW(), 'true')
        '''.format(media)
        
    def set_rank_for_not_checked_tapes(self):
        return ''' 
        UPDATE eodas.tape_info 
        SET rank=import_date
        WHERE NOT Checked'''
    def get_ranked_tape(self):
        return '''
        SELECT tape_id, segments 
        FROM eodas.tape_info 
        WHERE segments > 0 AND NOT removed ORDER by rank
        '''
    def get_structure_from_pid(self, pid):
        return '''
            SELECT  di.structure
            FROM internal.dataset_x_product dxp,
                 eodas.dataset_info di,
                 internal.dataset d
            WHERE di.dataset_name=d.name
              AND dxp.dataset_id=d.id
              AND dxp.product_id={}
        '''.format(pid)
        