

class db_query_ingestion():
    def get_product_status(self, product_id):
        return '''
                SELECT status 
                FROM eodas.product_status 
                WHERE product_id = {} 
                ORDER BY product_id DESC LIMIT 1"
        '''.format(product_id)
        
    def update_product_status(self, product_id, status):
        return '''
                UPDATE eodas.product_status 
                SET status = '{1}' WHERE product_id = '{0}'
        '''.format(product_id, status)
        
    def get_product_info(self, product_id):
        return '''
        SELECT name, product_type FROM internal.product WHERE product.id ='{}'
        '''.format(product_id) 
    
    def get_initial_path(self, product_id):
        return '''
        SELECT initial_path FROM eodas.product_status 
        WHERE product_status.product_id='{}'
        '''.format(product_id)
    def get_initial_path(self, product_id):
        return '''
        SELECT initial_path FROM eodas.product_status 
        WHERE product_status.product_id='{}'
        '''.format(product_id)
        
    def get_duplicated_prod(self, product_id):
        return '''
        SELECT duplicate_from 
        FROM eodas.product_x_flag 
        WHERE product_id='{}' and flag_type_id=2
        '''.format(product_id)   