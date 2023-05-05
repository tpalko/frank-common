import os
import sys 
import traceback 
import cowpy 
from jroutes.routing import base_response
from contextlib import contextmanager
import mariadb
from datetime import datetime 

logger = cowpy.getLogger()

TYPE_MAPPINGS = {
    str: 'varchar',
    int: 'int',
    datetime.date: 'datetime'
}

class DatabaseConfig(object):

    host = None 
    user = None 
    password = None 
    name = None 

    @staticmethod
    def New(host, user, password, name):

        __instance = DatabaseConfig()

        __instance.host = host 
        __instance.user = user 
        __instance.password = password 
        __instance.name = name 

        return __instance

class Database(object):

    cfg = None 
    tables = None 
    insert_cols = None 

    def __init__(self, *args, **kwargs):

        required_keys = ['config', 'tables']
        valid = all([ k in kwargs for k in required_keys ])

        if not valid:
            raise Exception(f'Please provide required keys: {required_keys}')
        
        if 'config' in kwargs:
            self.cfg = kwargs['config']

            if not isinstance(self.cfg, DatabaseConfig):
                raise Exception("Provided config is not DatabaseConfig")
        
        if 'tables' in kwargs:
            self.tables = kwargs['tables']
        
        with self.cursor() as cur:
            for tablename in self.tables['models'].keys():
                try:
                    cur.execute(f'select 1 from {tablename}')
                except:
                    logger.error("well that sucked")
                    cur.execute(f'create table {tablename} ({self.tables["models"][tablename]}) engine=innodb default charset=utf8;')

    def _column_type(self, col):
        size = ''
        if 'size' in col:
            size = f'({col["size"]})'
        return f'{TYPE_MAPPINGS[col["type"]]}{size}'
    
    def _column_def(self, col):
        return f'{col["name"]} {self._column_type(col)}'
    
    def _create_table(self, base, model):
        return f'(\
            {base["primary_key"]} int primary key auto_increment \
            {",".join([ self._column_def(col) for col in model ])}\
            {"," if len(base["timestamps"]) > 0 else ""} \
            {[",".join([ t + " datetime" for t in base["timestamps"] ])]} \
        )'
    
    @contextmanager
    def cursor(self):
        conn = mariadb.connect(host=self.cfg.host, user=self.cfg.user, password=self.cfg.password, database=self.cfg.name)
        with conn.cursor() as cur:
            yield cur 
        conn.commit()

    def _select(self, table, cols, where={1:1}, order_by=None):

        response = base_response()

        try:
            logger.debug(f'selecting {table} {cols} where {where}')
            where_clause = ' and '.join([ f'{w} = ?' for w in where.keys() ])
            params = tuple([ where[w] for w in where ])
            query = f'select {",".join(cols)} from {table} where {where_clause} '
            if order_by:
                query = f'{query} order by {order_by}'

            with self.cursor() as cur:
                cur.execute(query, params)
                response['data'] = cur.fetchall()

            response['success'] = True 

        except:
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'
            logger.error(response['message'])
            traceback.print_tb(sys.exc_info()[2])
            raise 
        
        return response 

    def _update(self, table, set={}, where={}):

        response = base_response()

        try:
            logger.debug(f'updating {table} {set} {where}')
            query = f'update {table} \
                set {",".join([ k + " = ? " for k in set.keys() ])} \
                where {" AND ".join([ k + " = ? " if where[k] else k + " is null " for k in where.keys() ])};'
            where = { k: where[k] for k in where.keys() if where[k] }
            with self.cursor() as cur:
                logger.debug(f'{query} {set} {where}')
                cur.execute(query, tuple(set.values()) + tuple(where.values()))
            response['success'] = True 
        except:
            logger.exception()
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'

    def _delete(self, table, id):

        response = base_response()

        try:
            logger.debug(f'deleting {table} {id}')
            query = f'delete from {table} where id = ?'
            with self.cursor() as cur:
                cur.execute(query, (id,))            
            response['success'] = True 
        except:
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'
            logger.error(response['message'])
            traceback.print_tb(sys.exc_info()[2])
        
        return response 

    def _insert(self, table, *params):

        response = base_response()

        try:
            logger.debug(f'inserting {table} {params}')
            query = f'insert into {table} ({",".join(self.insert_cols[table])}) values({",".join([ "?" for p in self.insert_cols[table] ])})'
            with self.cursor() as cur:
                cur.execute(query, tuple(params))    
                response['data']['insert_id'] = cur.lastrowid
            response['success'] = True 

        except:
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'
            logger.error(response['message'])
            traceback.print_tb(sys.exc_info()[2])
        
        return response 