import os
import sys 
import traceback 
import cowpy 
from contextlib import contextmanager
import sqlite3 
import mariadb
from enum import Enum
from datetime import datetime 

logger = cowpy.getLogger()

class DbType(Enum):
    MariaDB = 0
    Sqlite = 1

class Dialect(Enum):
    AUTO_INCREMENT = 0
    ENGINE = 1
    FLOAT = 2

db_dialect = {
    DbType.Sqlite: {
        Dialect.AUTO_INCREMENT: 'autoincrement',            
        Dialect.ENGINE: '',
        Dialect.FLOAT: 'real'
    },
    DbType.MariaDB: {
        Dialect.AUTO_INCREMENT: 'auto_increment',            
        Dialect.ENGINE: 'engine=innodb default charset=utf8',
        Dialect.FLOAT: 'decimal'
    }
}

db_providers = {
    DbType.Sqlite: lambda config: sqlite3.connect(config.filename),
    DbType.MariaDB: lambda config: mariadb.connect(host=config.host, user=config.user, password=config.password, database=config.name)
}

TYPE_MAPPINGS = {
    str: lambda config: 'varchar',
    int: lambda config: 'int',
    datetime.date: lambda config: 'datetime',
    bool: lambda config: 'tinyint(1)',
    float: lambda config: db_dialect[config.dbType][Dialect.FLOAT]
}

def _response(success=False, message='', data={}):
    return {
        'success': success,
        'message': message,
        'data': data 
    }

class DatabaseConfig(object):

    host = None 
    user = None 
    password = None 
    name = None 

    filename = None 

    dbType = None 

    @staticmethod
    def NewSqlite(filename):

        __instance = DatabaseConfig()

        __instance.filename = filename 

        __instance.dbType = DbType.Sqlite

        return __instance
    
    @staticmethod
    def NewMariadb(host, user, password, name):

        __instance = DatabaseConfig()

        __instance.host = host 
        __instance.user = user 
        __instance.password = password 
        __instance.name = name 

        __instance.dbType = DbType.MariaDB

        return __instance

class BcktDatabaseException(Exception):
    pass 

class Database(object):

    conn = None 
    cfg = None 
    tables = None 
    insert_cols = None 

    def __init__(self, *args, **kwargs):

        required_keys = ['config', 'tables']
        valid = all([ k in kwargs for k in required_keys ])

        if not valid:
            raise Exception(f'Please provide required keys: {required_keys}')
        
        self.cfg = kwargs['config']

        if not isinstance(self.cfg, DatabaseConfig):
            raise Exception("Provided config is not DatabaseConfig")
        
        self.tables = kwargs['tables']
        
        self.insert_cols = { m: [ c['name'] for c in self.tables['models'][m] ] for m in self.tables['models'].keys() }

        for m in self.insert_cols.keys():
            self.insert_cols[m].extend([ t for t in self.tables['base']['timestamps'] ])

        with self.cursor() as cur:
            for tablename in self.tables['models'].keys():
                try:
                    cur.execute(f'select 1 from {tablename}')
                except:
                    logger.exception()
                    logger.warning(f'{tablename} not found, creating now')
                    create_table_sql = f'create table {tablename} {self._create_table(tablename)} {db_dialect[self.cfg.dbType][Dialect.ENGINE]};'
                    logger.warning(create_table_sql)
                    try:
                        cur.execute(create_table_sql)
                    except:
                        logger.exception()

    def __repr__(self):
        return str(self.__dict__)

    def parse_type(self, column_name, value):
        if value is not None:
            if column_name[-3:] == '_at':
                parsed = value 
                try:
                    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                except:
                    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return parsed
            elif column_name[0:3] == 'is_':
                return bool(value)
        return value 
    
    def dict_factory(self, cursor, row):
        return { col[0]: self.parse_type(col[0], row[idx]) for idx,col in enumerate(cursor.description) }
    
    def _column_type(self, col):
        colType = TYPE_MAPPINGS[col["type"]](self.cfg)
        size = ''
        if 'size' in col:
            size = f'({col["size"]})'
        elif colType == 'varchar':
            colType = 'text'
        return f'{colType}{size}'
    
    def _column_def(self, col):
        return f'{col["name"]} {self._column_type(col)}'
    
    def _create_table(self, tablename):
        return f'(\
            {self.tables["base"]["primary_key"]} INTEGER PRIMARY KEY {db_dialect[self.cfg.dbType][Dialect.AUTO_INCREMENT]}, \
            {",".join([ self._column_def(col) for col in self.tables["models"][tablename] ])}\
            {"," if len(self.tables["base"]["timestamps"]) > 0 else ""} \
            {[",".join([ t + " datetime" for t in self.tables["base"]["timestamps"] ])]} \
        )'
    
    @contextmanager
    def get_cursor(self):
        '''Generic cursor munging, dialect fallback, nothing else'''
        try:
            # -- some cursors will have their own context 
            # -- e.g. mariadb
            with self.conn.cursor() as c:
                yield c 
        except AttributeError as ae:
            # -- there is a particular case where self.conn.cursor() will fail with sqlite 
            # -- and simply yieldling self.conn.cursor() is the answer 
            # -- no context will manage the transaction or connection for us
            # try:
            yield self.conn.cursor()
                #self.conn.commit()
            # finally:
                
        except:          
            # -- but if anything else goes wrong, kick
            logger.exception()  
            raise
        finally:
            self.conn.commit()
            self.conn.close()

    @contextmanager 
    def cursor(self):

        self.conn = db_providers[self.cfg.dbType](self.cfg)
        self.conn.row_factory = self.dict_factory

        with self.get_cursor() as c:
            try:
                yield c 
            except:
                # -- wrap all errors simply for backup callers
                logger.exception()
                raise BcktDatabaseException(sys.exc_info()[1])            
    
    ### ORIGINAL 
    # @contextmanager
    # def cursor(self):
    #     conn = mariadb.connect(host=self.cfg.host, user=self.cfg.user, password=self.cfg.password, database=self.cfg.name)
    #     with conn.cursor() as cur:
    #         yield cur 
    #     conn.commit()

    def raw(self, query, params=()):

        with self.cursor() as cur:
            cur.execute(query, params)
            yield cur

    def _select_cols(self, table):
        def_cols = [ f'{table[0]}.{col["name"]}' for col in self.tables['models'][table] ]
        return [ f'{table[0]}.{self.tables["base"]["primary_key"]}', *def_cols, *[ f'{table[0]}.{t}' for t in self.tables['base']['timestamps'] ] ]
    
    def _table_alias(self, table):
        return f'{table} {table[0]}'
    
    def _table_join(self, t1, table):

        if 'foreign_keys' not in self.tables:
            raise ValueError(f'cannot render join syntax between {t1} and {table} - table configuration does not include any foreign keys')
        
        if table in self.tables['foreign_keys'] and t1 in self.tables['foreign_keys'][table]:
            return f'inner join {self._table_alias(t1)} on {t1[0]}.{self.tables["foreign_keys"][table][t1]} = {table[0]}.{t1[0:-1]}_{self.tables["foreign_keys"][table][t1]}'
        elif t1 in self.tables['foreign_keys'] and table in self.tables['foreign_keys'][t1]:
            return f'inner join {self._table_alias(t1)} on {t1[0]}.{table[0:-1]}_{self.tables["foreign_keys"][table][t1]} = {table[0]}.{self.tables["foreign_keys"][table][t1]}'
        else:
            raise ValueError(f'cannot render join syntax between {t1} and {table} - foreign key configuration does not associate the two')
    
    def _parse_param_to_stmt(self, param):
        op = "="

        if param[-4:] == "__gt":
            op = ">"
            param = param[0:-4]
        elif param[-4:] == "__lt":
            op = "<"
            param = param[0:-4]
        elif param[-5:] == "__gte":
            op = ">="
            param = param[0:-5]
        elif param[-5:] == "__lte":
            op = "<="
            param = param[0:-5]

        return f'{param} {op} ?'
    
    def _select(self, table, cols=None, joins=[], join_cols=False, where={}, order_by=None):

        if not cols:
            cols = self._select_cols(table)

        if join_cols:
            for j in joins:
                cols.extend(self._select_cols(j))

        response = _response()

        try:
            logger.debug(f'selecting {table} {cols} where {where}')
            params = ()
            where_stmt = ''
            if len(where.keys()) > 0:
                where_stmt = 'where ' + ' and '.join([ self._parse_param_to_stmt(w) for w in where.keys() ])
                params = tuple([ where[w] for w in where ])
            query = f'select {",".join(cols)} from {self._table_alias(table)} {" ".join([ self._table_join(j, table) for j in joins ])} {where_stmt} '
            if order_by:
                query = f'{query} order by {order_by}'

            with self.cursor() as cur:
                logger.info(f'query: {query} params: {params}')
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

        response = _response()

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

        response = _response()

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

        response = _response()

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
