import os
import sys 
import traceback 
import cowpy 
from contextlib import contextmanager
import sqlite3 
import mariadb
from enum import Enum
from datetime import datetime 
import simplejson as json

logger = cowpy.getLogger()

class DbType(Enum):
    MariaDB = 0
    Sqlite = 1

class Dialect(Enum):
    AUTO_INCREMENT = 0
    ENGINE = 1
    FLOAT = 2
    CHAR = 3
    GET_CREATE_TABLE = 4
    INTEGER = 5
    JSON_TYPE = 6

db_dialect = {
    DbType.Sqlite: {
        Dialect.AUTO_INCREMENT: 'autoincrement',            
        Dialect.ENGINE: '',
        Dialect.FLOAT: 'float',
        Dialect.CHAR: 'char',
        Dialect.GET_CREATE_TABLE: 'select sql from sqlite_master where name = ?',
        Dialect.INTEGER: 'int',
        Dialect.JSON_TYPE: 'json'
    },
    DbType.MariaDB: {
        Dialect.AUTO_INCREMENT: 'auto_increment',            
        Dialect.ENGINE: 'engine=innodb default charset=utf8',
        Dialect.FLOAT: 'decimal',
        Dialect.CHAR: 'varchar',
        Dialect.GET_CREATE_TABLE: 'show create table',
        Dialect.INTEGER: 'int(11)',
        Dialect.JSON_TYPE: 'json'
    }
}

db_providers = {
    DbType.Sqlite: lambda config: sqlite3.connect(config.filename),
    DbType.MariaDB: lambda config: mariadb.connect(host=config.host, user=config.user, password=config.password, database=config.name)
}

# -- as of this writing (march 2024), TYPE_MAPPINGS is only referenced when calling create_table
TYPE_MAPPINGS = {
    str: lambda config: db_dialect[config.dbType][Dialect.CHAR],
    int: lambda config: db_dialect[config.dbType][Dialect.INTEGER],
    datetime.date: lambda config: 'datetime',
    bool: lambda config: 'bool', # tinyint(1)',
    float: lambda config: db_dialect[config.dbType][Dialect.FLOAT],
    json: lambda config: db_dialect[config.dbType][Dialect.JSON_TYPE]
}

DIALECT_MAPPINGS = {
    Dialect.GET_CREATE_TABLE: lambda config: db_dialect[config.dbType][Dialect.GET_CREATE_TABLE]
}

def get_db_dialect(lowercase_name):
    for d in DbType:
        if d.name.lower() == lowercase_name:
            return db_dialect[d]
    return None 

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

class Manager(object):
    pass 

class Database(object):

    conn = None 
    cfg = None 
    models = None 
    insert_cols = None 
    last_response = None 

    __instance = None 

    @staticmethod
    def createInstance(**kwargs):
        Database(**kwargs)

    def __init__(self, *args, **kwargs):

        if Database.__instance is not None:
            return 

        required_keys = ['config', 'models']
        valid = all([ k in kwargs for k in required_keys ])

        if not valid:
            raise Exception(f'Please provide required keys: {required_keys}')
        
        self.cfg = kwargs['config']
        self.models = kwargs['models']

        if not isinstance(self.cfg, DatabaseConfig):
            raise Exception("Provided config is not DatabaseConfig")
        
        print(f'models: {self.models}')
        print(f'{self.models[0]._meta}')
        print(f'{self.models[0]._meta.table}')
        self.models_by_table_name = { m._meta.table: m for m in self.models }
        print(f'models by table name: {self.models_by_table_name}')
        self.table_names = [ m._meta.table for m in self.models ]
        self.insert_cols = { m._meta.table: m._meta.insert_cols for m in self.models }   
             
        # self.insert_cols = { m: [ c['name'] for c in self.tables['models'][m] ] for m in self.tables['models'].keys() }

        # for m in self.insert_cols.keys():
        #     self.insert_cols[m].extend(self._get_timestamp_keys('insert'))

        # -- luxury auto-table creation 
        # with self.cursor() as cur:
        #     for tablename in self.table_names:
        #         try:
        #             cur.execute(f'select 1 from {tablename}')
        #         except:
        #             logger.exception()
        #             logger.warning(f'table {tablename} not found, creating now')
        #             create_table_sql = f'create table {tablename} {self.create_table(tablename)} {db_dialect[self.cfg.dbType][Dialect.ENGINE]};'
        #             logger.warning(create_table_sql)
        #             try:
        #                 cur.execute(create_table_sql)
        #             except:
        #                 logger.exception()
                
        for model in self.models:
            print(f'registering {model.__name__}')
            model.register_db(self)

        Database.__instance = self 

    def __repr__(self):
        return str(self.__dict__)

    def dump(self):
        '''Writes out all database records to stdout'''
        dump = {}        
        for model in self.models:
            dump[model._meta.table] = self._select(model._meta.table)
        # for table in self.table_names:
        #     dump[table] = self._select(table, [ c['name'] for c in self.tables['models'][table] ])
        logger.debug({ t: [ r for r in dump[t]['data'] ] for t in dump })
        return dump
    
    def parse_type(self, column_name, value):
        if value is not None:
            if column_name[-3:] == '_at' or column_name[-10:] == '_timestamp':
                parsed = value 
                if type(parsed) != datetime:
                    try:
                        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                    except:
                        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return parsed
            elif column_name[0:3] == 'is_':
                return bool(value)
        return value 
    
    def dict_factory(self, cursor, row):
        # logger.debug(f'dicting {cursor} {row}')
        return { col[0]: self.parse_type(col[0], row[idx]) for idx,col in enumerate(cursor.description) }
    
    def _column_type(self, col):
        colType = TYPE_MAPPINGS[col["type"]](self.cfg)
        size = ''
        if 'size' in col:
            size = f'({col["size"]})'
        elif col["type"] == str:
            colType = 'text'
        return f'{colType}{size}'
    
    def _column_def(self, col):
        return f'{col["name"]} {self._column_type(col)}{" null" if "null" in col and col["null"] == True else ""}'
    
    def create_table(self, tablename):
        return f'({self.tables["base"]["primary_key"]} {db_dialect[self.cfg.dbType][Dialect.INTEGER]} PRIMARY KEY {db_dialect[self.cfg.dbType][Dialect.AUTO_INCREMENT]}, {", ".join([ self._column_def(col) for col in self.tables["models"][tablename] ])}{"," if len(self.tables["base"]["timestamps"]) > 0 else ""}{", ".join([ t + " datetime" for t in self.tables["base"]["timestamps"] ]) if len(self.tables["base"]["timestamps"]) > 0 else ""})'
    
    @contextmanager
    def get_cursor(self):
        '''Generic cursor manifestation, dialect fallback, nothing else'''
        try:
            # -- some cursors will have their own context 
            # -- e.g. mariadb
            with self.conn.cursor() as c:
                yield c 
        except TypeError as te:

            yield self.conn.cursor() 
            
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
    
    def init_db(self):
        '''Checks database table schema against table schema definition, creating missing tables'''        

        for table in self.table_names:
            
            create_table_cmd = f'CREATE TABLE {table} {self.create_table(table)} {db_dialect[self.cfg.dbType][Dialect.ENGINE]}' 
            # f'CREATE TABLE "{table}" {TABLES[table](self.config)}'

            try:
                sql = None 
                # -- schemachecker 
                with self.cursor() as c:
                    get_create_table_sql = DIALECT_MAPPINGS[Dialect.GET_CREATE_TABLE](self.cfg)
                    logger.debug(f'executing {get_create_table_sql} {table}')
                    c.execute(f'{get_create_table_sql} {table}')
                    # c.execute(f'select sql from sqlite_master where name = ?', (table,))
                    firstrow = c.fetchone()
                    if not firstrow:
                        raise sqlite3.OperationalError("fetchone returned nothing")
                    sql = firstrow[1]
                if sql:
                    logger.success(f'Captured {table} schema: {sql}')

                    sql = " ".join([ s.strip() for s in sql.split(' ') if s.strip() != '' ]).replace('\'', '').replace('`', '').replace('"', '').replace('  ', ' ').lower()
                    create_table_cmd = " ".join([ s.strip() for s in create_table_cmd.split(' ') if s.strip() != '' ]).replace('\'', '').replace('"', '').replace('  ', ' ').lower()

                    if sql != create_table_cmd:
                        logger.warning(f'WARNING: {table} schema in database does not match schema in code')
                        logger.warning(f'Database:\t{sql}')
                        logger.warning(f'Code:\t\t{create_table_cmd}')
                    else:
                        logger.success(f'Table schema OK')
            except (BcktDatabaseException, sqlite3.OperationalError, mariadb.ProgrammingError) as oe:
                logger.error(f'Failed to read from table {table}')
                logger.error(oe)
                logger.warning(f'Creating table {table}..')
                with self.cursor() as c:                    
                    c.execute(create_table_cmd)
            except:
                logger.error(f'Something else failed testing table {table}')
                logger.exception()
                raise 

    ### ORIGINAL 
    # @contextmanager
    # def cursor(self):
    #     conn = mariadb.connect(host=self.cfg.host, user=self.cfg.user, password=self.cfg.password, database=self.cfg.name)
    #     with conn.cursor() as cur:
    #         yield cur 
    #     conn.commit()

    def raw(self, query, params=()):

        records = []
        with self.cursor() as cur:
            cur.execute(query, params)
            records = cur.fetchall()
        return records

    def _select_cols(self, table):
        def_cols = [ f'{table[0]}.{col}' for col in self.models_by_table_name[table]._meta.select_cols ]
        return [ f'{table[0]}.id', *def_cols ]
    
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
        elif param[-7:] == "__ilike":
            op = "like"
            param = param[0:-7]

        return f'{param} {op} ?'

    TIMESTAMP_LOOKUP = {
        'insert': {
            'created_at': lambda: datetime.utcnow(),
            'updated_at': lambda: datetime.utcnow(),                
        },
        'update': {                
            'updated_at': lambda: datetime.utcnow(),                
        },
        'delete': {                
            'deleted_at': lambda: datetime.utcnow(),                
        }
    }

    def _get_timestamp_keys(self, action):

        return [ t for t in self.TIMESTAMP_LOOKUP[action].keys() if t in self.tables['base']['timestamps'] ]
        
    # def _get_timestamp_values(self, action):

    #     return [ self.TIMESTAMP_LOOKUP[action][t]() for t in self.TIMESTAMP_LOOKUP[action].keys() if t in self.tables['base']['timestamps'] ]
    
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
                logger.debug(f'query: {query} params: {params}')
                cur.execute(query, params)
                all_records = cur.fetchall()
                if self.cfg.dbType == DbType.MariaDB:                    
                    all_records = [ self.dict_factory(cur, row=r) for r in all_records ]                
                response['data'] = all_records

            response['success'] = True 

        except:
            logger.exception()
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'
            raise 
        
        self.last_response = response 

        return self.last_response['data']

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
            raise 
        
        self.last_response = response 
        return self.last_response['success']

    def _delete(self, table, id):

        response = _response()

        try:
            logger.debug(f'deleting {table} {id}')
            query = f'delete from {table} where id = ?'
            with self.cursor() as cur:
                cur.execute(query, (id,))            
            response['success'] = True 
        except:
            logger.exception()
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'            
        
        self.last_response = response 
        return self.last_response['success']

    def _upsert(self, table, where={}, *params):

        found_rows = self.db._select('images', where=where)

        if len(found_rows) > 0:
            return found_rows[0]
        else:            
            record_id = self.db._insert('images', *params)                    
            return self._select(table, where={'id': record_id})

    def _insert(self, table, *params):

        insert_params = []

        logger.debug(params)

        # -- if we got a dict, unpack all the values
        if len(params) == 1 and type(params[0]) == dict:
            logger.debug(f'undictfying the params')
            params = params[0]
            insert_params = [ params[k] for k in params ]
        else:
            insert_params = list(params)

        # insert_params.extend(self._get_timestamp_values('insert'))

        insert_params = tuple(insert_params)

        response = _response()

        try:
            logger.debug(f'inserting {table} {insert_params}')
            query = f'insert into {table} ({",".join(self.models_by_table_name[table]._meta.insert_cols)}) values({",".join([ "?" for p in self.models_by_table_name[table]._meta.insert_cols ])})'
            logger.debug(query)
            logger.debug(insert_params)
            with self.cursor() as cur:
                cur.execute(query, insert_params)    
                response['data']['insert_id'] = cur.lastrowid
            response['success'] = True 

        except:
            logger.exception()
            err_type = sys.exc_info()[0]
            message = sys.exc_info()[1]
            response['message'] = f'{err_type}: {message}'            
            raise 

        self.last_response = response 
        return self.last_response['data']['insert_id']
