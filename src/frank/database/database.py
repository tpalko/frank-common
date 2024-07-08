import os
import sys 
import traceback 
import cowpy 
from enum import Enum
from contextlib import contextmanager
from datetime import datetime 
from frank.database.config import DatabaseConfig, DbType
from frank.database.dialect import db_dialect_mappings, get_db_connection

logger = cowpy.getLogger()

def _response(success=False, message='', data={}):
    return {
        'success': success,
        'message': message,
        'data': data 
    }

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
        '''Triggers the creation of the Database singleton. **Uses environment variables but accepts kwargs to override'''
        Database(**kwargs)

    @staticmethod
    def getInstance():
        '''Creates if DNE and returns the Database singleton. **Relies on environment variables**'''
        Database()
        # -- alternatively.. 
        # raise Exception(f'Database has not been initialized')
        return Database.__instance
    
    def __init__(self, *args, **kwargs):

        if Database.__instance is not None:
            return 

        # required_keys = ['config'] # , 'models']
        # valid = all([ k in kwargs for k in required_keys ])
        # if not valid:
        #     raise Exception(f'Please provide required keys: {required_keys}')
        
        self.cfg = DatabaseConfig(**kwargs['config'] if 'config' in kwargs else {})
        self.models = kwargs['models'] if 'models' in kwargs else []

        if not isinstance(self.cfg, DatabaseConfig):
            raise Exception("Provided config is not DatabaseConfig")
        
        # print(f'models: {self.models}')
        # print(self.models[0].__name__)
        # print(f'{self.models[0]._meta}')
        # print(f'{self.models[0]._meta.table}')

        # -- TODO: model here should have ._meta.table but at this point it's not accessible
        # -- so we duplicate the math done in BaseModel to get the table name based on the class name
        # -- TODO: another issue here is that we both register each BaseModel with ourself (Database, self)
        # -- _so that_ BaseModel instances don't have to have a Database instance to pass into each .get() or .save()
        # -- the BaseModel already has it.. but Database is also being made aware of the BaseModels in this lookup 
        # -- table, which is useful for select/insert col lookups per table
        self.models_by_table_name = { m.__name__.lower() + "s": m for m in self.models }
        
        # print(f'models by table name: {self.models_by_table_name}')
        
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
        #             create_table_sql = f'create table {tablename} {self.create_table(tablename)} {db_dialect_mappings[get_db_dialect(self.cfg.dbType)][Dialect.ENGINE]};'
        #             logger.warning(create_table_sql)
        #             try:
        #                 cur.execute(create_table_sql)
        #             except:
        #                 logger.exception()
                
        for model in self.models:
            logger.debug(f'registering {model.__name__}')
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
        return f'({self.tables["base"]["primary_key"]} {db_dialect_mappings[self.cfg.dbType][Dialect.INTEGER]} PRIMARY KEY {db_dialect_mappings[self.cfg.dbType][Dialect.AUTO_INCREMENT]}, {", ".join([ self._column_def(col) for col in self.tables["models"][tablename] ])}{"," if len(self.tables["base"]["timestamps"]) > 0 else ""}{", ".join([ t + " datetime" for t in self.tables["base"]["timestamps"] ]) if len(self.tables["base"]["timestamps"]) > 0 else ""})'
    
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

        self.conn = get_db_connection(self.cfg)        
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
            
            create_table_cmd = f'CREATE TABLE {table} {self.create_table(table)} {db_dialect_mappings[self.cfg.dbType][Dialect.ENGINE]}' 
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
                        # sqlite3.OperationalError
                        raise Exception("fetchone returned nothing")
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
                # -sqlite3.OperationalError, mariadb.ProgrammingError
            except (BcktDatabaseException) as oe:
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
            cols = self.models_by_table_name[table]._meta.select_cols

        if join_cols:
            for j in joins:
                cols.extend(self._select_cols(j))

        response = _response()

        try:
            logger.debug(f'selecting from {table} {cols} where {where}')
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

    def _insert(self, table, cols=[], **params):

        insert_params = []

        logger.debug(params)

        # -- if we got a dict, unpack all the values
        if len(params) == 1 and type(params[0]) == dict:
            logger.debug(f'undictfying the params')
            params = params[0]
            insert_params = [ params[k] for k in params ]
        else:
            insert_params = [ params[p] for p in params ] # list(params)

        insert_params = [ p.name if isinstance(p, Enum) else p for p in insert_params ]
        # insert_params.extend(self._get_timestamp_values('insert'))

        insert_params = tuple(insert_params)

        response = _response()

        try:
            logger.debug(f'inserting {table} {insert_params}')
            # query = f'insert into {table} ({",".join(self.models_by_table_name[table]._meta.insert_cols)}) values({",".join([ "?" for p in self.models_by_table_name[table]._meta.insert_cols ])})'
            query = f'insert into {table} ({",".join(cols)}) values({",".join([ "?" for p in cols ])})'
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
