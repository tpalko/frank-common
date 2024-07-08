import sys 
import cowpy
import importlib
import gc 
import simplejson as json 
from datetime import datetime 
from pytz import timezone 
from frank.database.database import Database 

logger = cowpy.getLogger()

# COLUMN_TYPE_MAP = {
#     'json': json,
#     'string': str,
#     'int': int,
#     'float': float,
#     'datetime': datetime,
#     'identity': int
# }

class Column(object):
    col_type = None 
    val = None 

    def set_val(self, val):
        self.val = val 
    
    def __repr__(self):
        return str(self.val)

    def __getitem__(self, key):
        if self.col_type == 'json':
            return json.loads(self.val)[key]
        return self.val[key]

    def __setitem__(self, key, val):
        if self.col_type == 'json':
            parsed = json.loads(self.val)
            parsed[key] = val 
            self.set_val(json.dumps(parsed))
            return 
        self.val[key] = val 

class JsonColumn(Column):
    col_type = 'json'

class StringColumn(Column):
    col_type = 'string'

class IntColumn(Column):
    col_type = 'int'

class BoolColumn(Column):
    col_type = 'bool'

class FloatColumn(Column):
    col_type = 'float'

class DateTimeColumn(Column):
    col_type = 'datetime'
    mark = None 

    def __init__(self, *args, **kwargs):
        super().__init__()
        if 'mark' in kwargs:
            self.mark = kwargs['mark']

    def timestamp(self, operation):
        logger.debug(f'getting timestamp for DateTimeColumn: self.val={self.val}, operation={operation}, self.mark={self.mark}')
        if self.val is None:
            if (operation == "insert" and self.mark == 'create') or (operation in ["insert", "update"] and self.mark == 'update'):
                return datetime.now(timezone('UTC'))
        return self.val

class IdentityColumn(Column):
    col_type = 'identity'

class ForeignKey(Column):
    col_type = 'foreign_key'
    to = None 

    def __init__(self, *args, **kwargs):
        super().__init__()
        # -- assume 'to' means to + _id is a column here
        # -- so find in To where 'id' is to_id
        # -- and that's to_set
        if 'to' in kwargs:
            self.to = kwargs['to']
            # if type(self.to) == str:
            #     logger.debug(f'{self.to}')
            #     logger.debug(f'{self}')
            #     logger.debug(f'{gc.get_referrers(self)}')
                # models_module_name = gc.get_referrers(self)[0]['__module__']
                # models_module = importlib.import_module(models_module_name)
                # self.to = models_module.__getattribute__(self.to)                

class BaseMeta:        
    identity_col = None
    built_in_cols = {
        'created_at': None, 
        'updated_at': None
    }
    table = None 
    user_def_col_names = None
    insert_cols = None
    select_cols = None
    # db: Database = None        

    def __init__(self, *args, **kwargs):
        self.user_def_col_names = []
        self.insert_cols = []
        self.select_cols = [] 

class BaseModel:

    _meta: BaseMeta = None 

    def __init__(self, *args, **kwargs):

        logger.debug(f'making new {self.__class__.__name__}')
        self.__class__._meta = BaseMeta()
        
        self.__class__._meta.identity_col = IdentityColumn()

        self.__class__._meta.built_in_cols['created_at'] = DateTimeColumn(mark='create')
        self.__class__._meta.built_in_cols['updated_at'] = DateTimeColumn(mark='update')

        # self.__class__._meta.db = Database.getInstance()
        self.__class__._meta.table = self.__class__.__name__.lower() + "s"        

        self.__class__._meta.user_def_col_names = [ a for a in self.__dir__() if isinstance(self.__getattribute__(a), Column) ]
        
        self.__class__._meta.insert_cols = [ f'{name}_id' if isinstance(self.__getattribute__(name), ForeignKey) else name for name in self.__class__._meta.user_def_col_names ]
        self.__class__._meta.insert_cols.extend(self.__class__._meta.built_in_cols.keys())

        self.__class__._meta.select_cols = [ 'id' ]
        self.__class__._meta.select_cols.extend([ name for name in self.__class__._meta.insert_cols ])
        
        logger.debug(f'checking {kwargs} in id, {self.__class__._meta.built_in_cols.keys()}, or {self.__class__._meta.user_def_col_names}')
        for k in kwargs:
            if k == 'id':
                self._meta.identity_col.set_val(kwargs[k])            
            elif k in self._meta.built_in_cols.keys():
                found_col = self._meta.built_in_cols[k]
                logger.debug(f'found col {type(found_col)} for {k}, setting with {kwargs[k]}')
                found_col.set_val(kwargs[k])
            elif k in self._meta.user_def_col_names:                
                self.__getattribute__(k).set_val(kwargs[k])
            # else:
            #     self.check_set_fk(k, kwargs[k])
        
        ########################################### DATABASE ###########################################
        #####
        ##
                                
        # dbConfig = {
        #     'host': None,
        #     'user': None,
        #     'password': None,
        #     'name': None 
        # }
        
        # for k in kwargs:
        #     logger.debug("Setting %s -> %s" % (k, kwargs[k]))
        #     if k in dbConfig.keys():
        #         dbConfig[k] = kwargs[k]
        
        # self._meta.db = Database(
        #     config=DatabaseConfig.NewMariadb(**dbConfig),
        #     models=[self]
        # )

        ##
        #####        
        ########################################### DATABASE ###########################################
    
    def check_set_fk(self, name, val):
        checked = False 
        logger.debug(f'checking FK on {name}/{val}')
        if name[-3:] == "_id":
            try:
                
                pot_fk = self.__getattribute__(name[0:-3])
                logger.debug(f'found {pot_fk.to} for {name[0:-3]}')
                logger.debug(f'{pot_fk.to()._meta.table}')
                assoc_recs = Database.getInstance()._select(pot_fk.to()._meta.table, cols=pot_fk.to()._meta.select_cols, where={'id': val})
                if len(assoc_recs) > 1:
                    raise Exception(f'{pot_fk.to.__class__.__name__} from {self.__class__.__name__} {name}={val} has multiple records')
                if len(assoc_recs) == 1:
                    self.__getattribute__(name[0:-3]).set_val(assoc_recs[0])                
                
                checked = True 
            except:
                logger.exception()
        return checked
    
    def __setattr__(self, name, val):
        # logger.debug(f'setting {name} -> {val}')
        if name in ["_meta"]:
            super().__setattr__(name, val)
        else:
            if not self.check_set_fk(name, val):
                self.__getattribute__(name).set_val(val)
            
    def __getattribute__(self, name):          
        if name[0:1] == "_":            
            return super().__getattribute__(name)
        if name == 'id':            
            return self._meta.identity_col.val
        if name in self._meta.built_in_cols.keys():            
            return self._meta.built_in_cols[name].val        
        return super().__getattribute__(name)

    def __repr__(self):
        vald = self.val_dict()
        return ", ".join({ f'{k}:{vald[k]}' for k in self.val_dict() }) # ", ".join([ f'{k}: {self.__getattribute__(k).val}' for k in self.val_dict() if self.__getattribute__(k) is not None ])

    # @classmethod 
    # def register_db(cls, db: Database):
    #     cls._meta.db = db 

    @classmethod 
    def first(cls, **kwargs):
        records = cls.get(**kwargs)
        if len(records) > 0:
            return records[0]
        return None

    @classmethod 
    def all(cls):
        return cls.get()

    @classmethod
    def get(cls, **kwargs):
        logger.debug(cls._meta)
        records = Database.getInstance()._select(cls.__name__.lower() + "s", cols=cls._meta.select_cols, where=kwargs)        
        # records = cls._meta.db._select(cls.__name__.lower() + "s", where=kwargs)
        logger.debug(records)
        for r in records:
            for field in r:
                if type(r[field]) == datetime:
                    r[field] = datetime.strftime(r[field], "%Y-%m-%d %H:%M:%S")        
        logger.debug(records)
        logger.debug(cls)
        typed_records = [ cls(**r) for r in records ]
        return typed_records 
    
    def val_dict(self, operation=None):
        user_col_vals = { f'{k}' if isinstance(self.__getattribute__(k), ForeignKey) else k: 
                         self.__getattribute__(k).val if not isinstance(self.__getattribute__(k).val, Column) else self.__getattribute__(k).val._meta.identity_col.val 
                         for k in self._meta.user_def_col_names }
        for builtin in self._meta.built_in_cols:
            user_col_vals[builtin] = self._meta.built_in_cols[builtin].timestamp(operation=operation)
        logger.debug(f'val dict giving {user_col_vals.keys()}')
        logger.debug({ k: type(self.__getattribute__(k)) for k in user_col_vals.keys() })
        return user_col_vals

    def upsert(self, **kwargs):
        dbrecords = []
        if len(kwargs) > 0:
            dbrecords = Database.getInstance()._select(self._meta.table, cols=self._meta.select_cols, where={ k: kwargs[k] for k in kwargs })
        if len(dbrecords) == 1:
            dbrecords[0].update(self.val_dict(operation='update'))
            Database.getInstance()._update(self._meta.table, set=dbrecords[0], where={'id':dbrecords[0]['id']})
            self._meta.identity_col.set_val(dbrecords[0]['id'])
        elif len(dbrecords) == 0:
            self._meta.identity_col.set_val(Database.getInstance()._insert(table=self._meta.table, cols=self._meta.insert_cols, **self.val_dict(operation='insert')))
        else:
            raise Exception(f'upserting {self.__class__.__name__} with {kwargs} matched {len(dbrecords)} records')
        
    def save(self):
        upsert_kwargs = {}
        if self._meta.identity_col.val is not None:
            upsert_kwargs['id'] = self._meta.identity_col.val
        self.upsert(**upsert_kwargs)   
