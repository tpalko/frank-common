import sys 
import simplejson as json 
from datetime import datetime 
from pytz import timezone 
from frank.database.database import Database 

COLUMN_TYPE_MAP = {
    'json': json,
    'string': str,
    'int': int,
    'float': float,
    'datetime': datetime,
    'identity': int
}

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
        if (operation == "insert" and self.mark == 'create') or (operation == "update" and self.mark == 'update'):
            return datetime.now(timezone('UTC'))
        return self.val

class IdentityColumn(Column):
    col_type = 'identity'

class BaseMeta:        
    identity_col = IdentityColumn()
    built_in_cols = {
        'created_at': DateTimeColumn(mark='create'), 
        'updated_at': DateTimeColumn(mark='update')
    }
    table = None 
    user_def_col_names = []

class BaseModel(object):

    class _meta:
        identity_col = IdentityColumn()
        built_in_cols = {
            'created_at': DateTimeColumn(), 
            'updated_at': DateTimeColumn()
        }
        table = None 
        user_def_col_names = []
        insert_cols = []
        select_cols = []
        db = None         

    def __init__(self, *args, **kwargs):
        
        self._meta.table = self.__class__.__name__.lower() + "s"        
        self._meta.user_def_col_names.extend([ a for a in self.__dir__() if isinstance(self.__getattribute__(a), Column) ])        
        
        self._meta.insert_cols = [ name for name in self._meta.user_def_col_names ]
        self._meta.insert_cols.extend(self._meta.built_in_cols.keys())

        self._meta.select_cols = [ 'id' ]
        self._meta.select_cols.extend([ name for name in self._meta.insert_cols ])
        
        for k in kwargs:
            if k == 'id':
                self._meta.identity_col.set_val(kwargs[k])
            if k in self._meta.user_def_col_names:
                self.__getattribute__(k).set_val(kwargs[k])
            if k in self._meta.built_in_cols.keys():
                self._meta.built_in_cols[k].set_val(kwargs[k])
        
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
        
    def __setattr__(self, name, val):
        # print(f'setting {name} -> {val}')
        if name in ["_meta"]:
            super().__setattr__(name, val)
        else:
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
        return json.dumps(self.val_dict())

    @classmethod 
    def register_db(cls, db: Database):
        cls._meta.db = db 

    @classmethod 
    def first(cls, **kwargs):
        records = cls.get(**kwargs)
        if len(records) > 0:
            return records[0]
        return None

    @classmethod
    def get(cls, **kwargs):
        records = cls._meta.db._select(cls.__name__.lower() + "s", where=kwargs)
        for r in records:
            for field in r:
                if type(r[field]) == datetime:
                    r[field] = datetime.strftime(r[field], "%Y-%m-%d %H:%M:%S")        
        typed_records = [ cls(**r) for r in records ]
        return typed_records 
    
    def val_dict(self, operation=None):
        user_col_vals = { k: self.__getattribute__(k).val for k in self._meta.user_def_col_names }
        for builtin in self._meta.built_in_cols:
            user_col_vals[builtin] = self._meta.built_in_cols[builtin].timestamp(operation)

    def upsert(self, **kwargs):
        dbrecords = []
        if len(kwargs) > 0:
            dbrecords = self._meta.db._select(self._meta.table, where={ k: kwargs[k] for k in kwargs })
        if len(dbrecords) == 1:
            dbrecords[0].update(self.val_dict(operation='update'))
            self._meta.db._update(self._meta.table, set=dbrecords[0], where={'id':dbrecords[0]['id']})
        elif len(dbrecords) == 0:
            self._meta.identity_col.set_val(self._meta.db._insert(self._meta.table, self.val_dict(operation='insert')))
        else:
            raise Exception(f'upserting {self.__class__.__name__} with {kwargs} matched {len(dbrecords)} records')
        
    def save(self):
        upsert_kwargs = {}
        if self._meta.identity_col.val is not None:
            upsert_kwargs['id'] = self._meta.identity_col.val
        self.upsert(**upsert_kwargs)   
