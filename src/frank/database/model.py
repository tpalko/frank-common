# import sys 
import cowpy
# import importlib
# import gc 
from datetime import datetime 

from frank.database.database import Database 
from frank.database.column import Column, DateTimeColumn, ForeignKey, IdentityColumn
from frank.database.query import Query 

logger = cowpy.getLogger()

# COLUMN_TYPE_MAP = {
#     'json': json,
#     'string': str,
#     'int': int,
#     'float': float,
#     'datetime': datetime,
#     'identity': int
# }



class BaseMeta:        
    table = None 
    joins = None
    identity_col = None
    built_in_cols = None
    user_cols = None
    insert_col_names = None
    select_col_names = None

    def __init__(self, *args, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

class InstanceMeta:

    identity_col = None 
    built_in_cols = None 
    user_cols = None 
    built_in_col_lookup = None 
    user_col_lookup = None 

class BaseModel:

    _meta: BaseMeta = None 
    _instancemeta: InstanceMeta = None 

    def __init__(self, *args, **kwargs):
        
        # logger.debug(f'BaseModel: instantiating new {self.__class__.__name__}')        

        if self.__class__._meta is None:
                
            column_type_attrs = [ attr_name
                for attr_name in self.__dir__() 
                if isinstance(self.__getattribute__(attr_name), Column)
            ]

            built_in_cols = [
                {
                    'name': 'created_at',
                    'type': DateTimeColumn,
                    'kwargs': {'mark': 'create'}
                },
                {
                    'name': 'updated_at',
                    'type': DateTimeColumn,
                    'kwargs': {'mark': 'update'}
                }
            ]
                    
            user_cols = [
                {
                    'name': a, 
                    'type': type(self.__getattribute__(a)),
                    'kwargs': self.__getattribute__(a).kwargs
                } for a in column_type_attrs
            ]
            
            insert_col_names = [ 
                f'{col["name"]}_id' if isinstance(self.__getattribute__(col["name"]), ForeignKey) else col["name"] 
                for col in user_cols 
            ]
            insert_col_names.extend([ f'{col["name"]}' for col in built_in_cols ])

            select_col_names = [ 'id' ]
            select_col_names.extend(insert_col_names)
            
            table = self.__class__.__name__.lower() + "s"
            alias = f'{table} {table[0]}'

            self.__class__._meta = BaseMeta(
                table=table, 
                alias=alias,
                identity_col={'name': 'id', 'type': IdentityColumn},
                built_in_cols=built_in_cols,
                user_cols=user_cols,
                insert_col_names=insert_col_names,
                select_col_names=select_col_names,
                joins=[]
            )

        self._instancemeta = InstanceMeta()

        self._instancemeta.identity_col = { 'name': self.__class__._meta.identity_col["name"], 'col': self.__class__._meta.identity_col["type"]() }
        self._instancemeta.built_in_cols = [ { 'name': col["name"], 'col': col["type"](**col["kwargs"]) } for col in self.__class__._meta.built_in_cols ]
        self._instancemeta.user_cols = [ { 'name': col["name"], 'col': col["type"](**col["kwargs"]) } for col in self.__class__._meta.user_cols ]
        self._instancemeta.built_in_col_lookup = { col['name']: col for col in self._instancemeta.built_in_cols }
        self._instancemeta.user_col_lookup = { col['name']: col for col in self._instancemeta.user_cols }

        # logger.debug(f'looking to set the value of each of {kwargs} as identity id, built-in {built_in_cols.keys()}, or user-defined column {user_def_col_names}')
        for k in kwargs:
            if k == self._instancemeta.identity_col['name']:
                self._instancemeta.identity_col['col'].set_val(kwargs[k])            
            elif k in self._instancemeta.built_in_col_lookup.keys():
                self._instancemeta.built_in_col_lookup[k]['col'].set_val(kwargs[k])
            elif k in self._instancemeta.user_col_lookup.keys():
                self._instancemeta.user_col_lookup[k]['col'].set_val(kwargs[k])
        
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
        
        # self.__class__._meta.db = Database(
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

        if name in ['_instancemeta']:
            super().__setattr__(name, val)

        elif name in self._instancemeta.built_in_col_lookup:
            self._instancemeta.built_in_col_lookup[name]['col'].set_val(val)
        elif name in self._instancemeta.user_col_lookup:
            self._instancemeta.user_col_lookup[name]['col'].set_val(val)        
        elif not self.check_set_fk(name, val):
            self.__getattribute__(name).set_val(val)
        else:
            logger.warn(f'__setattr__ confused with {name}={val}')  

    def __getattribute__(self, name):          

        if name in ['__class__', '_instancemeta']:
            return super().__getattribute__(name)
        
        if name in ['get', 'join', 'all']:
            return Query.__getattribute__(Query, name)
    
        if name in ['_id_col_val']:
            return self._instancemeta.identity_col['col'].val
        
        if self._instancemeta:
            if name in self._instancemeta.built_in_col_lookup:
                return self._instancemeta.built_in_col_lookup[name]['col'].val
            elif name in self._instancemeta.user_col_lookup:
                return self._instancemeta.user_col_lookup[name]['col'].val
            elif name == self._instancemeta.identity_col['name']:
                return self._instancemeta.identity_col['col'].val
        
        return super().__getattribute__(name)
    
        # if name[0:1] == "_":            
        #     return super().__getattribute__(name)
        # if name == 'id':            
        #     return self.__class__._meta.identity_col.val
        # if name in self.__class__._meta.built_in_cols.keys():            
        #     return self.__class__._meta.built_in_cols[name].val        
        # return super().__getattribute__(name)

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
    def join(cls, **kwargs):
        return Query(cls, **kwargs)        
            
    @classmethod
    def get(cls, **kwargs):
        
        logger.debug(f'calling get() from {cls.__name__}: select cols: {cls._meta.select_col_names}, where: {kwargs}')
        
        records = Database.getInstance()._select(cls, joins=cls._meta.joins, join_cols=False, cols=cls._meta.select_col_names, where=kwargs)        
        # records = cls._meta.db._select(table_name, where=kwargs)
        
        # logger.debug(records)
        
        for r in records:
            for field in r:
                if type(r[field]) == datetime:
                    r[field] = datetime.strftime(r[field], "%Y-%m-%d %H:%M:%S")        
        # logger.debug(records)
        typed_records = [ cls(**r) for r in records ]
        return typed_records 
    
    def val_dict(self, operation=None):
        # user_col_vals = { f'{k}_id' 
        #                     if isinstance(self.__getattribute__(k), ForeignKey) 
        #                     else k: 
        #                  self.__getattribute__(k).val 
        #                     if not isinstance(self.__getattribute__(k).val, Column) 
        #                     else self.__getattribute__(k).val._meta.identity_col.val 
        #                  for k in self.__class__._meta.user_cols }
        user_col_vals = { k['name']: self._instancemeta.user_col_lookup[k['name']]['col'].val for k in self.__class__._meta.user_cols }
        
        # for builtin in self.__class__._meta.built_in_cols:
        #     user_col_vals[builtin] = self.__class__._meta.built_in_cols[builtin].timestamp(operation=operation)
        
        user_col_vals.update({ builtin['name']: self._instancemeta.built_in_col_lookup[builtin['name']]['col'].timestamp(operation=operation) for builtin in self.__class__._meta.built_in_cols })
        
        # logger.debug(f'val dict giving {user_col_vals.keys()}')
        # logger.debug({ k: type(self.__getattribute__(k)) for k in user_col_vals.keys() })
        
        return user_col_vals

    def upsert(self, **kwargs):

        upsert_on = {}
        if 'on' in kwargs:
            on_fields = kwargs['on']
            if type(on_fields) == str:
                on_fields = on_fields.split(',')
            upsert_on = { o: self._instancemeta.user_col_lookup[o]['col'].val for o in on_fields }

        # - there are, maybe, interesting use cases here, calling upsert with any variety of column values on model instances in a variety of states
        # - for now, upsert is a general purpose concept narrowly focused to handle either a fresh save or a simple update to an existing record
        if self._id_col_val is not None and 'id' in upsert_on and self._id_col_val != upsert_on['id']:
            raise Exception(f'Don\'t try to trick me into cross-inserting. I already have an id, just call save.')
        
        # -- if we have an id but it wasn't included in the 'on', include it in the 'on'
        if self._id_col_val is not None and 'id' not in upsert_on:
            upsert_on['id'] = self._id_col_val

        logger.debug(f'upsert kwargs {upsert_on}')
        dbrecords = []        
        
        # if presented with any query, we look for a singular database record to update
        if len(upsert_on) > 0:
            dbrecords = Database.getInstance()._select(self.__class__, cols=self.__class__._meta.select_col_names, where=upsert_on)

        # if we find that singular record, update with our column vals
        if len(dbrecords) == 1:
            logger.debug(f'db record found: {dbrecords}')
            vals = self.val_dict(operation='update')
            logger.debug(f'updating db record with {vals}')
            dbrecords[0].update(vals)
            id_match = self._id_col_val or dbrecords[0]['id']
            Database.getInstance()._update(self.__class__, set=dbrecords[0], where={'id':id_match})
            # self._instancemeta.identity_col['col'].set_val(dbrecords[0]['id'])
            # -- the timestamps are the only values that possibly vary during this operation (dynamically set from val_dict)
            for builtin in [ c for c in self.__class__._meta.built_in_cols if 'mark' in c['kwargs'] and c['kwargs']['mark'] in ['create', 'update'] ]:
                self._instancemeta.built_in_col_lookup[builtin['name']]['col'].set_val(vals[builtin['name']])
        elif len(dbrecords) == 0:
            vals = self.val_dict(operation='insert')
            insert_response = Database.getInstance()._insert(
                table=self.__class__._meta.table, 
                cols=self.__class__._meta.insert_col_names, 
                **vals
            )
            self._instancemeta.identity_col['col'].set_val(insert_response)
            for builtin in [ c for c in self.__class__._meta.built_in_cols if 'mark' in c['kwargs'] and c['kwargs']['mark'] in ['create', 'update'] ]:
                self._instancemeta.built_in_col_lookup[builtin['name']]['col'].set_val(vals[builtin['name']])
        else:
            raise Exception(f'upserting {self.__class__.__name__} with {kwargs} matched {len(dbrecords)} records')
        
    def save(self):
        upsert_kwargs = {}
        if self._instancemeta.identity_col['col'].val is not None:
            upsert_kwargs['id'] = self._instancemeta.identity_col['col'].val
        self.upsert(**upsert_kwargs)   
