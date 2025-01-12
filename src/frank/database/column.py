import cowpy
import simplejson as json 
from pytz import timezone 
from datetime import datetime 
from frank.database.dialect import text 

logger = cowpy.getLogger()

class Column(object):
    col_type = None 
    val = None 
    kwargs = None 

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs 

    def set_val(self, val):
        # logger.debug(f'setting {self.__class__.__name__} as {val}')
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
    col_type = json

class TextColumn(Column):
    col_type = text 

class StringColumn(Column):
    col_type = str
    
class IntColumn(Column):
    col_type = int

class BoolColumn(Column):
    col_type = bool

class FloatColumn(Column):
    col_type = float

class DateTimeColumn(Column):
    col_type = datetime.date
    mark = None 

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'mark' in kwargs:
            self.mark = kwargs['mark']

    def timestamp(self, operation):
        if self.val is None:
            if (operation == "insert" and self.mark == 'create') or (operation in ["insert", "update"] and self.mark == 'update'):
                return datetime.now(timezone('UTC'))
        return self.val

class ColumnFactory():

    @classmethod 
    def Get(cls, instance):
        return type(instance)

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