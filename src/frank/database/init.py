import os 
import sys 
from frank.database.database import Database 
from frank.database.model import BaseModel
from importlib import import_module

def setup():
        
    # print(sys.path)

    settings_module_name = os.getenv('FRANKDB_SETTINGS')
    settings = None 

    if settings_module_name:
        settings = import_module(settings_module_name)

    frankdb_models_module = settings.FRANKDB_MODELS

    print(f'Loading models: {frankdb_models_module}')
    t = import_module(frankdb_models_module)

    # print(t)
    # print(dir(t))

    for d in dir(t):
        sub = t.__getattribute__(d)
        # print(f'Testing attribute {sub}')    
        is_sub = type(sub) == type and issubclass(sub, BaseModel) and sub != BaseModel
        if is_sub:        
            print(f'Init: {sub}')
            sub()

    print(f'Loading database config: {settings.DB_CONFIG}')
    if settings:
        Database.createInstance(**{'config': settings.DB_CONFIG})
