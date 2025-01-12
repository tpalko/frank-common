import os 
import cowpy 
from frank.database.database import Database 
from frank.database.model import BaseModel
from frank.database.config import DatabaseConfig
from importlib import import_module

logger = cowpy.getLogger()

def setup():
        
    # logger.debug(sys.path)

    # settings_module_name = os.getenv('FRANKDB_SETTINGS')
    # settings = None 

    # if settings_module_name:
    #     settings = import_module(settings_module_name)

    # frankdb_models_module = settings.FRANKDB_MODELS

    models_module_name = os.getenv('FRANKDB_MODELS')
    logger.info(f'Loading models: {models_module_name}')
    t = import_module(models_module_name)

    # logger.debug(t)
    # logger.debug(dir(t))

    for d in dir(t):
        sub = t.__getattribute__(d)
        # logger.debug(f'Testing attribute {sub}')    
        is_sub = type(sub) == type and issubclass(sub, BaseModel) and sub != BaseModel
        if is_sub:        
            logger.info(f'Init: {sub}')
            sub.init()

    ## - DATABASE CONFIG 

    DB_PARAMS = [
        'DB_USER',
        'DB_HOST',
        'DB_DATABASE',
        'DB_PASSWORD',
        'DB_TYPE'
    ]
    params = { p: os.getenv(p.upper()) for p in DB_PARAMS }
    
    if not all(params.values()):
        raise EnvironmentError(f'Not all db values were provided: {params}')
    
    logger.debug(f'Loading database config: {params}')
    config = DatabaseConfig(**params)
    Database.createInstance(config)

    return True 
