import os 
import simplejson as json
from enum import Enum

class DbType(Enum):
    MariaDB = 0
    Sqlite = 1

ENV_MAPPING = {
    'DB_USER': 'user',
    'DB_HOST': 'host',
    'DB_DATABASE': 'name',
    'DB_PASSWORD': 'password',
    'DB_TYPE': 'dbType',
    'DB_FILENAME': 'filename'
}

class DatabaseConfig():

    host = None 
    user = None 
    password = None 
    name = None 

    filename = None 

    dbType = None 

    def __init__(self, *args, **kwargs):
        
        for e in ENV_MAPPING.keys():
            coalesced_value = kwargs[ENV_MAPPING[e]] if ENV_MAPPING[e] in kwargs else os.getenv(e)
            if coalesced_value is not None:
                print(f'setting {ENV_MAPPING[e]} <-- {coalesced_value}')
                self.__setattr__(ENV_MAPPING[e], coalesced_value)

        # if any([ self.__getattribute__(ENV_MAPPING[k]) is None for k in ENV_MAPPING ]):
        #     raise Exception(f'Not everything was set!')
        
        for d in DbType:
            if d.name.lower() == self.dbType:
                self.dbType = d 
                break 
        
        if self.dbType not in DbType:
            raise Exception(f'DatabaseConfig dbType {self.dbType} is not of DbType')
        
    def __repr__(self):
        return json.dumps({ ENV_MAPPING[k]: str(self.__getattribute__(ENV_MAPPING[k])) for k in ENV_MAPPING })

    # @staticmethod
    # def NewSqlite(filename):

    #     __instance = DatabaseConfig()

    #     __instance.filename = filename 

    #     __instance.dbType = DbType.Sqlite

    #     return __instance
    
    # @staticmethod
    # def NewMariadb(host, user, password, name):

    #     __instance = DatabaseConfig()

    #     __instance.host = host 
    #     __instance.user = user 
    #     __instance.password = password 
    #     __instance.name = name 

    #     __instance.dbType = DbType.MariaDB

    #     return __instance
