import simplejson as json
from datetime import datetime 
import sqlite3 
import mariadb
from enum import Enum

class DbType(Enum):
    MariaDB = 0
    Sqlite = 1

db_providers = {
    DbType.Sqlite: lambda config: sqlite3.connect(config.filename),
    DbType.MariaDB: lambda config: mariadb.connect(host=config.host, user=config.user, password=config.password, database=config.name)
}

class text(str):
    pass 

class Dialect(Enum):
    AUTO_INCREMENT = 0
    ENGINE = 1
    FLOAT = 2
    CHAR = 3
    GET_CREATE_TABLE = 4
    INTEGER = 5
    JSON_TYPE = 6
    TEXT = 7

# DIALECT_MAPPINGS = {
#     Dialect.GET_CREATE_TABLE: lambda config: db_dialect_mappings[config.dbType][Dialect.GET_CREATE_TABLE]
# }

def get_db_connection(config):
    return db_providers[config.dbType](config)

db_dialect_mappings = {
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
        Dialect.TEXT: 'text',
        Dialect.GET_CREATE_TABLE: 'show create table',
        Dialect.INTEGER: 'int',
        Dialect.JSON_TYPE: 'json'
    }
}

# -- as of this writing (march 2024), TYPE_MAPPINGS is only referenced when calling create_table
TYPE_MAPPINGS = {
    str: lambda config: db_dialect_mappings[config.dbType][Dialect.CHAR],
    text: lambda config: db_dialect_mappings[config.dbType][Dialect.TEXT],
    int: lambda config: db_dialect_mappings[config.dbType][Dialect.INTEGER],
    datetime.date: lambda config: 'datetime',
    bool: lambda config: 'bool', # tinyint(1)',
    float: lambda config: db_dialect_mappings[config.dbType][Dialect.FLOAT],
    json: lambda config: db_dialect_mappings[config.dbType][Dialect.JSON_TYPE]
}

