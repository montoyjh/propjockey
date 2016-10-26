"""Collection of helper utilities for the main ilprn application."""

from pymongo import MongoClient

class Bunch:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

def config_to_uri(cfg):
    if not 'host' in cfg:
        cfg['host'] = 'localhost'
    if not 'port' in cfg:
        cfg['port'] = 27017
    format_str= "mongodb://{username}:{password}@{host}:{port}/{database}"
    return format_str.format(**cfg)

def mongoconnect(cfg, connect=False):
    return MongoClient(config_to_uri(cfg), connect=connect)

def get_collection(connector, config, name):
    conn, cfg, n = connector, config, name
    return conn[n][cfg[n]['database']][cfg[n]['collection']]
