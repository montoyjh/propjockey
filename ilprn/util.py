"""Collection of helper utilities for the main ilprn application."""

import uuid

from pymongo import MongoClient


class Bunch:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def config_to_uri(cfg):
    if 'host' not in cfg:
        cfg['host'] = 'localhost'
    if 'port' not in cfg:
        cfg['port'] = 27017
    if 'username' not in cfg:
        format_str = "mongodb://{host}:{port}/{database}"
    else:
        format_str = "mongodb://{username}:{password}@{host}:{port}/{database}"
    return format_str.format(**cfg)


def mongoconnect(cfg, connect=False):
    return MongoClient(config_to_uri(cfg), connect=connect)


def get_collection(connector, config, name):
    conn, cfg, n = connector, config, name
    return conn[n][cfg[n]['database']][cfg[n]['collection']]


def make_requesters_aliases(votes_collection, requesters_field):
    alias_map = {}
    aliases = set()
    for d in votes_collection.find():
        for user in d[requesters_field]:
            if user not in alias_map:
                candidate = uuid.uuid4().hex
                while candidate in aliases:
                    candidate = uuid.uuid4().hex
                aliases.add(candidate)
                alias_map[user] = candidate + '@aliased.gov'
    return alias_map


def set_requesters_aliases(votes_collection, requesters_field, alias_map):
    # XXX There be dragons in iterating over a cursor rather than
    # pre-fetching all docs with `list()` prior to iteration, unless
    # you add a {'$snapshot": True} query modifier to the cursor,
    # except that doesn't work with sharded collections.
    for d in list(votes_collection.find({}, {requesters_field: 1})):
        aliased = [alias_map[user] for user in d[requesters_field]]
        votes_collection.update_one(
            {'_id': d['_id']},
            {'$set': {requesters_field: aliased}})
