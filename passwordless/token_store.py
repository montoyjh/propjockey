import abc


class TokenStore(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        pass

    @abc.abstractmethod
    def store_or_update(self, token, userid, ttl=600, origin=None):
        return

    @abc.abstractmethod
    def invalidate_token(self, userid):
        return

    @abc.abstractmethod
    def get_by_userid(self, userid):
        return


class MemoryTokenStore(TokenStore):
    STORE = {}

    def store_or_update(self, token, userid, ttl=600, origin=None):
        self.STORE[userid] = token

    def invalidate_token(self, userid):
        del self.STORE[userid]

    def get_by_userid(self, userid):
        return self.STORE.get(userid, None)


class MongoTokenStore(TokenStore):
    def __init__(self, config):
        from pymongo import MongoClient
        self.dbname = config.get('dbname', 'tokenstore')
        self.dbhost = config.get('dbhost', 'localhost')
        self.dbport = config.get('dbport', 27017)
        self.client = MongoClient(self.dbhost, self.dbport)
        self.db = self.client[self.dbname]
        self.collection_name = config.get('collection', 'tokenstore')
        self.collection = self.db[self.collection_name]
        self.collection.create_index("userid")

    def store_or_update(self, token, userid, ttl=None, origin=None):
        if not token or not userid:
            return False
        self.collection.replace_one({'userid': userid},
                                    {'userid': userid, 'token': token},
                                    upsert=True)

    def invalidate_token(self, userid):
        self.collection.delete_many({'userid': userid})

    def get_by_userid(self, userid):
        usertoken = self.collection.find_one({'userid': userid})
        return usertoken.get('token') if usertoken else None

TOKEN_STORES = {
    'memory': MemoryTokenStore,
    'mongo': MongoTokenStore,
}
