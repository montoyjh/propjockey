import abc

from ilprn.util import mongoconnect


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
        ts_config = config['tokenstore_client']
        self.client = mongoconnect(ts_config)
        self.db = self.client[ts_config['database']]
        self.collection = self.db[ts_config['collection']]
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
