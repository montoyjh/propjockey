import uuid
from .token_store import TOKEN_STORES
from .login_url import LOGIN_URLS
from .delivery_methods import DELIVERY_METHODS


class Passwordless(object):
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
        self.single_use = True

    def init_app(self, app):
        config = app.config['PASSWORDLESS']
        token_store = config['TOKEN_STORE']
        self.token_store = TOKEN_STORES[token_store](config)
        # Does the token expire after a single login session,
        # i.e. is it bookmark-able?
        self.single_use = config.get('SINGLE_USE', True)

        delivery_method = config['DELIVERY_METHOD']
        self.delivery_method = DELIVERY_METHODS[delivery_method](app.config)

        login_url = config['LOGIN_URL']
        self.login_url = LOGIN_URLS[login_url](app.config)

        self.user_permitted = config.get('user_permitted', lambda user: True)

    def request_token(self, user, deliver=True):
        token = uuid.uuid4().hex
        self.token_store.store_or_update(token, user)
        login_url = self.login_url.generate(token, user)
        permitted = self.user_permitted(user)
        if deliver:
            return self.delivery_method(
                login_url, email=user, permitted=permitted)
        else:
            return login_url

    def authenticate(self, flask_request):
        token, uid = self.login_url.parse(flask_request)
        is_authenticated = self.token_store.get_by_userid(uid) == token
        if is_authenticated and self.single_use:
            self.token_store.invalidate_token(uid)

        return is_authenticated
