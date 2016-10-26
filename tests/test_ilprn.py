import json
from itertools import tee, izip, groupby
from operator import itemgetter

import pytest
from ilprn import ilprn

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

@pytest.fixture
def client(request):
    ilprn.app.config['TESTING'] = True
    client = ilprn.app.test_client()
    return client

@pytest.fixture
def db():
    with ilprn.app.app_context():
        return ilprn.get_collections()

def login(client, user):
    return client.post('/login', data=dict(
        user=user
    ), follow_redirects=True)

def logout(client):
    return client.get('/logout', follow_redirects=True)

def test_get_collections(db):
    assert db.votes.count() > 0
    assert db.entries.count() > 0
    assert db.workflows.count() > 0

def test_nofilter(client):
    rv = client.get('/rows')
    assert len(json.loads(rv.data)) > 0

def test_nofilter_incrvotes(client):
    rv = client.get('/rows?psort=incr')
    rows = json.loads(rv.data)
    assert all(ri['nvotes'] <= rj['nvotes'] for ri, rj in pairwise(rows))

def test_nofilter_decrextrasort(client):
    rv = client.get('/rows?ssort=decr')
    rows = json.loads(rv.data)
    for _, g in groupby(rows, key=itemgetter('nvotes')):
        assert all(ri['extrasort'] >= rj['extrasort']
                   for ri, rj in pairwise(list(g)))

def test_withfilter(client):
    elsym = 'O'
    rv = client.get('/rows?filter=*-{}'.format(elsym))
    rows = json.loads(rv.data)
    formulae = [r['description'].split()[0] for r in rows]
    assert all(elsym in f and len(f) >= 2 for f in formulae)

def test_withfilter_incrvotes(client):
    elsym = 'O'
    rv = client.get('/rows?psort=incr&filter=*-{}'.format(elsym))
    rows = json.loads(rv.data)
    formulae = [r['description'].split()[0] for r in rows]
    assert all(elsym in f and len(f) >= 2 for f in formulae)
    assert all(ri['nvotes'] <= rj['nvotes'] for ri, rj in pairwise(rows))

def test_login_logout(client):
    """Make sure login and logout works"""
    user = 'maartendft@gmail.com'
    rv = login(client, user)
    assert 'Logged in as' in rv.data
    assert user in rv.data
    rv = logout(client)
    assert 'You are not logged in' in rv.data

def test_nofilter_onlymyvotes(client):
    user = 'maartendft@gmail.com'
    login(client, user)
    rv = client.get('/rows?useronly=true')
    rows = json.loads(rv.data)
    assert all(r['votedfor'] for r in rows)

def test_toggle_onlymyvotes(client):
    # Regression test to guard against overwriting
    # app.config['VOTES']['filter_active'].
    user = 'shyamd@lbl.gov'
    login(client, user)
    rv = client.get('/rows?useronly=true')
    rows = json.loads(rv.data)
    assert all(r['votedfor'] for r in rows)
    rv = client.get('/rows?useronly=whatever')
    rows = json.loads(rv.data)
    assert not all(r['votedfor'] for r in rows)
