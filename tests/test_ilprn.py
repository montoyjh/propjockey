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

def set_test_config():
    def get_workflow_ids(eids, coll):
        rv = sorted(coll.find({'eid': {'$in': eids}}), key=itemgetter('eid'))
        return [d['wid'] for d in rv]

    ilprn.app.config['CLIENTS'] = {
        k: {'database': 'ilprn_test', 'collection': k}
        for k in ['votes', 'entries', 'workflows']
    }
    ilprn.app.config['WORKFLOWS']['get_workflow_ids'] = get_workflow_ids

@pytest.fixture
def client(request):
    ilprn.app.config['TESTING'] = True
    set_test_config()
    client = ilprn.app.test_client()
    return client

def login(client, user):
    return client.post('/login', data=dict(
        user=user
    ), follow_redirects=True)

def logout(client):
    return client.get('/logout', follow_redirects=True)

def get_rows(rv):
    return json.loads(rv.data)['rows']

def test_nofilter(client):
    rv = client.get('/rows')
    assert len(json.loads(rv.data)) > 0

def test_nofilter_incrvotes(client):
    rv = client.get('/rows?psort=incr')
    rows = get_rows(rv)
    assert all(ri['nvotes'] <= rj['nvotes'] for ri, rj in pairwise(rows))

def test_nofilter_decrextrasort(client):
    rv = client.get('/rows?ssort=decr')
    rows = get_rows(rv)
    for _, g in groupby(rows, key=itemgetter('nvotes')):
        assert all(ri['extrasort'] >= rj['extrasort']
                   for ri, rj in pairwise(list(g)))

def test_withfilter(client):
    elsym = 'O'
    rv = client.get('/rows?filter=*-{}'.format(elsym))
    rows = get_rows(rv)
    formulae = [r['description'].split()[0] for r in rows]
    assert all(elsym in f and len(f) >= 2 for f in formulae)

def test_active_withfilter_incrvotes(client):
    elsym = 'O'
    rv = client.get(
        '/rows?which=active&psort=incr&filter=*-{}'.format(elsym))
    rows = get_rows(rv)
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
    # Should return only entries for which user has upvoted,
    # including completed entries.
    user = 'maartendft@gmail.com'
    login(client, user)
    rv = client.get('/rows?useronly=true')
    rows = get_rows(rv)
    assert all(r.get('p_link') or r['votedfor'] for r in rows)
    # Ensure the user for this test case has at least one completed entry.
    assert any(r.get('p_link') for r in rows)

def test_toggle_onlymyvotes(client):
    # Regression test to guard against overwriting
    # app.config['VOTES']['filter_active'].
    user = 'shyamd@lbl.gov'
    login(client, user)
    rv = client.get('/rows?useronly=true')
    rows = get_rows(rv)
    assert all(r.get('p_link') or r['votedfor'] for r in rows)
    rv = client.get('/rows?useronly=whatever')
    rows = get_rows(rv)
    assert not all(r.get('p_link') or r.get('votedfor') for r in rows)

def test_row_fetch_order(client):
    # With filter, order should be
    # {active votes} -> {missing property} -> {has property},
    # respecting psort and ssort sort parameters

    # Looks like filter=W-* would currently be useful for this.
    rv = client.get('/rows?filter=W-*')
    rows = get_rows(rv)
    inactive_has, active_missing, inactive_missing = [], [], []
    for (i, r) in enumerate(rows):
        if 'p_link' in r:
            inactive_has.append(i)
        elif 'nvotes' in r:
            active_missing.append(i)
        else:
            inactive_missing.append(i)
    assert all(i < j < k for (i,j,k) in
               zip(active_missing, inactive_missing, inactive_has))

def test_paginaton(client):
    # Need to pass `skip` to mongo cursors appropriately.
    pass

def test_voting(client):
    # upvoting and downvoting
    pass

def test_form_ui_and_table_display(client):
    pass

def test_authtoken_gen_and_fulfillment(client):
    pass

def test_email_authtoken(client):
    # Need to verify from external API that user is authorized.
    pass

def test_email_notification(client):
    pass

def test_app_auth(client):
    # Be able to get data on behalf of user given app id and token.
    # In this way, one can build a service that consumes ilprn data.
    pass
