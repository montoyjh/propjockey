import json
from itertools import tee, izip, groupby
from operator import itemgetter
import re
import uuid

import pytest
from ilprn import ilprn
from passwordless import Passwordless


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

permitted_test_user = str(uuid.uuid4())+'@example.gov'


def set_test_config():
    ilprn.app.config['CLIENTS'] = {
        k: {'database': 'ilprn_test', 'collection': k}
        for k in ['votes', 'entries', 'workflows']
    }

    def get_workflow_ids(eids, coll):
        rv = sorted(coll.find({'eid': {'$in': eids}}), key=itemgetter('eid'))
        return [d['wid'] for d in rv]
    ilprn.app.config['WORKFLOWS']['get_workflow_ids'] = get_workflow_ids

    ilprn.app.config['VOTES'].update({'max_active_votes_per_user': 10})
    ilprn.app.config['NOTIFY'].update({'MAILER': 'null'})

    def user_permitted(user):
        print(user)
        if user == permitted_test_user:
            return {'success': True}
        else:
            return {'success': False, 'text': 'Better luck next time.'}
    ilprn.app.config['PASSWORDLESS'].update({
        'TOKEN_STORE': 'mongo',
        'DELIVERY_METHOD': 'null',
        'LOGIN_URL': 'plain',
        'dbname': 'ilprn_test',
        'user_permitted': user_permitted,
    })


@pytest.fixture
def client(request, user_unknown):
    ilprn.app.config['TESTING'] = True
    set_test_config()
    assert ilprn.app.config['PASSWORDLESS']['DELIVERY_METHOD'] == 'null'
    ilprn.passwordless = Passwordless(ilprn.app)
    client = ilprn.app.test_client()
    ctx = ilprn.app.test_request_context()
    ctx.push()
    login(client, user_unknown)
    return client


@pytest.fixture
def db():
    with ilprn.app.app_context():
        return ilprn.get_collections()


@pytest.fixture
def user_with_completed(db):
    doc = db.votes.find_one(ilprn.vconf['filter_completed'])
    return doc[ilprn.vconf['requesters']][0]


@pytest.fixture
def user_with_top_active_entry(db):
    vconf = ilprn.vconf
    doc = db.votes.find_one(vconf['filter_active'],
                            sort=[(vconf['nvotes'], -1)])
    return doc[vconf['requesters']][0], str(doc[vconf['entry_id']])


@pytest.fixture
def user_with_few_votes(db):
    vconf = ilprn.vconf

    def n_active_votes(user):
        filt = {vconf['requesters']: user}
        filt.update(vconf['filter_active'])
        return db.votes.find(filt).count()

    for doc in db.votes.find(vconf['filter_active']):
        for user in doc[vconf['requesters']]:
            if n_active_votes(user) < ilprn.econf['rows_per_page']:
                return user
    raise Exception("No user found without a pageful of active votes.")


@pytest.fixture
def user_unknown():
    return str(uuid.uuid4())+'@example.gov'


@pytest.fixture
def entryid_top(db):
    vconf = ilprn.vconf
    doc = db.votes.find_one(vconf['filter_active'],
                            sort=[(vconf['nvotes'], -1)])
    return str(doc[vconf['entry_id']])


@pytest.fixture
def eid_inactive_missing(db):
    econf, vconf = ilprn.econf, ilprn.vconf
    for e in db.entries.find({}, {econf['e_id']: 1, '_id': 0}):
        eid = e[econf['e_id']]
        if db.votes.find_one({vconf['entry_id']: eid}) is None:
            return eid
    raise Exception("All entries have votes.")


def login(client, user):
    pconf = ilprn.pconf
    app_id, app_secret = pconf['remote_app_id'], pconf['remote_app_secret']
    rv = client.post('/authtoken', data=dict(
        user=user,
        app_id=app_id,
        app_secret=app_secret
    ))
    uri = json.loads(rv.data)['uri']
    path = re.match('.*?//.*?(/.*)', uri).group(1)
    return client.get(path, follow_redirects=True)


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


def test_login_logout(client, user_unknown):
    """Make sure login and logout works"""
    user = user_unknown
    rv = login(client, user)
    assert 'Logged in as' in rv.data
    assert user in rv.data
    rv = logout(client)
    assert 'user@example.gov' in rv.data


def test_nofilter_onlymyvotes(client, user_with_completed):
    # Should return only entries for which user has upvoted,
    # including completed entries.
    user = user_with_completed
    login(client, user)
    rv = client.get('/rows?useronly=true&psize=500')
    rows = get_rows(rv)
    assert all(r.get('p_link') or r['votedfor'] for r in rows)
    # Ensure the user for this test case has at least one completed entry.
    assert any(r.get('p_link') for r in rows)


def test_toggle_onlymyvotes(client, user_with_few_votes):
    # Regression test to guard against overwriting
    # app.config['VOTES']['filter_active'].
    user = user_with_few_votes
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
    assert len(rows) > 0
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
    rows_accum = []
    base = '/rows?filter=W-*'
    for pnum in range(5):
        rows_accum.extend(
            get_rows(client.get(base+'&psize=100&pnum={}'.format(pnum))))
    rows_oneshot = get_rows(client.get(base+'&psize=500'))
    assert [r['id'] for r in rows_accum] == [r['id'] for r in rows_oneshot]


def test_voting(client, db, user_with_top_active_entry, user_unknown,
                eid_inactive_missing):
    # upvoting and downvoting
    user_already_requested, eid = user_with_top_active_entry
    user_hasnt_requested = user_unknown

    def try_up(eid=eid):
        return client.post('/vote', data=dict(how='up', eid=eid))

    def try_down(eid=eid):
        return client.post('/vote', data=dict(how='down', eid=eid))

    logout(client)
    rv = try_up()
    assert 'cannot vote anonymously' in rv.data
    rv = try_down()
    assert 'cannot vote anonymously' in rv.data

    max_votes = ilprn.vconf['max_active_votes_per_user']
    ilprn.vconf['max_active_votes_per_user'] = db.votes.count() + 1
    login(client, user_already_requested)
    rv = try_up()
    assert 'cannot upvote twice' in rv.data
    rv = try_down()
    assert 'downvoted' in rv.data and 'success' in rv.data
    rv = try_up()
    assert 'upvoted' in rv.data and 'success' in rv.data
    logout(client)
    ilprn.vconf['max_active_votes_per_user'] = max_votes

    login(client, user_hasnt_requested)
    rv = try_down()
    assert 'can only downvote after upvote' in rv.data
    rv = try_up()
    assert 'upvoted' in rv.data and 'success' in rv.data
    rv = try_down()
    assert 'downvoted' in rv.data and 'success' in rv.data

    # upvote something without a votedoc
    vconf = ilprn.vconf
    filt = {vconf['entry_id']: eid_inactive_missing,
            vconf['prop_field']: vconf['prop_value']}
    assert db.votes.find(filt).count() == 0
    rv = try_up(eid_inactive_missing)
    assert 'upvoted' in rv.data and 'success' in rv.data
    db.votes.delete_one(filt)


def test_form_ui_and_table_display(client, user_with_top_active_entry):
    user, eid = user_with_top_active_entry
    login(client, user)
    rv = client.get('/', follow_redirects=True)
    assert eid in rv.data
    # Pagination
    assert 'Next' in rv.data
    rv = client.get('/rows?format=html&pnum=1')
    assert 'Previous' in rv.data


def test_webui_voting(client, user_unknown, user_with_top_active_entry):
    # doesn't actually do 'click testing' of UI, but exercises endpoint.
    user = user_unknown
    _, eid = user_with_top_active_entry
    login(client, user)
    assert eid in client.get('/rows?format=html').data
    rv = client.post(
        'vote',
        data=dict(redirect_path='/rows?format=html',
                  eid=eid,
                  how='up'),
        follow_redirects=True)
    assert ('success: upvoted {}'.format(eid) in rv.data
            and 'Log out' in rv.data)
    rv = client.post(
        'vote',
        data=dict(redirect_path='/rows?format=html',
                  eid=eid,
                  how='down'),
        follow_redirects=True)
    assert ('success: downvoted {}'.format(eid) in rv.data
            and 'Log out' in rv.data)


def test_votelimit(client, user_unknown):
    # Each user has vconf['max_active_votes_per_user'] votes.
    user = user_unknown
    login(client, user)
    n = ilprn.vconf['max_active_votes_per_user']
    assert n <= 30  # Ensure reasonably small test setting
    rv = client.get('/rows?psize={}&filter=*-O&format=json'.format(n+1))
    eids = [r['id'] for r in json.loads(rv.data)['rows']]
    data = []
    for eid in eids:
        data.append(client.post('/vote', data=dict(how='up', eid=eid)).data)
    assert any('Consider revoking votes' in d for d in data)
    for eid in eids:
        client.post('/vote', data=dict(how='down', eid=eid))


def test_authtoken_gen_and_fulfillment(client, user_unknown):
    user = user_unknown
    token_uri = ilprn.passwordless.request_token(
        user=user, deliver=False)
    assert ('authenticate?token=' in token_uri and
            '&uid={}'.format(user) in token_uri)
    m = re.match('.*?//.*?(/.*)', token_uri)
    path = m.group(1)
    rv = client.get(path, follow_redirects=True)
    assert 'bad user email or token' not in rv.data
    rv = client.get(path, follow_redirects=True)
    assert 'bad user email or token' in rv.data


def test_deliver_authtoken(client, user_unknown):
    # Verify (e.g. via external API) that user is authorized to use
    # this instance of ILPRN. Once that is confirmed, send email with
    # login link.  If not confirmed, send email e.g. saying that
    # registration with external service is required.
    #
    # Note: this feature is not strictly necessary if users only
    # arrive to the ILPRN web interface via tokenized links requested
    # by the remote app.
    pconf = ilprn.pconf
    not_registered_user = user_unknown
    registered_user = permitted_test_user
    permitted = pconf['user_permitted'](not_registered_user)
    assert not permitted['success'] and len(permitted['text']) > 0
    permitted = pconf['user_permitted'](registered_user)
    print(permitted)
    assert permitted['success']

    message, category = ilprn.passwordless.request_token(
        user=registered_user, deliver=True)
    assert category == 'success'
    message, category = ilprn.passwordless.request_token(
        user=not_registered_user, deliver=True)
    assert category == 'warning'


def test_email_notification(client):
    # needs to be a module that one can run as a cron job
    #
    # Note: there is already an up-and-running cron job for MP apart
    # from ILPRN that can be adapted.
    #
    assert ilprn.app.config['NOTIFY']['MAILER'] == 'null'
    from ilprn.notify import notify
    # TODO: figure out why `notify()` takes so long with zero requests
    # needing notification.
    pass


def test_app_auth(client, user_unknown):
    # Be able to get data on behalf of user given app id and token.
    # In this way, one can build a service that consumes ilprn data.
    #
    # E.g. be able to vote using app id and token in request header.
    #
    # given MP app id and token, I want to get a token_uri for any user.
    user = user_unknown
    pconf = ilprn.pconf
    app_id, app_secret = pconf['remote_app_id'], pconf['remote_app_secret']
    rv = client.post('/authtoken', data=dict(user=user))
    assert rv.status_code == 401
    rv = client.post('/authtoken', data=dict(
        user=user,
        app_id='letsget',
        app_secret='dangerous'
    ))
    assert rv.status_code == 401
    rv = client.post('/authtoken', data=dict(
        user=user,
        app_id=app_id,
        app_secret=app_secret
    ))
    assert rv.status_code == 200 and '/authenticate?' in rv.data


def test_auth_lockdown(client):
    logout(client)
    rv = client.get('/', follow_redirects=True)
    assert 'user@example.gov' in rv.data
    rv = client.get('/rows', follow_redirects=True)
    assert 'user@example.gov' in rv.data
