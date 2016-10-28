from operator import itemgetter

from flask import Flask, session, redirect, url_for, escape, request
from flask import g, jsonify
from pymongo import ASCENDING, DESCENDING
from toolz import join, memoize, dissoc, merge

from util import Bunch, get_collection, mongoconnect


app = Flask(__name__)

app.config.from_envvar('ILPRN_SETTINGS', silent=True)

econf = app.config['ENTRIES']
vconf = app.config['VOTES']
wconf = app.config['WORKFLOWS']

def set_test_config():
    def get_workflow_ids(eids, coll):
        rv = sorted(coll.find({'eid': {'$in': eids}}), key=itemgetter('eid'))
        return [d['wid'] for d in rv]

    app.config['CLIENTS'] = {
        k: {'database': 'ilprn_test', 'collection': k}
        for k in ['votes', 'entries', 'workflows']
    }
    app.config['WORKFLOWS']['get_workflow_ids'] = get_workflow_ids

@app.route('/')
def index():
    if 'user' in session:
        return 'Logged in as %s' % escape(session['user'])
    return 'You are not logged in'

def connect_collections():
    """Connects to and provides handles to the MongoDB collections."""
    if app.config.get('USE_TEST_CLIENTS'): set_test_config()
    clients_config = app.config['CLIENTS']
    b = Bunch()
    b.clients = {name: mongoconnect(clients_config[name])
                 for name in clients_config}
    for name in clients_config:
        setattr(b, name, get_collection(b.clients, clients_config, name))
    return b

def get_collections():
    """Opens new client connections if there are none yet for the
    current application context.
    """
    if not hasattr(g, 'bunch'):
        g.bunch = connect_collections()
    return g.bunch

def tablerow_data((votedoc, entry, w_id), prop_missing=True):
    entry['description'] = econf['describe_entry'](
        entry, econf.get('description_fields', []))
    entry['id'] = entry[econf['e_id']]
    entry['e_link'] = econf['url_for_entry'].format(e_id=entry['id'])
    entry['extrasort'] = entry[econf['extrasort']['field']]
    if w_id:
        entry['w_link'] = wconf['url_for'].format(w_id=w_id)
    for k, _ in entry.items():
        if k not in ['id', 'description', 'extrasort', 'w_link', 'e_link']:
            del entry[k]

    if votedoc:
        votedoc['nvotes'] = votedoc[vconf['nvotes']]
        if 'user' in session:
            votedoc['votedfor'] = vconf['user_voted'](
                session['user'], prefilter=False, votes_doc=votedoc)
        for k, _ in votedoc.items():
            if k not in ['nvotes', 'votedfor']:
                del votedoc[k]
    elif not prop_missing:
        entry['p_link'] = econf['url_for_prop'].format(e_id=entry['id'])

    return merge(entry, votedoc or {})

def find_votes(completed=False, user_only=False, sortdir=DESCENDING):
    db = get_collections()
    filt = vconf['filter_completed' if completed else 'filter_active'].copy()
    if user_only and 'user' in session:
        filt.update(vconf['user_voted'](session['user'], prefilter=True))
    return db.votes.find(
        filt,
        votedoc_projection(),
        sort=[(vconf['nvotes'], sortdir)])

def get_workflow_ids(entry_ids):
    """Return list of workflows ids corresponding to given ids in order.

    If no workflow corresponds to a given entry id, yield `None`.
    """
    db = get_collections()
    return wconf['get_workflow_ids'](entry_ids, db.workflows)


def entries_by_filter(entry_filter, sort=None, limit=0):
    db = get_collections()
    return db.entries.find(
        entry_filter,
        entry_projection(),
        sort=sort,
        limit=limit)

def order_by_idlist(entries, entry_ids):
    """Return `entries` sorted by the order in `entry_ids`.

    n=len(entry_ids) may be greater than m=len(entries).
    Running time is O(n+m).
    """
    emap = {}
    for e in entries:
        emap[e[econf['e_id']]] = e
    e_id_set = {e_id for e_id in emap}
    entry_ids_present = [e_id for e_id in entry_ids if e_id in e_id_set]
    return [emap[e_id] for e_id in entry_ids_present]

@app.route('/rows')
def get_rows():
    user_only = request.args.get('useronly', 'false') == 'true'
    primary_sort_dir = (
        DESCENDING if request.args.get('psort', 'decr') == 'decr'
        else ASCENDING)
    secondary_sort_dir = (
        ASCENDING if request.args.get('ssort', 'incr') == 'incr'
        else DESCENDING)
    user_filter = None
    if request.args.get('filter'):
        # TODO want to wrap below in try/except for bad input
        # XXX hack to get at all systems for debugging
        if request.args.get('filter') == "{}":
            user_filter = {}
        else:
            user_filter = econf['filter']['transform'](request.args.get('filter'))
    which = set(request.args.getlist('which')
                or ['active', 'inactive_missing', 'inactive_has'])

    #import ipdb; ipdb.set_trace()
    active_votedocs, active_entry_ids = votedocs_and_eids(
        completed=False, user_only=user_only, sortdir=primary_sort_dir)
    result = []
    if 'active' in which:
        result += rows_active(active_votedocs, active_entry_ids,
                              primary_sort_dir, secondary_sort_dir,
                              user_filter=user_filter, user_only=user_only)
    from toolz import pluck
    assert 'mp-81' not in pluck('id', result)
    # Surplus. Cull and return.
    if len(result) > econf['rows_per_page']:
        result = result[:econf['rows_per_page']]
        return jsonify({'rows': result})
    # User supplied no filter or supplied an entry id as filter. Return.
    if ((user_filter is None and not user_only) or
        (len(result) == 1 and econf['e_id'] in user_filter)):
        return jsonify({'rows': result, 'nomore': True})

    # At this point, there may be few results, or a user simply wants
    # to fetch more. If `user_filter` is not None, we can return
    # additional entries without active votes. With filter, order
    # should be
    #
    # {active votes} -> {missing property} -> {has property},
    #
    # respecting sorting parameter psort and ssort.
    sort=[(econf['extrasort']['field'], secondary_sort_dir)]
    deficit = econf['rows_per_page'] - len(result)
    # Adding one to deficit for `limit` ensures that, in the case of
    # zero deficit, we can (a) check whether the user can request
    # another "page" of results, and (b) avoid setting limit=0 on a
    # mongo cursor, i.e. we avoid setting *no* limit.
    limit = deficit + 1
    if user_only:
        _, completed_entry_ids = votedocs_and_eids(
            completed=True, user_only=user_only, sortdir=primary_sort_dir)
        e_id_constraint = {'$in': completed_entry_ids}
        result += rows_inactive(e_id_constraint, user_filter,
                                prop_missing=False, sort=sort, limit=limit)
        if len(result) > econf['rows_per_page']:
            result = result[:econf['rows_per_page']]
            return jsonify({'rows': result})
        else:
            return jsonify({'rows': result, 'nomore': True})

    e_id_constraint = {'$nin': active_entry_ids}
    if 'inactive_missing' in which:
        result += rows_inactive(e_id_constraint, user_filter,
                                prop_missing=True, sort=sort, limit=limit)
    if len(result) > econf['rows_per_page']:
        result = result[:econf['rows_per_page']]
        return jsonify({'rows': result})

    deficit = econf['rows_per_page'] - len(result)
    limit = deficit + 1
    if 'inactive_has' in which:
        result += rows_inactive(e_id_constraint, user_filter,
                                prop_missing=False, sort=sort, limit=limit)
    if len(result) > econf['rows_per_page']:
        result = result[:econf['rows_per_page']]
        return jsonify({'rows': result})
    else:
        return jsonify({'rows': result, 'nomore': True})

def votedocs_and_eids(completed=False, user_only=False, sortdir=DESCENDING):
    # TODO make user_only be falsy or a user string, to make this
    # function cacheable. Can rename user_only for clarity.
    votedocs = list(find_votes(completed=completed, user_only=user_only,
                               sortdir=sortdir))
    entry_ids = [d[vconf['entry_id']] for d in votedocs]
    return votedocs, entry_ids

def rows_active(active_votedocs, active_entry_ids,
                primary_sort_dir, secondary_sort_dir,
                user_filter=None, user_only=False):
    # Because the scope of returned entries is limited to those with
    # active votes, and because we want to sort them by nvotes, we
    # require all vote-active entry ids, in sorted order, from the
    # votes collection to form a basis filter for querying the entries
    # collection.
    filt = {econf['e_id']: {'$in': active_entry_ids}}
    # override econf['e_id'] filter spec with `user_filter`'s.
    if user_filter: filt.update(user_filter)
    # Fetch/construct equal-length lists of entries, workflow_ids, and
    # votedocs, keeping nvotes-sorted order.
    entries = order_by_idlist(entries_by_filter(filt), active_entry_ids)
    entry_ids = [e[econf['e_id']] for e in entries]
    workflow_ids = get_workflow_ids(entry_ids)
    entry_ids_set = set(entry_ids)
    votedocs = [d for d in active_votedocs
                if d[vconf['entry_id']] in entry_ids_set]
    result = [tablerow_data(z) for z in zip(votedocs, entries, workflow_ids)]
    result.sort(key=itemgetter('extrasort'),
                reverse=secondary_sort_dir is DESCENDING)
    result.sort(key=itemgetter('nvotes'),
                reverse=primary_sort_dir is DESCENDING)
    return result

def rows_inactive(e_id_constraint, user_filter,
                  prop_missing=True, sort=None, limit=0):
    filt = {econf['e_id']: e_id_constraint}
    if user_filter is not None: filt.update(user_filter)
    filt.update(econf['missing_property' if prop_missing else 'has_property'])
    entries = list(entries_by_filter(filt, sort=sort, limit=limit))
    nones = len(entries) * [None]
    return [tablerow_data(z, prop_missing=prop_missing)
            for z in zip(nones, entries, nones)]

@memoize
def entry_projection():
    econf = app.config['ENTRIES']
    projlist = [econf['e_id']]
    projlist.append(econf['extrasort']['field'])
    projlist.extend(econf.get('description_fields', []))
    projdict = {'_id': 0}
    for elt in projlist:
        projdict[elt] = 1
    return projdict

@memoize
def votedoc_projection():
    projlist = [vconf['entry_id']]
    projlist.append(vconf['nvotes'])
    projlist.extend(vconf['projection_extras'])
    projdict = {'_id': 0}
    for elt in projlist:
        projdict[elt] = 1
    return projdict


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        session['user'] = request.form['user']
        return redirect(url_for('index'))
    return '''
        <form action="" method="post">
            <p><input type=text name=user>
            <p><input type=submit value=Login>
        </form>
    '''

@app.route('/logout')
def logout():
    # remove the username from the session if it's there
    session.pop('user', None)
    return redirect(url_for('index'))

@app.cli.command('make_test_db')
def make_test_db():
    from pymongo import MongoClient
    client = MongoClient()
    tdb = client.ilprn_test
    db = get_collections()

    tdb.votes.drop()
    tdb.votes.insert_many(list(db.votes.find()))
    print(tdb.votes.count())

    entry_ids = [d[vconf['entry_id']] for d in tdb.votes.find()]
    wids = get_workflow_ids(entry_ids)
    tdb.workflows.drop()
    tdb.workflows.insert_many([{'eid': eid, 'wid': wid} for (eid, wid)
                               in zip(entry_ids, wids) if wid])
    print(tdb.workflows.count())

    tdb.entries.drop()
    proj = {'elasticity.K_VRH': 1}
    proj.update(entry_projection())
    tdb.entries.insert_many(list(db.entries.find({}, proj)))
    print(tdb.entries.count())

app.secret_key = '&WB:\xab\xbe\xdc-\xa8v-\xfc+\xd0d_\x15\xc4!\x91N\xcbH\xe6'
