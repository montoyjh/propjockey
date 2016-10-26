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

@app.route('/')
def index():
    if 'user' in session:
        return 'Logged in as %s' % escape(session['user'])
    return 'You are not logged in'

def connect_collections():
    """Connects to and provides handles to the MongoDB collections."""
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

def tablerow_data((votedoc, entry, w_id)):
    entry['description'] = econf['describe_entry'](entry)
    if w_id:
        entry['w_link'] = wconf['url_for'].format(w_id=w_id)
    entry['id'] = entry[econf['e_id']]
    entry['e_link'] = econf['url_for_entry'].format(e_id=entry['id'])
    entry['extrasort'] = entry[econf['extrasort']['field']]
    for k, _ in entry.items():
        if k not in ['id', 'description', 'extrasort', 'w_link', 'e_link']:
            del entry[k]
    votedoc['nvotes'] = votedoc[vconf['nvotes']]
    if 'user' in session:
        votedoc['votedfor'] = vconf['user_voted'](
            session['user'], prefilter=False, votes_doc=votedoc)
    for k, _ in votedoc.items():
        if k not in ['nvotes', 'votedfor']:
            del votedoc[k]
    return merge(entry, votedoc)

def find_active_votes(user_only=False, sortdir=DESCENDING,
                      limit=econf['rows_per_page']):
    db = get_collections()
    filt = vconf['filter_active'].copy()
    if user_only and 'user' in session:
        filt.update(vconf['user_voted'](session['user'], prefilter=True))
    return db.votes.find(
        filt,
        votedoc_projection(),
        limit=limit,
        sort=[(vconf['nvotes'], sortdir)])

def get_workflow_ids(entry_ids):
    """Return list of workflows ids corresponding to given ids in order.

    If no workflow corresponds to a given entry id, yield `None`.
    """
    db = get_collections()
    return wconf['get_workflow_ids'](entry_ids, db.workflows)


def entries_by_filter(entry_filter, limit=0):
    db = get_collections()
    return db.entries.find(
        entry_filter,
        entry_projection(),
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
    #
    # If there aren't many entries with active votes that fit the
    # filter, go ahead and fetch entries that have completed after
    # voting and then entries that have no votes but match the entry
    # filter, sorted by the extrasort field.

    primary_sort_dir = (
        DESCENDING if request.args.get('psort', 'decr') == 'decr'
        else ASCENDING)
    secondary_sort_dir = (
        ASCENDING if request.args.get('ssort', 'incr') == 'incr'
        else DESCENDING)
    user_only = request.args.get('useronly', 'false') == 'true'

    # Do a first filter for entry ids with active votes. If there is
    # an entry filter string, merge the resulting filter dict
    # `added_filter` with the first filter (overriding the
    # econf['e_id'] filter spec with that from `added_filter` if
    # present) to fetch entries.
    active_votedocs = list(find_active_votes(user_only=user_only,
                                             sortdir=primary_sort_dir))
    active_entry_ids = [d[vconf['entry_id']] for d in active_votedocs]
    filterstr = request.args.get('filter')
    entry_filter = {econf['e_id']: {'$in': active_entry_ids}}
    added_filter = None
    if filterstr:
        # XXX want to wrap below in try/except for bad input
        added_filter = econf['filter']['transform'](filterstr)
        entry_filter.update(added_filter)

    # Fetch/construct equal-length lists of entries, workflow_ids, and
    # votedocs, keeping nvotes-sorted order.
    entries = order_by_idlist(entries_by_filter(entry_filter),
                              active_entry_ids)
    entry_ids = [e[econf['e_id']] for e in entries]
    workflow_ids = get_workflow_ids(entry_ids)
    entry_ids_set = set(entry_ids)
    votedocs = [d for d in active_votedocs
                if d[vconf['entry_id']] in entry_ids_set]
    result = list(map(tablerow_data, zip(votedocs, entries, workflow_ids)))
    result.sort(key=itemgetter('extrasort'),
                reverse=secondary_sort_dir is DESCENDING)
    result.sort(key=itemgetter('nvotes'),
                reverse=primary_sort_dir is DESCENDING)

    # At this point, there may be few results, or a user simply wants
    # to fetch more. If there is an `added_filter`, we can return
    # entries without active votes, ordered by the 'extrasort' field.
    deficit = len(result) - econf['rows_per_page']

    return jsonify(result)

@memoize
def entry_projection():
    econf = app.config['ENTRIES']
    projlist = [econf['e_id']]
    projlist.append(econf['extrasort']['field'])
    projlist.extend(econf.get('projection_extras', []))
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

app.secret_key = '&WB:\xab\xbe\xdc-\xa8v-\xfc+\xd0d_\x15\xc4!\x91N\xcbH\xe6'
