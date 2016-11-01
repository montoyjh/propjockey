from operator import itemgetter

from flask import Flask, session, redirect, url_for, escape, request
from flask import g, jsonify, render_template, flash, abort
from pymongo import ASCENDING, DESCENDING
from toolz import join, memoize, dissoc, merge

from util import Bunch, get_collection, mongoconnect
from passwordless import Passwordless


app = Flask(__name__)

app.config.from_envvar('ILPRN_SETTINGS', silent=True)

econf = app.config['ENTRIES']
vconf = app.config['VOTES']
wconf = app.config['WORKFLOWS']
pconf = app.config['PASSWORDLESS']

passwordless = Passwordless(app)

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
        return redirect(url_for('rows', format='html'))
    else:
        return redirect(url_for('login'))

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


def entries_by_filter(entry_filter, sort=None, skip=0, limit=0):
    db = get_collections()
    return db.entries.find(
        entry_filter,
        entry_projection(),
        sort=sort,
        skip=skip,
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

def format_rows(data):
    fmt = request.args.get('format', 'json')
    if fmt == 'json':
        return jsonify(data)
    if fmt != 'html':
        return ("error: Unknown response format: {}."
                " Please choose 'json' or 'html'.".format(fmt))

    params = _rows_params()
    params.update({'filter': request.args.get('filter')})
    extrasort_label = econf['extrasort']['label']
    rows = data['rows']
    for r in rows:
        r['description'] = econf['describe_entry_html'](r['description'])
    return render_template(
        'index.html',
        rows=rows,
        params=params,
        extrasort_label=extrasort_label,
        no_more_rows=data.get('nomore'))

def _rows_params():
    user_only = request.args.get('useronly', 'false') == 'true'
    primary_sort_dir = (
        DESCENDING if request.args.get('psort', 'decr') == 'decr'
        else ASCENDING)
    secondary_sort_dir = (
        ASCENDING if request.args.get('ssort', 'incr') == 'incr'
        else DESCENDING)
    user_filter = None
    # TODO want to wrap below in try/except for bad input
    if request.args.get('filter'):
        user_filter = econf['filter']['transform'](request.args.get('filter'))
    which = set(request.args.getlist('which')
                or ['active', 'inactive_missing', 'inactive_has'])
    pagesize = request.args.get('psize', econf['rows_per_page'], type=int)
    pagenum = request.args.get('pnum', 0, type=int)
    skip = pagenum * pagesize
    return dict(
        user_only=user_only,
        primary_sort_dir=primary_sort_dir,
        secondary_sort_dir=secondary_sort_dir,
        user_filter=user_filter,
        which=which,
        pagesize=pagesize,
        pagenum=pagenum,
        skip=skip)

@app.route('/rows')
def rows():
    params = _rows_params()
    user_only = params['user_only']
    primary_sort_dir = params['primary_sort_dir']
    secondary_sort_dir = params['secondary_sort_dir']
    user_filter = params['user_filter']
    which = params['which']
    pagesize = params['pagesize']
    skip = params['skip']

    active_votedocs, active_entry_ids = votedocs_and_eids(
        completed=False, user_only=user_only, sortdir=primary_sort_dir)
    result = []
    if 'active' in which:
        rows = rows_active(active_votedocs, active_entry_ids,
                           primary_sort_dir, secondary_sort_dir,
                           user_filter=user_filter, user_only=user_only)
        if skip < len(rows):
            result += rows[skip:]
            skip = 0
        else:
            skip -= len(rows)
    if len(result) > pagesize:
        result = result[:pagesize]
        return format_rows({'rows': result})
    if ((user_filter is None and not user_only) or
        (len(result) == 1 and user_filter and econf['e_id'] in user_filter)):
        return format_rows({'rows': result, 'nomore': True})

    # At this point, there may be few results, or a user simply wants
    # to fetch more. If `user_filter` is not None, we can return
    # additional entries without active votes. With filter, order
    # should be
    #
    # {active votes} -> {missing property} -> {has property},
    #
    # respecting sorting parameter psort and ssort.
    sort=[(econf['extrasort']['field'], secondary_sort_dir)]
    deficit = pagesize - len(result)
    # Adding one to deficit for `limit` ensures that, in the case of
    # zero deficit, we can (a) check whether the user can request
    # another "page" of results, and (b) avoid setting limit=0 on a
    # mongo cursor, i.e. we avoid setting *no* limit.
    limit = deficit + 1
    if user_only:
        _, completed_entry_ids = votedocs_and_eids(
            completed=True, user_only=user_only, sortdir=primary_sort_dir)
        e_id_constraint = {'$in': completed_entry_ids}
        prop_missing = False
        cursor = entries_inactive(
            e_id_constraint, user_filter, prop_missing=prop_missing,
            sort=sort, skip=skip, limit=limit)
        result += rows_inactive(list(cursor), prop_missing=prop_missing)
        if len(result) > pagesize:
            result = result[:pagesize]
            return format_rows({'rows': result})
        else:
            return format_rows({'rows': result, 'nomore': True})

    e_id_constraint = {'$nin': active_entry_ids}
    if 'inactive_missing' in which:
        prop_missing = True
        cursor = entries_inactive(
            e_id_constraint, user_filter, prop_missing=prop_missing,
            sort=sort, skip=0, limit=limit)
        if skip < cursor.count():
            cursor = cursor.skip(skip)
            result += rows_inactive(list(cursor), prop_missing=prop_missing)
            skip = 0
        else:
            skip -= cursor.count()
    if len(result) > pagesize:
        result = result[:pagesize]
        return format_rows({'rows': result})

    deficit = pagesize - len(result)
    limit = deficit + 1
    if 'inactive_has' in which:
        prop_missing = False
        cursor = entries_inactive(
            e_id_constraint, user_filter, prop_missing=prop_missing,
            sort=sort, skip=0, limit=limit)
        if skip < cursor.count():
            cursor = cursor.skip(skip)
            result += rows_inactive(list(cursor), prop_missing=prop_missing)
            skip = 0
        else:
            skip -= cursor.count()
    if len(result) > pagesize:
        result = result[:pagesize]
        return format_rows({'rows': result})
    else:
        return format_rows({'rows': result, 'nomore': True})

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

def rows_inactive(entries, prop_missing=True):
    if not entries:
        return []
    nones = len(entries) * [None]
    return [tablerow_data(z, prop_missing=prop_missing)
            for z in zip(nones, entries, nones)]

def entries_inactive(e_id_constraint, user_filter, prop_missing=True,
                     sort=None, skip=0, limit=0):
    filt = {econf['e_id']: e_id_constraint}
    if user_filter is not None: filt.update(user_filter)
    filt.update(econf['missing_property' if prop_missing else 'has_property'])
    return entries_by_filter(filt, sort=sort, skip=skip, limit=limit)

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
        user = request.form['user']
        session['user'] = user
        passwordless.request_token(user)
        return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/authenticate')
def authenticate():
    if passwordless.authenticate(request):
        session['user'] = request.args.get('uid')
        return redirect(url_for('index'))
    else:
        flash('bad user email or token', 'danger')
        return redirect(url_for('login'))

@app.route('/logout')
def logout():
    # remove the username from the session if it's there
    session.pop('user', None)
    return redirect(url_for('index'))

@app.route('/authtoken', methods=['POST'])
def authtoken():
    """Endpoint for a companion web service to get a token for a user."""
    app_id = request.form.get('app_id')
    app_secret = request.form.get('app_secret')
    if (app_id != pconf['remote_app_id'] or
        app_secret != pconf['remote_app_secret']):
        abort(401)
    user = request.form.get('user')
    if not user:
        abort(400)
    return jsonify({'uri': passwordless.request_token(user, deliver=False)})

@app.route('/vote', methods=['POST'])
def vote():
    user = session.get('user')
    eid = request.form.get('eid')
    how = request.form.get('how')
    redirect_path = request.form.get('redirect_path')

    message, category= _vote(user, eid, how)
    if not redirect_path:
        return jsonify((message, category))
    else:
        # 'error' -> 'danger' for front-end styling
        category = 'danger' if category == 'error' else category
        flash(message, category)
        return redirect(redirect_path)

def _vote(user, eid, how):
    ERROR, INFO, WARNING, SUCCESS = 'error', 'info', 'warning', 'success'
    if not user:
        return 'cannot vote anonymously', ERROR
    if (not eid) or (how not in ['up', 'down']):
        return 'must specify entry id and how to vote ("up" or "down")', ERROR
    db = get_collections()

    if how == 'up':
        filt_active_user_voted = vconf['filter_active'].copy()
        filt_active_user_voted.update(vconf['user_voted'](user, prefilter=True))
        num_active_user_voted = db.votes.find(filt_active_user_voted).count()
        max_active = vconf['max_active_votes_per_user']
        if num_active_user_voted >= max_active:
            return ("There is a maximum of {} active votes per user "
                    "from this interface. Consider revoking votes for your "
                    "least favorite entries.".format(max_active)), ERROR

    filt = {vconf['entry_id']: eid}
    filt.update(vconf['filter_active'])
    active_doc = db.votes.find_one(filt)
    filt_for_update = {vconf['entry_id']: eid,
                       vconf['prop_field']: vconf['prop_value']}
    if active_doc:
        user_voted = vconf['user_voted'](
            user, prefilter=False, votes_doc=active_doc)
        if how == 'up' and user_voted:
            return 'cannot upvote twice', ERROR
        elif how == 'up':
            return vconf['record_vote'](
                user, active_doc, db.votes, 'up', filt_for_update), SUCCESS
        elif how == 'down' and not user_voted:
            return 'can only downvote after upvote', ERROR
        elif how == 'down':
            return vconf['record_vote'](
                user, active_doc, db.votes, 'down', filt_for_update), SUCCESS
        else:
            return "unknown voting operation on active entry", ERROR
    else:
        filt = {vconf['entry_id']: eid}
        filt.update(vconf['filter_completed'])
        completed_doc = db.votes.find_one(filt)
        if completed_doc:
            return "cannot vote on completed entry", ERROR
        filt = {econf['e_id']: eid}
        filt.update(econf['missing_property'])
        entry_exists = db.entries.find(filt).count() == 1
        if not entry_exists:
            return "cannot vote on non-existent entry {}".format(eid), ERROR
        if how == 'down':
            return "cannot downvote entry with missing property", ERROR

        return vconf['record_vote'](
                user, {}, db.votes, 'up', filt_for_update), SUCCESS

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
    proj = {f: 1 for f in econf['filter_fields']}
    proj.update(entry_projection())
    tdb.entries.insert_many(list(db.entries.find({}, proj)))
    print(tdb.entries.count())

app.secret_key = '&WB:\xab\xbe\xdc-\xa8v-\xfc+\xd0d_\x15\xc4!\x91N\xcbH\xe6'
