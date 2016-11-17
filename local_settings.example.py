import pymongo
from pymatgen import MPRester


def describe_entry(e, fields):
    """Join fields in entry e to provide a string description.

    Example:
    >>> e = {'formula': 'LiCoO2', {'spacegroup': {'symbol': 'R-3m'}}}
    >>> describe_entry(e, ['formula', 'spacegroup.symbol'])
    'LiCoO2 R-3m'
    """
    from operator import getitem
    return " ".join([reduce(getitem, f.split('.'), e)
                     for f in fields])


def describe_entry_html(description):
    import re
    formula, spacegroup = description.split(" ")
    formula = re.sub(r'\s', r'', formula)
    formula = re.sub(r'(\.?\d+\.?\d*)', r'<sub>\1</sub>', formula)
    spacegroup = re.sub(r'\_(\d)', r'<sub>\1</sub>', spacegroup)
    spacegroup = re.sub(r'\-(\d)',
                        r'<span style="text-decoration:overline;">\1</span>',
                        spacegroup)
    return "{} {}".format(formula, spacegroup)

ENTRIES = {
    'has_property': {
        'elasticity': {'$exists': True}
    },
    'missing_property': {
        'elasticity': {'$exists': False}
    },
    'e_id': 'task_id',
    'extrasort': {
        'field': 'e_above_hull',
        'label': 'E above hull / atom (eV)',
        'default': pymongo.ASCENDING
    },
    'url_for_entry': 'https://materialsproject.org/materials/{e_id}',
    'url_for_prop': 'https://materialsproject.org/materials/{e_id}',
    'description_fields': ['pretty_formula', 'spacegroup.symbol'],
    'describe_entry': describe_entry,
    'describe_entry_html': describe_entry_html,
    'prop_displayname': 'elasticity',
    'filter': {
        'placeholder': 'Fe-O',
        'transform': MPRester.parse_criteria
    },
    'filter_fields': ['elasticity.K_VRH', 'chemsys'],
    'rows_per_page': 10,
}


def get_workflow_ids(entry_ids, workflow_collection):
    fireworks = workflow_collection.database.fireworks
    fk_field = "spec.snl.about._mp_id"
    fws = fireworks.find({fk_field: {"$in": entry_ids}},
                         {"_id": 0, "fw_id": 1, fk_field: 1})
    idmap = {}
    for fw in fws:
        entry_id = fw['spec']['snl']['about']['_mp_id']
        idmap[entry_id] = fw['fw_id']
    return [idmap.get(e_id, None) for e_id in entry_ids]

WORKFLOWS = {
    'get_workflow_ids': get_workflow_ids,
    'url_for': 'http://elastic.dash.materialsproject.org/wf/{w_id}',
}


def user_voted(email, prefilter=True, votes_doc=None):
    if prefilter:
        return {'requesters': email}
    else:
        return email in votes_doc['requesters']


def record_vote(email, votes_doc, votes_collection, how, filt_for_update):
    assert how in ['up', 'down']
    if how == 'up':
        assert email not in votes_doc.get('requesters', [])
    else:
        assert email in votes_doc['requesters']

    op = '$push' if how == 'up' else '$pull'
    amt = 1 if how == 'up' else -1
    update = {'$inc': {'nrequesters': amt}, op: {'requesters': email}}
    votes_collection.update_one(filt_for_update, update, upsert=True)
    return "success: {}voted {}".format(how, filt_for_update['material_id'])

# `filter_completed` must be something you can pass to "$set" to
# update a vote document to mark it as completed.
VOTES = {
    'filter_active': {'state': {'$ne': 'COMPLETED'}, 'prop': 'elasticity'},
    'filter_completed': {'state': 'COMPLETED', 'prop': 'elasticity'},
    'entry_id': 'material_id',
    'prop_field': 'prop',
    'prop_value': 'elasticity',
    'requesters': 'requesters',
    'nvotes': 'nrequesters',
    'user_voted': user_voted,
    'record_vote': record_vote,
    'projection_extras': ['requesters'],
    'max_active_votes_per_user': 1000,
    'requesters_notified': 'requesters_notified',
}

NOTIFY = {
    'MAILER': 'mailgun',
    'user_text': ("You voted, perhaps by requesting a prediction of "
                  "elastic bulk moduli, for the full elastic tensor "
                  "and associated properties of material {} to be "
                  "calculated. This data is now online at {}.\n\n"
                  "Thank you,\nMaterials Project team"),
    'user_subject': "Elasticity data for {} is online",
    'from': ("Elasticity Requests "
             "<elastiquests@example.gov>"),
    'to_for_bcc': "noreply@example.gov",
    'staff_text': ("Sent notification about {eid} "
                   "to {n} requester{s}.\n"
                   "Data online at {url_for_prop}\n"),
    'staff_to': "elastiquests-staff@example.gov",
    'staff_subject': "Sent notifications about {} materials to {} users",
}

USE_TEST_CLIENTS = True
CLIENTS = {
    'votes': {
        'host': 'localhost',
        'port': 57010,
        'database': 'apps',
        'collection': 'property_requests',
        'username': 'ilprn_readwrite',
        'password': 'emulsify-gamester-fealty-dwarf-county',
    },
    'entries': {
        'host': 'localhost',
        'port': 57010,
        'database': 'data',
        'collection': 'materials',
        'username': 'ilprn_read',
        'password': 'teacher-unfurl-anyone-facial-abyss',
    },
    'workflows': {
        'host': 'localhost',
        'port': 57011,
        'database': 'elastijobs',
        'collection': 'workflows',
        'username': 'ilprn_read',
        'password': 'aback-stool-padre-poky-salute',
    }
}


def user_permitted(user):
    import requests
    r = requests.post("https://materialsproject.org/is_user",
                      headers={'X-API-KEY': 'API_KEY'},
                      data=dict(user=user))
    if r.json() is True:
        return {'success': True}
    elif r.json() is False:
        return {
            'success': False,
            'text': ("No Materials Project account for {} was found. "
                     "Please register at https://materialsproject.org "
                     "and then try again.").format(user),
            }
    else:
        raise Exception('`user_permitted` failed')

PASSWORDLESS = {
    'LOGIN_URL': 'plain',
    'TOKEN_STORE': 'mongo',
    'tokenstore_client': {
        'database': 'ilprn_test',
        'collection': 'tokenstore',
    },
    'DELIVERY_METHOD': 'mailgun',
    'user_permitted': user_permitted,
    'remote_app_id': 'APP_ID',
    'remote_app_secret': 'APP_SECRET',
    'remote_app_name': 'Materials Project',
    'remote_app_uri': 'https://materialsproject.org',
}

MAILGUN = {
    'API_KEY': 'API_KEY',
    'BASE_URL': 'https://api.mailgun.net/v3/example.gov',
    'DELIVER_LOGIN_URL': {
        'FROM': ("Elasticity Requests "
                 "<elastiquests@example.gov>"),
        'SUBJECT': 'Login link for elastiquests.example.gov',
    },
}

# http://flask.pocoo.org/docs/0.11/quickstart/#sessions
# >>> import os; os.urandom(24)
APP_SECRET_KEY = (
    'This should be as random as possible.')
