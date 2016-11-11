import time

from passwordless import mailgun
from ilprn import connect_collections, econf, vconf, app

nconf = app.config['NOTIFY']
db = connect_collections()

vcoll = db.votes
ecoll = db.entries

requests_pending = vcoll.find(vconf['filter_active'])

for r in requests_pending:
    eid = r[vconf['entry_id']]
    filt = econf['has_property'].copy()
    filt.update({econf['e_id']: eid})
    entry = ecoll.find_one(filt)
    if entry:
        vcoll.update_one({'_id': r['_id']},
                         {'$set': vconf['filter_completed']})

filt_notify = vconf['filter_completed'].copy()
filt_notify.update({vconf['requesters_notified']: {'$ne': True}})
requests_needing_notification = vcoll.find(filt_notify)

requests_with_notification_sent = []
for r in requests_needing_notification:
    # Throttle to mitigate delivery failure to e.g. @qq.com emails.
    time.sleep(5)
    eid = r[vconf['entry_id']]
    response = mailgun.send({
        "to": r[vconf['requesters']],
        "subject": nconf['user_subject'].format(eid),
        "text": nconf['user_text'].format(
            eid, econf['url_for_prop'].format(eid)),
        "from": nconf['from'],
        "use_bcc": True,
        "to_for_bcc": nconf['to_for_bcc'],
    })
    if response.status_code == 200:
        print("Sent notification about {} to {} requesters.".format(
            eid, len(r[vconf['requesters']])))
        vcoll.update_one({'_id': r['_id']},
                         {'$set': {vconf['requesters_notified']: True}})
        requests_with_notification_sent.append(r)

body_staff = "\n".join([
    nconf['staff_text'].format(
        eid=r[vconf['entry_id']],
        n=r[vconf['nrequesters']],
        s='s' if r[vconf['nrequesters']] > 1 else '',
        url_for_prop=econf['url_for_prop'].format(r[vconf['entry_id']])
    )
    for r in requests_with_notification_sent])


n_entries = len(requests_with_notification_sent)
n_users = len({u for r in requests_with_notification_sent
               for u in r[vconf['requesters']]})
if n_entries:
    response = mailgun.send({
        "to": nconf['staff_to'],
        "subject": nconf["staff_subject"].format(
            n_entries, n_users),
        "text": body_staff,
        "from": nconf['from'],
        "use_bcc": False,
    })
    if response.status_code == 200:
        print("Sent summary to staff. {} entries done.".format(n_entries))
