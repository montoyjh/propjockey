# Interactive Leaderboard for Property Requests and Notification (ILPRN).

## Development

In a fresh virtualenv:

```
pip install -e .
export FLASK_APP=ilprn
export FLASK_DEBUG=1
export ILPRN_SETTINGS=$(pwd)/local_settings.py
flask run
```

## Running Email Notifcation as a Cron Job

```
# cd to directory with local_settings.py ...
# activate the virtualenv ...
export ILPRN_SETTINGS=$(pwd)/local_settings.py
python -m ilprn.notify
```
