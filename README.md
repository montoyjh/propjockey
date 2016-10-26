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
