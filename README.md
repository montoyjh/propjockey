Propjockey helps you queue up workflows and dynamically change their priorities for running;
it's like having a [slipmat](https://en.wikipedia.org/wiki/Slipmat) for scientific computing.

Propjockey is a tool for staff managing scientific computing
resources to facilitate a user community voting to prioritize
calculation of properties across a database of known entities. For
example, across a database of entities corresponding to known
crystalline materials, the full elastic tensor may not already be
calculated for each material because such a calculation is
computationally expensive, not of interest for all materials, etc.

The staff managing the growth and dissemination of this database of
material structures and properties wishes to empower the community of
users consuming the data to help prioritize computational jobs in a
way that will dovetail with in-house priorities. Propjockey helps with
this, connecting users to details of running workflows and providing
email notification when property calculations complete.

* Former name: Interactive Leaderboard for Property Requests and Notification (ILPRN).

Example deployment: http://elastic.ilprn.materialsproject.org/.

[Science Gateways 2016](http://sciencegateways.org/gateways2016/) conference: [extended abstract](docs/gateways2016-extended-abstract.pdf) and [talk slides](docs/gateways2016-talk-slides.pdf).

## Development

In a fresh virtualenv:

```
pip install -e .
export FLASK_APP=ilprn
export FLASK_DEBUG=1
export ILPRN_SETTINGS=$(pwd)/local_settings.py
flask run
```

There is a provided `local_settings.example.py` to serve as a template
for your `local_settings.py`, which if stored at the root level will
be `.gitignore`d.

Ensure local settings are appropriate for your use case. Function
definitions are part of the settings. Hopefully, many of the various
settings, including some of the functions, will not need to change for
you to get started.

## Testing

You can make a test database derived from your real, live data. The
database will be located at mongodb://localhost:27017/ilprn_test and
have entries, votes, and workflows collections. All user emails in
your real data will be aliased to protect user identities. To drop any
existing test database and generate a new one using your data:

```
flask make_test_db
```

Then, to test the code against the test database:

```
python setup.py test
```

The `USE_TEST_CLIENTS` local setting enables you to use your test
database during development and not just when running automated
tests. This is nice when lacking a reliable/fast network connection.

To save your test database to a file for backup/sharing:

```
mongodump --db=ilprn_test --gzip --archive=ilprn_test.gz
```

and to restore it during development / on a testing server:

```
mongorestore --drop db=ilprn_test --gzip --archive=ilprn_test.gz
```

## Deployment

There are many officially documented
[options](http://flask.pocoo.org/docs/0.11/deploying/) for deploying a
Flask app such as this, for example [gunicorn](http://gunicorn.org/)
behind an [nginx](https://nginx.org/en/) proxy, e.g.

```
# activate the virtualenv
gunicorn -w 4 -b 0.0.0.0:4000 ilprn:app
# and ensure your nginx configuration `proxy_pass`es to 0.0.0.0:4000
```

An example proxy setup is described at the official Flask
documentation
[here](http://flask.pocoo.org/docs/0.11/deploying/wsgi-standalone/#proxy-setups).

## Running Email Notification as a Cron Job

```
# activate the virtualenv
# cd to directory with local_settings.py
# Ensure `USE_TEST_CLIENTS` local setting is False
export ILPRN_SETTINGS=$(pwd)/local_settings.py
python -m ilprn.notify
```
