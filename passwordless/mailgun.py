import requests


class Mailgun(object):
    def __init__(self, config):
        self.API_KEY = config['API_KEY']
        self.BASE_URL = config['BASE_URL']

    def send(self, message):
        to = message['to']
        to = to if isinstance(to, list) else [to]
        if message.get('use_bcc') is True:
            to_for_bcc = message.get('to_for_bcc', message['from'])
            to, bcc = [to_for_bcc], to
        else:
            to, bcc = to, []

        return requests.post(
            self.BASE_URL + "/messages",
            auth=("api", self.API_KEY),
            data={
                "text": message['text'],
                "from": message['from'],
                "to": to,
                "subject": message['subject'],
                "bcc": bcc,
            }
        )
