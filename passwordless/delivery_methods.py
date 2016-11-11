import abc
import logging
import sys

from .mailgun import Mailgun

SUCCESS, INFO, WARNING, ERROR = 'success', 'info', 'warning', 'danger'


class DeliveryMethod(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, login_url, **kwargs):
        return


class DeliverByNull(DeliveryMethod):
    def __init__(self, config):
        pass

    def __call__(self, login_url, email, permitted):
        if permitted['success']:
            return "Deliver: {} to {}".format(login_url, email), SUCCESS
        else:
            return "Deliver: no permission to {}".format(email), WARNING


class DeliverByLog(DeliveryMethod):
    def __init__(self, config):
        """ just log that we tried to deliver. """
        self.logs = logging.getLogger(__name__)
        self.logs.setLevel(logging.DEBUG)
        log = logging.StreamHandler(sys.stdout)
        log.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log.setFormatter(formatter)
        self.logs.addHandler(log)

    def __call__(self, login_url, email, permitted):
        if permitted['success']:
            self.logs.debug("Deliver: " + login_url + " to " + email)
            return "Deliver: {} to {}".format(login_url, email), SUCCESS
        else:
            self.logs.debug("Deliver: no permission to " + email)
            return "Deliver: no permission to {}".format(email), WARNING


class DeliverByMailgun(DeliveryMethod):
    def __init__(self, config):
        config = config['MAILGUN']
        self.mailgun = Mailgun(config)
        self.from_email = config['DELIVER_LOGIN_URL']['FROM']
        self.subject = config['DELIVER_LOGIN_URL']['SUBJECT']

    def __call__(self, login_url, email, permitted):
        if permitted['success']:
            text = "Here's your login link:\n{}\n".format(login_url)
        else:
            text = permitted['text']
        message = {
            "text": text,
            "from": self.from_email,
            "to": [email],
            "subject": self.subject,
        }
        response = self.mailgun.send(message)
        if response.status_code == 200:
            return "Sent login link to {}".format(email), SUCCESS
        else:
            return "Failed to send login link.", ERROR

DELIVERY_METHODS = {
    'log': DeliverByLog,
    'null': DeliverByNull,
    'mailgun': DeliverByMailgun,
}
