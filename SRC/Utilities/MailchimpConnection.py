from mailchimp3 import MailChimp
import logging
from SRC.Utilities.Param import Param


class MailchimpConnection:
    __connection = None

    __params = None

    connection = None

    @staticmethod
    def connect():
        """ Static access method. """
        if MailchimpConnection.__connection == None:
            MailchimpConnection()
        return MailchimpConnection.__connection

    def __init__(self):
        logging.debug("Param : you entered the singleton constructor of connection")

        localParam = Param.getParam()
        self.__params = localParam.params
        self.connection = MailChimp(mc_api=self.__params['mailchimp_key'], mc_user=self.__params['mailchimp_user'])

        logging.debug("Connected to MailChimp as user : %s", self.__params['mailchimp_user'])
        """ Virtually private constructor. """
        if MailchimpConnection.__connection is not None:
            raise Exception("This class is a singleton!")
        else:
            MailchimpConnection.__connection = self

