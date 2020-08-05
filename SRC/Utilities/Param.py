import json
import logging
import uuid


class Param:
    __param = None

    params = None

    uuid = None

    @staticmethod
    def getParam():
        """ Static access method. """
        if Param.__param == None:
            Param('/Users/xavierbouquiaux/PycharmProjects/MailChimpAuto/SRC/param.json')
        return Param.__param

    def __init__(self, filename):
        logging.debug("Param : you entered the singleton constructor")
        with open(filename, 'r') as f:
            self.params = json.load(f)
            self.uuid = str(uuid.uuid1())
        """ Virtually private constructor. """
        if Param.__param is not None:
            raise Exception("This class is a singleton!")
        else:
            Param.__param = self

