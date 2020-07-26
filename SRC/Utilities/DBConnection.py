import logging
import psycopg2
from SRC.Utilities.Param import Param


class DBConnection:
    __connection = None

    __params = None

    connection = None

    @staticmethod
    def connect():
        """ Static access method. """
        if DBConnection.__connection == None:
            DBConnection()
        return DBConnection.__connection

    def __init__(self):
        logging.debug("Param : you entered the singleton constructor of connection")

        localParam = Param.getParam()
        self.__params = localParam.params
        self.connection = psycopg2.connect(user=self.__params['db_user'],
                                           password=self.__params['db_pwd'],
                                           host=self.__params['db_host'],
                                           port=self.__params['db_port'],
                                           database=self.__params['db_name'])

        logging.debug("Connected to database %s as user : %s", self.__params['db_name'], self.__params['db_user'])
        """ Virtually private constructor. """
        if DBConnection.__connection is not None:
            raise Exception("This class is a singleton!")
        else:
            DBConnection.__connection = self

    def close(self):
        self.connection.close()