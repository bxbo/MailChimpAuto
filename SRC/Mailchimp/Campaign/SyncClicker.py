import logging

from psycopg2 import OperationalError

from SRC.Mailchimp.Audience.MailChimpAudience import MailChimpAudience
from SRC.Utilities.DBConnection import DBConnection
from SRC.Utilities.MailchimpConnection import MailchimpConnection
from SRC.Utilities.Param import Param
from SRC.Utilities.utilities import intersectionList, dateSQL


# Author BXBO
# V 0.1
# Creation : 20200726
# Update : 20200726
# Goal :
#    - fetch all campaign for which i need to get clickers
#    - Get clickers
#    - Create source file
#    - Update refresh date for campaigns

def Sync():
    logging.info("Start Sync of Clickers")
    #### INIT MANDATORY
    localConnection = DBConnection.connect()
    connection = localConnection.connection
    localParam = Param.getParam()
    params = localParam.params
    localMCConnection = MailchimpConnection.connect()
    mailchimpConnection = localMCConnection.connection

    #### GET ALL CAMPAIGNS TO BE REFRESHED
