import logging
import datetime

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
#    - Get Audience to be synced into params
#    - Get last sync date from database
#    - Sync Subscriber -> create list
#    - Update refresh date for audience

def Sync():
    logging.info("Start Sync of Audience Potentiel")
    #### INIT MANDATORY
    localConnection = DBConnection.connect()
    connection = localConnection.connection
    localParam = Param.getParam()
    params = localParam.params
    localMCConnection = MailchimpConnection.connect()
    mailchimpConnection = localMCConnection.connection
    audienceToUpdate = []

    ### GET FROM PARAMS AUDIENCE_ID TO BE REFRESHED
    audiences_id = params["mailchimp_audience_to_sync"].split(":")
    logging.debug("number of audience to sync : %s (based on params)", len(audiences_id))

    for id in audiences_id:
        tempAudience = MailChimpAudience()
        tempAudience.getAudienceOnID(connection,id)
        if tempAudience.last_sync_date is None or tempAudience.last_sync_date < datetime.datetime.now()-datetime.timedelta(days=1):
            audienceToUpdate.append(tempAudience)

    logging.debug("number of audience to sync : %s (based on date)", len(audienceToUpdate))


    for audience in audienceToUpdate:
        if audience.last_sync_date is None:
            mailchimpConnection.lists.members.all(list_id=audience.id, get_all=True,
                                                  fields=params["mailchimp_members_fields_to_sync"])
        else:
            mailchimpConnection.lists.members.all(list_id=audience.id, get_all=True,
                                                  fields=params["mailchimp_members_fields_to_sync"],
                                                  since_last_changed=audience.last_sync_date)

    #### GET ALL CAMPAIGNS TO BE REFRESHED

