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
#    - fetch all audiences from mailchimp
#    - compare to what is know in the DB
#    - create new ones
#    - update existing ones

def Sync():
    logging.info("SyncAudience : Start Sync of Audience")
    #### INIT MANDATORY
    localConnection = DBConnection.connect()
    connection = localConnection.connection
    localParam = Param.getParam()
    params = localParam.params
    localMCConnection = MailchimpConnection.connect()
    mailchimpConnection = localMCConnection.connection

    currentMCAudience = []
    knownMCAudience = []
    audiences = mailchimpConnection.lists.all(get_all=True)
    logging.debug('SyncAudience : mailchimp audience retreived : %s', len(audiences['lists']))

    for c in audiences['lists']:
        currentAudience = MailChimpAudience(c['id'],
                                            c['web_id'],
                                            c['name'],
                                            dateSQL(c['date_created']),
                                            c['stats']['member_count'],
                                            c['stats']['unsubscribe_count'],
                                            dateSQL(c['stats']['last_sub_date']),
                                            dateSQL(c['stats']['last_unsub_date']))
        currentMCAudience.append(currentAudience)

    knownAudienceQuery = "SELECT * FROM mailchimp_audiences "
    try:
        cursor = connection.cursor()
        cursor.execute(knownAudienceQuery)
        for c in cursor.fetchall():
            currentCampaign = MailChimpAudience(c[0],
                                                c[1],
                                                c[2],
                                                c[3],
                                                c[4],
                                                c[5],
                                                c[6],
                                                c[7])
            knownMCAudience.append(currentCampaign)
        cursor.close()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    logging.debug('SyncAudience : mailchimp known Audience retreived : %s', len(knownMCAudience))

    newMCAudiences = (set(currentMCAudience) - set(knownMCAudience))
    logging.debug("SyncAudience : number of new Audience = %s ", len(newMCAudiences))
    for campaign in newMCAudiences:
        campaign.insertDB(connection)

    ### CAMPAIGNS TO UPDATE
    ### If id is known i update everything exect id from mailchimp
    ### Have to invest time in understanding why but ORDER IS IMPORTANT
    updateMCAudiences = intersectionList(knownMCAudience, currentMCAudience)
    logging.debug("SyncAudience : number of Audience to update = %s ", len(updateMCAudiences))
    for campaign in updateMCAudiences:
        campaign.updateDBonID(connection)

    logging.info("SyncAudience : %s audiences created + %s audiences updated", len(newMCAudiences), len(updateMCAudiences))