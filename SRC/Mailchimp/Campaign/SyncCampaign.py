import logging

from psycopg2 import OperationalError

from SRC.Mailchimp.Campaign.MailChimpCampaign import MailChimpCampaign
from SRC.Utilities.DBConnection import DBConnection
from SRC.Utilities.MailchimpConnection import MailchimpConnection
from SRC.Utilities.Param import Param
from SRC.Utilities.utilities import intersectionList, dateSQL


# Author BXBO
# V 0.2
# Creation : 20200706
# Update : 20200726
# Goal :
#    - fetch all campaign from mailchimp
#    - compare to what is know in the DB
#    - create new ones
#    - update existing ones

# Update 20200726 -> use of signleton (no more params needed)
# Update 20200726 -> problem with date format solved
#                    If datas comes from sql query -> no problem
#                    If datas comes from mailchimp -> format must be changed via Utilities
# Update 20200726 -> extract MailChimpCampaign to file

def Sync():
    logging.info("Start Sync of Campaign")
    #### INIT MANADTORY
    localConnection = DBConnection.connect()
    connection = localConnection.connection
    localParam = Param.getParam()
    params = localParam.params
    localMCConnection = MailchimpConnection.connect()
    mailchimpConnection = localMCConnection.connection
    #### FETCH ALL MAILCHIMP CAMPAIGN
    currentMCCampaigns = []
    knownMCCampaigns = []
    mailchimpCampaigns = mailchimpConnection.campaigns.all(get_all=True)
    logging.debug('mailchimp campaigns retreived : %s', len(mailchimpCampaigns['campaigns']))

    for c in mailchimpCampaigns['campaigns']:
        currentCampaign = MailChimpCampaign(c['id'],
                                            c['web_id'],
                                            c['status'],
                                            c['emails_sent'],
                                            dateSQL(c['send_time']),
                                            c['report_summary']['unique_opens'],
                                            c['report_summary']['open_rate'],
                                            c['report_summary']['clicks'],
                                            c['report_summary']['click_rate'])
        currentMCCampaigns.append(currentCampaign)

    knownCampaignQuery = "SELECT * FROM mailchimp_campaigns "
    try:
        cursor = connection.cursor()
        cursor.execute(knownCampaignQuery)
        for c in cursor.fetchall():
            currentCampaign = MailChimpCampaign(c[0],
                                                c[1],
                                                c[2],
                                                c[3],
                                                c[4],
                                                c[5],
                                                c[6],
                                                c[7],
                                                c[8])
            knownMCCampaigns.append(currentCampaign)
        cursor.close()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    logging.debug('mailchimp known campaigns retreived : %s', len(knownMCCampaigns))

    ### with the implementation of __hash and __eq in the class i can compare the 2 lists

    newMCCampaigns = (set(currentMCCampaigns) - set(knownMCCampaigns))
    logging.debug("number of new campaigns = %s " , len(newMCCampaigns))
    for campaign in newMCCampaigns:
        campaign.insertDB(connection)

    ### CAMPAIGNS TO UPDATE
    ### If id is known i update everything exect id from mailchimp
    ### Have to invest time in understanding why but ORDER IS IMPORTANT
    updateMCCampaign = intersectionList(knownMCCampaigns, currentMCCampaigns)
    logging.debug("number of cmapaign to update = %s ", len(updateMCCampaign))
    for campaign in updateMCCampaign:
        campaign.updateDBonID(connection)


    logging.info("%s campaigns created + %s campaigns updated" ,len(newMCCampaigns),len(updateMCCampaign))


