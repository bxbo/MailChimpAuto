import json
import logging
import datetime

from psycopg2 import OperationalError

from SRC.Mailchimp.Audience.MailChimpAudience import MailChimpAudience
from SRC.Mailchimp.Audience.MailChimpMember import MailChimpMember
from SRC.Utilities import utilities
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
#    - Sync Subscriber
#       - create list of subscriber
#       - create list of unsubs
#       - create list of bounce
#    - Update refresh date for audience

def Sync():
    logging.info("SyncAudiencePotential : Start Sync of Audience Potentiel")
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
    logging.debug("SyncAudiencePotential : number of audience to sync : %s (based on params)", len(audiences_id))

    for id in audiences_id:
        tempAudience = MailChimpAudience()
        tempAudience.getAudienceOnID(connection, id)
        audienceToUpdate.append(tempAudience)

    logging.debug("SyncAudiencePotential : number of audience to sync : %s (based on date)", len(audienceToUpdate))

    timeStampTo = utilities.currentTimeStamp()

    for audience in audienceToUpdate:

        MCMembersList = []
        updateStatusMemberList = []
        DBMembersList = []
        updateMemberList = []
        newMemberList = []

        ### If it's the first time i sync this audience -> no filter on date
        if audience.last_sync_date is None:
            print('NONE')
            mcList = mailchimpConnection.lists.members.all(list_id=audience.id, get_all=True,
                                                           fields=params["mailchimp_members_fields_to_sync"],
                                                           before_last_changed=timeStampTo.isoformat())

        else:
            mcList = mailchimpConnection.lists.members.all(list_id=audience.id, get_all=True,
                                                           fields=params["mailchimp_members_fields_to_sync"],
                                                           since_last_changed=audience.last_sync_date.isoformat(),
                                                           before_last_changed=timeStampTo.isoformat())

        logging.debug("SyncAudiencePotential : Number of Members to sync : %s", len(mcList['members']))

        ### CREATING MEMBERS IN DATABASE IF NOT EXISTS
        for mcMember in mcList['members']:
            currentMember = MailChimpMember(mcMember['id'],
                                            mcMember['email_address'],
                                            mcMember['status'],
                                            dateSQL(mcMember['timestamp_signup']),
                                            dateSQL(mcMember['timestamp_opt']),
                                            dateSQL(mcMember['last_changed']),
                                            mcMember['language'],
                                            mcMember['unique_email_id'],
                                            mcMember['web_id'],
                                            audience.id)
            MCMembersList.append(currentMember)


        DBMembersList = MailChimpMember.getAllMember(connection)
        if len(MCMembersList) > 0:

            newMemberList = (set(MCMembersList) - set(DBMembersList))
            if len(newMemberList) > 0:
                for member in newMemberList:
                    member.insertDB(connection,timeStampTo)

            updateMemberList = intersectionList(DBMembersList, MCMembersList)
            if len(updateMemberList) > 0:
                for member in updateMemberList:
                    member.updateDBonID(connection,timeStampTo)

            logging.debug("SyncAudiencePotential : Number of new member = %s" , len(newMemberList) )
            logging.debug("SyncAudiencePotential : Number of updated member = %s", len(updateMemberList))

            newFileName = params['working_directory']+'/'+params['new_members_file_pattern']+'_'+localParam.uuid+'.csv'
            MailChimpMember.createFileNew(newMemberList,newFileName)

            ### Updating last sync date for audience
            audience.updateDBLastSyncDate(connection, timeStampTo)
            logging.debug("SyncAudiencePotential : Update Last Sync Date to %s for audience %s", str(timeStampTo),
                          audience.id)


        else:
            logging.debug("SyncAudiencePotential : No Potential to Sync")

