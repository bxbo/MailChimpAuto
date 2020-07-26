import logging

from SRC.Mailchimp.Audience import SyncAudience
from SRC.Mailchimp.Campaign import SyncCampaign
from SRC.Utilities.Param import Param
from SRC.Utilities.DBConnection import DBConnection
from SRC.Utilities.MailchimpConnection import MailchimpConnection


## TO DO -> PARAMS


def main():
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)

    ### PARAM CREATION
    localParam = Param.getParam()
    logging.debug('Params Created')
    logging.info('Starting CLT FOR %s', localParam.params['client'])

    ### DB CONNECTION CREATION

    localDBConnection = DBConnection.connect()
    logging.info('Database connection created')

    ### MAILCHIMP CONNECTION

    localMailchimp = MailchimpConnection.connect()
    logging.info("Connected to mailchimp as user : %s", localParam.params['mailchimp_user'])


    ### SYNC CAMPAIGN


    SyncCampaign.Sync()


    ### SYNC AUDIENCES

    SyncAudience.Sync()

    print("test")
    print("test2")
    ### SYNC CLICKERS/OPENERS



    ### SYNC BOUNCES

    ### SYNC UNSUBS


    #### FINAL
    localDBConnection.close()


if __name__ == '__main__':
    main()
