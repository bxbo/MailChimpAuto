import logging

# Author BXBO
# V 0.1
# Creation : 20200726
# Update : 202000802
# Goal :
#    - Store Mailchimp Audience

# Update 20200802
# - add last_sync_date -> today
# Reformat Logging
# udpateDBLastSyncDate

from SRC.Utilities import utilities


class MailChimpAudience:
    id = None
    web_id = None
    name = None
    date_created = None
    member_count = None
    unsubscribe_count = None
    last_sub_date = None
    last_unsub_date = None
    last_sync_date = None

    def __init__(self, id=None, web_id=None, name=None, date_created=None, member_count=None, unsubscribe_count=None,
                 last_sub_date=None, last_unsub_date=None):
        self.id = id
        self.web_id = web_id
        self.name = name
        self.date_created = date_created
        self.member_count = member_count
        self.unsubscribe_count = unsubscribe_count
        self.last_sub_date = last_sub_date
        self.last_unsub_date = last_unsub_date

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        try:
            return self.id == (other.id)
        except AttributeError:
            return NotImplemented

    def __str__(self):
        return "%s : %s : %s : %s : %s : %s : %s : %s " % (self.id, self.web_id, self.name, self.date_created,
                                                           self.member_count, self.unsubscribe_count,
                                                           self.last_sub_date,
                                                           self.last_unsub_date)

    def insertDB(self, connection):

        cursor = connection.cursor()
        postgres_insert_query = """INSERT INTO public.mailchimp_audiences(id, web_id, name, date_created, member_count,
        unsubscribe_count, last_sub_date, last_unsub_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
        record_to_insert = (
            self.id, self.web_id, self.name, self.date_created, self.member_count,
            self.unsubscribe_count, self.last_sub_date, self.last_unsub_date)
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        cursor.close()
        logging.debug('MailChimpAudience : insertDB where id = %s ', self.id)

    def updateDBonID(self, connection):
        cursor = connection.cursor()

        sql_update_query = """UPDATE public.mailchimp_audiences SET web_id =%s, name =%s, date_created =%s, 
        member_count =%s, unsubscribe_count =%s, last_sub_date =%s, last_unsub_date =%s 
        WHERE id = %s;"""
        cursor.execute(sql_update_query, (self.web_id, self.name, self.date_created, self.member_count,
                                          self.unsubscribe_count, self.last_sub_date, self.last_unsub_date,
                                           self.id))
        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        logging.debug('MailChimpAudience : updateDBonID where id = %s ', self.id)
        cursor.close()

    def getAudienceOnID(self, connection, id):
        cursor = connection.cursor()
        sql_query = "SELECT * FROM public.mailchimp_audiences WHERE id =%s ;"
        cursor.execute(sql_query, (id,))
        temp = cursor.fetchone()
        self.id = temp[0]
        self.web_id = temp[1]
        self.name = temp[2]
        self.date_created = temp[3]
        self.member_count = temp[4]
        self.unsubscribe_count = temp[5]
        self.last_sub_date = temp[6]
        self.last_unsub_date = temp[7]
        self.last_sync_date = temp[8]
        logging.debug('MailChimpAudience :  getAudienceOnID where id = %s : last_sync_date = %s ', id,
                      str(self.last_sync_date))
        return self

    def updateDBLastSyncDate(self, connection, lastSyncDate):
        cursor = connection.cursor()

        sql_update_query = """UPDATE public.mailchimp_audiences SET last_sync_date=%s
                WHERE id = %s;"""
        cursor.execute(sql_update_query, (lastSyncDate, self.id))
        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        logging.debug('MailChimpAudience : udpateDBLastSyncDate where id = %s ', self.id)
        cursor.close()
