import csv
import logging

from SRC.Utilities import utilities


class MailChimpMember:
    id = None
    email_address = None
    status = None
    timestamp_signup = None
    timestamp_opt = None
    last_changed = None
    language = None
    last_unsub_date = None
    last_sync_date = None
    unique_email_id = None
    web_id = None
    audience_id = None

    def __init__(self, id=None, email_address=None, status=None, timestamp_signup=None, timestamp_opt=None,
                 last_changed=None,
                 language=None, unique_email_id=None, web_id=None, audience_id=None):
        self.id = id
        self.email_address = email_address
        self.status = status
        self.timestamp_signup = timestamp_signup
        self.timestamp_opt = timestamp_opt
        self.last_changed = last_changed
        self.language = language
        self.unique_email_id = unique_email_id
        self.web_id = web_id
        self.audience_id = audience_id

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        try:
            return self.id == (other.id)
        except AttributeError:
            return NotImplemented

    def insertDBIfNotFound(self, connection, timeStampTo):
        cursor = connection.cursor()
        sql_query = "SELECT * FROM public.mailchimp_members WHERE id =%s ;"
        cursor.execute(sql_query, (self.id,))
        temp = cursor.fetchone()
        cursor.close()
        if temp is None:
            self.insertDB(connection, timeStampTo)
        else:
            self.updateDBonID(connection, timeStampTo)

    def insertDB(self, connection, timeStampTo=None):

        cursor = connection.cursor()
        postgres_insert_query = """INSERT INTO public.mailchimp_members(
        id, email_address, status, timestamp_signup, timestamp_opt, last_changed, language, last_unsub_date, 
        last_sync_date, unique_email_id, web_id,audience_id )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        if timeStampTo is None:
            record_to_insert = (
                self.id, self.email_address, self.status, self.timestamp_signup, self.timestamp_opt, self.last_changed,
                self.language, self.last_unsub_date, utilities.currentTimeStamp(), self.unique_email_id, self.web_id,
                self.audience_id)
        else:
            record_to_insert = (
                self.id, self.email_address, self.status, self.timestamp_signup, self.timestamp_opt, self.last_changed,
                self.language, self.last_unsub_date, timeStampTo, self.unique_email_id, self.web_id,
                self.audience_id)
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        cursor.close()
        logging.debug('MAILCHIMP MEMBER INSERT TO DB ID = %s', self.id)

    def updateDBonID(self, connection, timeStampTo=None):
        cursor = connection.cursor()

        sql_update_query = """UPDATE public.mailchimp_members
        SET email_address=%s, status=%s, timestamp_signup=%s, timestamp_opt=%s, last_changed=%s, language=%s, 
        last_unsub_date=%s, last_sync_date=%s, unique_email_id=%s, web_id=%s, audience_id=%s
        WHERE id = %s ;"""
        if timeStampTo is None:
            cursor.execute(sql_update_query,
                           (self.email_address, self.status, self.timestamp_signup, self.timestamp_opt,
                            self.last_changed, self.language, self.last_unsub_date,
                            utilities.currentTimeStamp(), self.unique_email_id, self.web_id,
                            self.audience_id, self.id))
        else:
            cursor.execute(sql_update_query,
                           (self.email_address, self.status, self.timestamp_signup, self.timestamp_opt,
                            self.last_changed, self.language, self.last_unsub_date,
                            timeStampTo, self.unique_email_id, self.web_id,
                            self.audience_id, self.id))
        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        logging.debug('MAILCHIMP MEMBER UPDATE DB ID = %s', self.id)
        cursor.close()


    def getAllMember(connection):
        sql_query = "SELECT * FROM public.mailchimp_members"
        allResult = []
        try:
            cursor = connection.cursor()
            cursor.execute(sql_query)
            for c in cursor.fetchall():
                currentMember = MailChimpMember(c[0],
                                                c[1],
                                                c[2],
                                                c[3],
                                                c[4],
                                                c[5],
                                                c[6],
                                                c[9],
                                                c[10],
                                                c[11])
                allResult.append(currentMember)
            cursor.close()
        except:
            print('MailChimpMember : getAllMember : error on select')
        logging.debug('MailChimpMember : getAllMember : number of members retreived : %s', len(allResult))
        return allResult

    def createFileNew(memberList, filename):
        with open(filename, 'w', newline='' ) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['id', 'email_address', 'status', 'timestamp_signup', 'timestamp_opt', 'last_changed',
                             'language', 'unique_email_id', 'web_id', 'audience_id'])
            for mem in memberList:
                writer.writerow([mem.id,mem.email_address,mem.status,mem.timestamp_signup,mem.timestamp_opt,
                                 mem.last_changed,mem.language,mem.unique_email_id,mem.web_id,mem.audience_id])

    def __str__(self):
        return "%s : %s : %s : %s : %s : %s : %s : %s : %s : %s : %s : %s " % (self.id,
                                                                               self.email_address,
                                                                               self.status,
                                                                               self.timestamp_signup,
                                                                               self.timestamp_opt,
                                                                               self.last_changed,
                                                                               self.language,
                                                                               self.last_unsub_date,
                                                                               self.last_sync_date,
                                                                               self.unique_email_id,
                                                                               self.web_id,
                                                                               self.audience_id)
