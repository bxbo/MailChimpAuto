import logging


class MailChimpCampaign:
    id = None
    web_id = None
    status = None
    emails_sent = None
    send_time = None
    unique_opens = None
    open_rate = None
    clicks = None
    click_rate = None

    def __init__(self, id, web_id, status, emails_sent, send_time, unique_opens, open_rate, clicks, click_rate):
        self.id = id
        self.web_id = web_id
        self.status = status
        self.emails_sent = emails_sent
        self.send_time = send_time
        self.unique_opens = unique_opens
        self.open_rate = open_rate
        self.clicks = clicks
        self.click_rate = click_rate

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        try:
            return self.id == (other.id)
        except AttributeError:
            return NotImplemented

    def insertDB(self, connection):

        cursor = connection.cursor()
        postgres_insert_query = """INSERT INTO mailchimp_campaigns(id, web_id, status, emails_sent, send_time, 
        unique_opens, open_rate, clicks, click_rate ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); """
        record_to_insert = (
            self.id, self.web_id, self.status, self.emails_sent, self.send_time,
            self.unique_opens, self.open_rate,
            self.clicks, self.click_rate)
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        cursor.close()
        logging.debug('inserting to database')

    def updateDBonID(self, connection):
        cursor = connection.cursor()
        sql_update_query = """UPDATE public.mailchimp_campaigns SET web_id=%s, status=%s, emails_sent=%s, 
        send_time=%s, unique_opens=%s, open_rate=%s, clicks=%s, click_rate=%s WHERE id = %s ; """
        cursor.execute(sql_update_query, (self.web_id, self.status, self.emails_sent, self.send_time,
                                          self.unique_opens, self.open_rate, self.clicks, self.click_rate, self.id))
        connection.commit()
        count = cursor.rowcount
        #### TO_DO -> CHECKS AND ERROR
        logging.debug('update')
        cursor.close()
