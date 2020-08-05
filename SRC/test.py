import json

from SRC.Utilities.MailchimpConnection import MailchimpConnection

localMCConnection = MailchimpConnection.connect()
mailchimpConnection = localMCConnection.connection
#,fields="lists.name,lists.id"
audience=mailchimpConnection.lists.all(get_all=True)

#print(json.dumps(audience, indent=2))

unsub = mailchimpConnection.lists.abuse_reports.all(list_id='80d0dbbc43', get_all=False)
unsub = mailchimpConnection.lists.segments.all(list_id='80d0dbbc43', get_all=False)
#print(json.dumps(unsub, indent=2))


lits = mailchimpConnection.lists.members.all(list_id='80d0dbbc43', get_all=True, fields="members.email_address,members.last_changed", since_last_changed="2020-08-01T22:30:22+00:00")

print(json.dumps(lits, indent=2))