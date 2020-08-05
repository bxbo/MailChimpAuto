from datetime import datetime

import dateutil.parser

def intersectionList(lst1, lst2):
    return list(set(lst1) & set(lst2))

def dateSQL(date):
    if date == '' or date is None :
        return None
    else:
        return dateutil.parser.parse(date)

def currentTimeStamp():
    return datetime.now()