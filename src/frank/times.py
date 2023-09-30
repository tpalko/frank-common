from datetime import datetime 
import math
from pytz import timezone

UTC = timezone("UTC")
HERE = timezone("America/New_York")

SECOND = 1
MINUTE = 60
HOUR = MINUTE*60
DAY = HOUR*24
WEEK = DAY*7

time_units = [
    {
        'name': 'week',
        'duration': WEEK
    },
    {
        'name': 'day',
        'duration': DAY
    },
    {
        'name': 'hour',
        'duration': HOUR
    },
    {
        'name': 'minute',
        'duration': MINUTE
    },
    {
        'name': 'second',
        'duration': SECOND
    }
]

def since_humanize(date_obj):

    now = UTC.localize(datetime.utcnow())

    if not date_obj.tzinfo or str(date_obj.tzinfo) != str(UTC.zone):
        print(date_obj.tzinfo)
        date_obj = UTC.localize(date_obj)

    ret = ''
    seconds_since = (now - date_obj).total_seconds()
    parts = 0
    MAX_PARTS = 1

    for time_unit in time_units:
        if time_unit['duration'] < seconds_since:
            num_units = math.floor(seconds_since / time_unit['duration'])
            pluralizer = 's' if num_units > 1 else ''
            ret += f'{num_units} {time_unit["name"]}{pluralizer} '
            seconds_since -= num_units*time_unit['duration']    
            parts += 1
        if parts >= MAX_PARTS:
            break 
    
    ret = f'{ret} ago' if ret else 'just now'

    return ret 

def ordinal(num):
    num = int(num)
    if num > 10 and num < 14:
        return "th"
    elif str(num)[-1] == "3":
        return "rd"
    elif str(num)[-1] == "2":
        return "nd"
    elif str(num)[-1] == "1":
        return "st"
    else:
        return "th"

def date_humanize(date_obj):
    '''UTC Date() -> local Sun Jun 4th'''
    tzDate = localize_utc_date(date_obj)
    return f'{datetime.strftime(tzDate, "%a %b")} {int(datetime.strftime(tzDate, "%d"))}{ordinal(datetime.strftime(tzDate, "%d"))}'

def time_fmt(date_obj):
    '''UTC Date() -> local 12:24 pm'''
    return f'{int(datetime.strftime(localize_utc_date(date_obj), "%I"))}{datetime.strftime(localize_utc_date(date_obj), ":%M %P")}'

def parse_datestring_as_utc(date_str):
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.000Z")

def truncate_date(date_obj, truncate_to):
    if truncate_to == "day":
        return datetime.strptime(datetime.strftime(date_obj, "%Y-%m-%d"), "%Y-%m-%d")    
    raise NotImplementedError(f'truncate_date not implemented for {truncate_to}')

def utcify(naive_local_date_obj):
    '''local 2023-06-04T11:38:42.000Z -> UTC Date()'''
    return HERE.localize(naive_local_date_obj).astimezone(UTC)        

def localize_utc_date(utc_date):
    '''UTC Date() -> local Date()'''
    if not utc_date.tzinfo or str(utc_date.tzinfo) != str(UTC.zone):
        utc_date = UTC.localize(utc_date)
    return utc_date.astimezone(HERE)

# -- serializing datetime columns
def date_loc_and_fmt(date_obj):
    '''UTC Date() -> local 2023-06-04 09:24 am'''
    return datetime.strftime(localize_utc_date(date_obj), "%Y-%m-%d %I:%M %P")
