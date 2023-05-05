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

    ret = ''
    seconds_since = (datetime.utcnow() - date_obj).total_seconds()
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
    tzDate = UTC.localize(date_obj).astimezone(HERE)
    return f'{datetime.strftime(tzDate, "%a %b")} {int(datetime.strftime(tzDate, "%d"))}{ordinal(datetime.strftime(tzDate, "%d"))}'

def time_fmt(date_obj):
    return f'{int(datetime.strftime(UTC.localize(date_obj).astimezone(HERE), "%I"))}{datetime.strftime(UTC.localize(date_obj).astimezone(HERE), ":%M %P")}'

def utcify(date_str):
    return HERE.localize(datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.000Z")).astimezone(UTC)        

def date_loc_and_fmt(date_obj):
    return datetime.strftime(UTC.localize(date_obj).astimezone(HERE), "%Y-%m-%d %I:%M %P")
