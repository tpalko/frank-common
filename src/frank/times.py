from datetime import datetime, timedelta, UTC
import math
from pytz import timezone
import cowpy 

logger = cowpy.getLogger()

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

    now = datetime.now(UTC)

    if not date_obj.tzinfo or str(date_obj.tzinfo) != str(UTC.zone):
        # print(date_obj.tzinfo)
        date_obj = date_obj.astimezone(UTC)

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

def utc_date_boundaries(day_offset, tz):

    THERE = timezone(tz)

    # 7/7 2 am (7/6 9 pm local)
    utc_today = datetime.now(UTC)
    loc_today = utc_today.astimezone(THERE) # THERE.localize(utc_today)
    # loc_today = THERE.localize(utc_today)
    hour_offset = loc_today.utcoffset().total_seconds() / (60*60)
    # utc_today_str = datetime.strftime(utc_today, "%Y-%m-%d")
    loc_today_str = datetime.strftime(loc_today, "%Y-%m-%d")
    origin_applied_at_midnight = datetime.strptime(f'{loc_today_str}T00:00:00', "%Y-%m-%dT%H:%M:%S")
    applied_at_midnight_utc = origin_applied_at_midnight + timedelta(days=int(day_offset))
    
    # applied_at_midnight_str = datetime.strftime(applied_at_midnight, "%Y-%m-%d")    
    # logger.debug(f'getting points -- offset {day_offset} ({applied_at_midnight_str})')
    
    # -- with UTC values in the database, what we want to say is 
    # -- where applied_at between <chosen date> +0500 and <chosen date + 1> +0500
    # where_applied_at = datetime.strftime(applied_at_date, "%Y-%m-%d")
    # where.update({'date(applied_at)': where_applied_at})

    start_boundary = applied_at_midnight_utc - timedelta(hours=hour_offset)
    end_boundary = applied_at_midnight_utc + timedelta(days=1) - timedelta(hours=hour_offset)

    return start_boundary, end_boundary, applied_at_midnight_utc

def date_humanize(date_obj):
    '''UTC Date() -> local Sun Jun 4th'''
    # tzDate = date_obj
    # if not tzDate.tzinfo:
    #     tzDate = localize_utc_date(date_obj)
    # %a %b = Mon Apr
    # %d = 8
    # ordinal(%d) = th
    # Mon Apr 8th
    return f'{datetime.strftime(date_obj, "%a %b")} {int(datetime.strftime(date_obj, "%d"))}{ordinal(datetime.strftime(date_obj, "%d"))}'

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
    '''
        UTC Date() -> local Date()
        if passed a naive date, no change is made - this effectively assumes an error has been made in passing an already-localized object
        the only problem case here is passing a value that is a UTC date but isn't aware it is UTC        
    '''
    if not utc_date.tzinfo or str(utc_date.tzinfo) != str(UTC):
        utc_date = utc_date.astimezone(UTC)
    return utc_date.astimezone(HERE)

# -- serializing datetime columns
def date_loc_and_fmt(date_obj):
    '''UTC Date() -> local 2023-06-04 09:24 am'''
    return datetime.strftime(localize_utc_date(date_obj), "%Y-%m-%d %I:%M %P")
