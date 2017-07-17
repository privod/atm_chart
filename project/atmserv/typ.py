import datetime as dt
import time as tm

_format_gp_date = '%Y%m%d'
_format_gp_time = '%H%M'
_format_gp_datetime = '%Y%m%d%H%M%S'


def datetime_to_gp(datetime):
    return datetime.strftime(_format_gp_datetime)


def date_to_gp(date):
    return date.strftime(_format_gp_date)


def time_to_gp(time):
    return time.strftime(_format_gp_time)


def gp_to_date(gp_date):
    return dt.datetime.strptime(gp_date, _format_gp_date)


def gp_to_time(gp_time):

    if int(gp_time) > 2359:
        return dt.time(23, 59)

    gp_time = gp_time.rjust(4, '0')

    struct_time = tm.strptime(gp_time, _format_gp_time)
    return dt.time(struct_time.tm_hour, struct_time.tm_min, struct_time.tm_sec)


def gp_to_datetime(gp_datetime):
    return dt.datetime.strptime(gp_datetime, _format_gp_datetime)
