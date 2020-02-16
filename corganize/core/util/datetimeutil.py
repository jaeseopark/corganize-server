import datetime
import time


def get_posix_now():
    return to_posix(datetime.datetime.now())


def to_posix(dt_obj: datetime.datetime) -> int:
    return int(time.mktime(dt_obj.timetuple()))
