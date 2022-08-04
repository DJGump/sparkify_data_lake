# def test_fx():
#     print("hello main")
from datetime import datetime



def sparkify_get_datetime(ts):
    """
    converts timestamp from miliseconds, to seconds, then to a datetime.
    Assumes input is of type int, and is a timestamp in miliseconds.
    """
    ts_seconds = ts // 1000
    new_datetime = datetime.fromtimestamp(ts_seconds)



    return new_datetime