# def test_fx():
#     print("hello main")
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date



def sparkify_get_datetime(ts):
    """
    converts timestamp from miliseconds, to seconds, then to a datetime.
    Assumes input is of type int, and is a timestamp in miliseconds.
    """
    ts_seconds = ts // 1000
    new_datetime = to_date(ts_seconds)



    return new_datetime