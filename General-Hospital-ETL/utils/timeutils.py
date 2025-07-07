from datetime import datetime, date

def convert_date_to_int(value):
    # date format %Y-%m-%d
    digits = value.split("-")
    return int(''.join(digits))


def get_day_of_week(value):
    date_value = datetime.strptime(value, "%Y-%m-%d").weekday()
    DAY_OF_WEEK = {
        0: "MONDAY", 1: "TUESDAY", 2: "WEDNESDAY", 3: "THURSDAY", 4: "FRIDAY", 5: "SATURDAY", 6: "SUNDAY"
    }
    return DAY_OF_WEEK[date_value]

def is_valid_datetime_format(date_string):
    DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%d/%m/%Y", "%d/%m/%Y %H:%M"]
    for datetime_format in DATETIME_FORMATS:
        try:
            datetime.strptime(date_string, datetime_format)
            return True
        except ValueError:
            pass
    return False

def get_quarter(value):
    month = datetime.strptime(value, "%Y-%m-%d").date().month
    return (month - 1) // 3 + 1 

def is_weekend(value):
    day_of_week = get_day_of_week(value)
    return True if day_of_week in ["SATURDAY", "SUNDAY"] else False 
