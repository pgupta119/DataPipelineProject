import isodate


def get_mins(str_duration):
    try:
        minutes = isodate.parse_duration(str_duration).total_seconds() / 60
        return minutes
    except (isodate.ISO8601Error, ValueError):
        raise ValueError("duration field is empty")

