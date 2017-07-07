import datetime as dt


def date_range(start, stop, step=dt.timedelta(days=1), inclusive=True):
    # inclusive=False to behave like range by default
    if step.days > 0:
        while start < stop:
            yield start
            start = start + step
    elif step.days < 0:
        while start > stop:
            yield start
            start = start - step
    if inclusive and start == stop:
        yield start
