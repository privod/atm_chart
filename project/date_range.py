import datetime as dt


def date_list(start, stop, step=dt.timedelta(days=1), inclusive=True):
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


def transpose(range_list):
    return [item for item in zip(*range_list)]


def normal(range_list):
    return [sorted(date) for date in range_list]


def sort(range_list):
    return sorted(normal(range_list))


def inner_join(range_list):
    beg_list, end_list = transpose(normal(range_list))
    beg = max(beg_list)
    end = min(end_list)

    if beg < end:
        return beg, end

    return None, None


def outer_join(range_list):

    join_list = []
    join_beg = None
    join_end = None

    for beg, end in sort(range_list):

        if join_beg is None:
            join_beg = beg
            join_end = end
            continue

        if beg > join_end:
            join_list.append((join_beg, join_end))
            join_beg = beg

        join_end = end

    if None in [join_beg, join_end]:
        return None, None

    join_list.append((join_beg, join_end))

    return join_list


class DateRange:
    def __init__(self, beg, end):
        self.beg = beg
        self.end = end


