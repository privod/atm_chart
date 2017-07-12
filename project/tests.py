import datetime as dt

from unittest import TestCase

import project.date_range as dr


class DateRangeTestCase(TestCase):
    def test_inner_join(self):
        range_list = [
            (dt.date(2017, 5, 3), dt.date(2017, 5, 8)),
            (dt.date(2017, 5, 10), dt.date(2017, 5, 5)),
            (dt.date(2017, 5, 5), dt.date(2017, 5, 28)),
            (dt.date(2017, 5, 1), dt.date(2017, 5, 20)),
        ]

        result = dr.inner_join(range_list)
        expected = (dt.date(2017, 5, 5), dt.date(2017, 5, 8))
        self.assertEqual(result, expected)

    def test_inner_join_none(self):
        range_list = [
            (dt.datetime(2017, 5, 3), dt.datetime(2017, 5, 8)),
            (dt.datetime(2017, 5, 10), dt.datetime(2017, 5, 5)),
            (dt.datetime(2017, 5, 5), dt.datetime(2017, 5, 28)),
            (dt.datetime(2017, 5, 10), dt.datetime(2017, 5, 20)),
        ]

        result = dr.inner_join(range_list)
        expected = (None, None)
        self.assertEqual(result, expected)

    def test_outer_join(self):
        range_list = [
            (dt.date(2017, 5, 1), dt.date(2017, 5, 4)),
            (dt.date(2017, 5, 3), dt.date(2017, 5, 8)),
            (dt.date(2017, 5, 17), dt.date(2017, 5, 24)),
            (dt.date(2017, 5, 13), dt.date(2017, 5, 5)),
            (dt.date(2017, 5, 19), dt.date(2017, 5, 16)),
        ]

        result = dr.outer_join(range_list)
        expected = [
                (dt.date(2017, 5, 1), dt.date(2017, 5, 13)),
                (dt.date(2017, 5, 16), dt.date(2017, 5, 24))
        ]

        self.assertListEqual(result, expected)

    # def test_normal(self):
    #     self.assertIsNone(dr.normal(None))
