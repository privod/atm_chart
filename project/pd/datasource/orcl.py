import logging
import os

import pandas.io.sql as psql

import project.atmserv.db as db
import project.atmserv.typ as typ

os.environ['NLS_LANG'] = 'American_America.AL32UTF8'


class DataFrameOrcl(db.Orcl):
    def __init__(self, user='privod_ust_atm_new', password='121', dns='orcl'):
        super().__init__(user, password, dns)

    def get_test(self, date):
        return psql.read_sql_query(
            con=self.connect,
            sql=("select * from r_atm a\n"
                 "  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc\n"
                 "  inner join r_service_arc sa on aa.ref = sa.atm_ref\n"
                 "  inner join r_service s on s.a_serv_arc = sa.a_serv_arc\n"
                 "  inner join r_order o on o.service_type in (1, 2)\n"
                 "    and o.atm_ref = a.ref\n"
                 "    and :d between trunc(o.date_reg/1000000) and trunc(o.date_end/1000000)\n"
                 "where 1 = 1\n"
                 "  and sa.type in (1, 2)\n"
                 "  and :d between sa.a_start and a_finish"),
            params={'d': typ.date_to_gp(date)}
        )

    def get_service(self, beg, end):
        return psql.read_sql_query(
            con=self.connect,
            sql=("select sa.* from r_service s\n"
                 "  inner join r_service_arc sa on s.a_serv_arc = sa.a_serv_arc\n"
                 "where 1 = 1\n"
                 "  and sa.type in (1, 2)\n"
                 "  and :beg < sa.a_finish\n"
                 "  and :end > sa.a_start\n"),
            params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
        )

    def get_atm(self, beg, end):
        return psql.read_sql_query(
            con=self.connect,
            sql=("\n"
                 "select aa.ref atm_ref, aa.serial, aa.a_number, aa.type, aa.producer_name producer,\n"
                 "  aa.model_name model, aa.city, aa.addr, aa.location, aa.bank_ref,\n"
                 "  sa.ref service_ref, sa.contract_ref, sa.type service_type, sa.a_start, sa.a_finish,\n"
                 "  sa.a_recovery, sa.a_days, sa.a_time_beg, sa.a_time_end, sa.a_price, sa.a_zone\n"
                 "from r_atm a       \n"
                 "  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc\n"
                 "  inner join r_service_arc sa on aa.ref = sa.atm_ref\n"
                 "  inner join r_service s on s.a_serv_arc = sa.a_serv_arc\n"
                 "where 1 = 1\n"
                 "  and sa.type in (1, 2)\n"
                 "  and :beg < a_finish\n"
                 "  and :end > sa.a_start"),
        params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
        )

    def get_orders(self, beg, end):
        df = psql.read_sql_query(
            con=self.connect,
            sql=("select * from r_order o\n"
                 "where 1 = 1\n"
                 "  and o.service_type in (1, 2) \n"
                 "  and :beg < trunc(o.date_end/1000000)\n"
                 "  and :end > trunc(o.date_reg/1000000)"),
            params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
            # , parse_dates={'DATE_REG': '%Y%m%d%H%M%S'}
        )

        df.DATE_REG = typ.gp_to_datetime(df.DATE_REG)
        df.DATE_END = typ.gp_to_datetime(df.DATE_END)
        return df
