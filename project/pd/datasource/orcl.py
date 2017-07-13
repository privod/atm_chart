import os

import pandas as pd
import pandas.io.sql as psql

import project.atmserv.db as db
import project.atmserv.typ as typ

os.environ['NLS_LANG'] = 'American_America.AL32UTF8'


class DataFrameReaderOrcl(db.Orcl):
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

    def get_atm_service(self, beg, end):
        df = psql.read_sql_query(
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

        df.A_TIME_BEG = df.A_TIME_BEG.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))
        df.A_TIME_END = df.A_TIME_END.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))

        return df

    def get_orders_for_idle(self, beg, end):
        df = psql.read_sql_query(
            con=self.connect,
            sql=("select o.a_number order_number, o.date_reg, o.date_end,\n"
                 "  aa.ref atm_ref, aa.serial, aa.a_number atm_number, aa.producer_name producer,\n"
                 "  aa.model_name model, aa.city, aa.addr, aa.location, aa.bank_ref,\n"
                 "  sa.ref service_ref, sa.contract_ref, sa.type service_type, sa.a_start, sa.a_finish,\n"
                 "  sa.a_recovery, sa.a_days, sa.a_time_beg, sa.a_time_end, sa.a_price, sa.a_zone\n"
                 "from r_order o\n"
                 "  inner join r_atm a on a.ref = o.atm_ref\n"
                 "  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc\n"
                 "  inner join r_service_arc sa on aa.ref = sa.atm_ref and trunc(o.date_reg/1000000) between sa.a_start and sa.a_finish\n"
                 "  inner join r_service s on s.a_serv_arc = sa.a_serv_arc\n"
                 "where 1 = 1\n"
                 "  and sa.type in (1, 2)\n"
                 "  and :beg < trunc(o.date_end/1000000)\n"
                 "  and :end > trunc(o.date_reg/1000000)\n"),
            params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
            # , parse_dates={'DATE_REG': '%Y%m%d%H%M%S'}
        )

        df.DATE_REG = pd.to_datetime(df.DATE_REG, format=typ._format_gp_datetime)
        df.DATE_END = pd.to_datetime(df.DATE_END, format=typ._format_gp_datetime)
        df.A_TIME_BEG = df.A_TIME_BEG.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))
        df.A_TIME_END = df.A_TIME_END.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))

        return df

    def get_service(self, beg, end):
        df = psql.read_sql_query(
            con=self.connect,
            sql=("select sa.ref service_ref, sa.atm_ref, sa.contract_ref, sa.type service_type,\n"
                 "  sa.a_start, sa.a_finish, sa.a_recovery, sa.a_days, sa.a_time_beg, sa.a_time_end,\n"
                 "  sa.a_price, sa.a_zone\n"
                 "from r_service s\n"
                 "  inner join r_service_arc sa on s.a_serv_arc = sa.a_serv_arc\n"
                 "where 1 = 1\n"
                 "  and sa.type in (1, 2)\n"
                 "  and :beg < sa.a_finish\n"
                 "  and :end > sa.a_start\n"),
            params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
        )

        df.A_TIME_BEG = df.A_TIME_BEG.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))
        df.A_TIME_END = df.A_TIME_END.apply(lambda gp_time: typ.gp_to_time(str(gp_time)))

        return df

    def get_atm(self):
        return psql.read_sql_query(
            con=self.connect,
            sql=("select aa.ref atm_ref, aa.serial, aa.a_number, aa.type, aa.a_producer,\n"
                 "  aa.model_name model, aa.city, aa.addr, aa.location, aa.bank_ref,\n"
                 "  aa.mon_beg, aa.mon_end, aa.tue_beg, aa.tue_end, aa.wed_beg, aa.wed_end, aa.thu_beg,\n"
                 "  aa.thu_end, aa.fri_beg, aa.fri_end, aa.sat_beg, aa.sat_end, aa.sun_beg, aa.sun_end\n"
                 "from r_atm a\n"
                 "  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc\n")
        )



    def get_orders(self, beg, end):
        df = psql.read_sql_query(
            con=self.connect,
            sql=("select o.a_number order_number, o.atm_ref,"
                 "  o.date_reg, o.date_end\n"
                 "from r_order o\n"
                 "where 1 = 1\n"
                 "  and o.service_type in (1, 2)\n"
                 "  and :beg < trunc(o.date_end/1000000)\n"
                 "  and :end > trunc(o.date_reg/1000000)\n"),
            params={'beg': typ.date_to_gp(beg), 'end': typ.date_to_gp(end)}
            # , parse_dates={'DATE_REG': '%Y%m%d%H%M%S'}
        )

        df.DATE_REG = pd.to_datetime(df.DATE_REG, format=typ._format_gp_datetime)
        df.DATE_END = pd.to_datetime(df.DATE_END, format=typ._format_gp_datetime)

        return df
