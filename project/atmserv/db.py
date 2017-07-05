import logging
import os

import cx_Oracle

os.environ['NLS_LANG'] = 'American_America.AL32UTF8'


class Orcl:
    def __init__(self, user, password, dns):
        try:
            self.connect = cx_Oracle.connect(user, password, dns)
        except cx_Oracle.DatabaseError as info:
            logging.error("DB logon  error: {}".format(info))
            exit(0)
        self.cursor = self.connect.cursor()

        def __del__(self):
            self.connect.close()

    def sql_exec(self, sql, params):
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
        except cx_Oracle.DatabaseError as info:
            logging.error(
                'SQL error: {}\n'
                '\tSQL execute: {}\n'
                '\tParams: {}'
                    .format(info, self.cursor.statement, params)
            )
