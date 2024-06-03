import cx_Oracle

import os

from dotenv import load_dotenv

from helpers.query_formatter import QueryFormatter

os.environ["LD_LIBRARY_PATH"] = "/opt/instantclient_21_13"
load_dotenv()


class DbOracleConfig:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.query_formatter = QueryFormatter()

    def execute(self, query, params=({})):
        try:
            username = os.environ.get('ORACLE_USERNAME')
            password = os.environ.get('ORACLE_PASSWORD')
            dsn = os.environ.get('ORACLE_DSN')

            self.connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
            self.cursor = self.connection.cursor()

            formatted_query = query.strip().replace("\n", "")

            self.cursor.execute(formatted_query, params)

            if formatted_query.split(' ')[0].upper() == 'SELECT' or formatted_query.split(' ')[0].upper() == 'WITH':
                result = self.cursor.fetchall()

                status, formatted_result = self.query_formatter.execute(result, self.cursor)

            else:
                formatted_result = self.cursor.rowcount
                self.connection.commit()

                status = True

            return status, formatted_result
        except Exception as exc:

            return False, {
                "status": False,
                "message": str(exc),
                "result": None,
                "code": 500
            }
        finally:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
