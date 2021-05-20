import time

import psycopg2
import psycopg2.extras


class Connection:
    def __init__(self, conn_info):
        self.dbname = conn_info['dbname']
        self.host = conn_info['host']
        self.user = conn_info['user']
        self.port = conn_info.get('port', 5432)
        self.passwd = conn_info['password']
        self.connect = None
        self.cursor = None
        self.last_connection = None

    def connect_to_redshift(self):
        time.sleep(2)
        if self.last_connection == str(self.dbname) + str(self.host) + str(self.user) + str(self.passwd):
            return self.cursor, self.connect
        connect = psycopg2.connect(dbname=self.dbname,
                                   host=self.host,
                                   port=self.port,
                                   user=self.user,
                                   password=self.passwd)

        cursor = connect.cursor()
        self.connect = connect
        self.cursor = cursor
        self.last_connection = str(self.dbname) + str(self.host) + str(self.user) + str(self.passwd)
        return cursor, connect

    def execute_and_commit(self, statement):
        cursor, connect = self.connect_to_redshift()

        try:
            cursor.execute(statement)
            connect.commit()
        except Exception as e:
            # self.log.error(e)
            print("Something went wrong: {}".format(e))
            connect.rollback()
            self.last_connection = None
            raise
        # self.log.info("Statement %s executed" % statement)

    def execute_and_fetch(self, statement):
        cursor, connect = self.connect_to_redshift()

        try:
            cursor.execute(statement)
            result = cursor.fetchone()
            return result
        except Exception as e:
            # self.log.error(e)
            print("Something went wrong: {}".format(e))
            raise

    def table_columns(self, table_schema, table_name):
        cursor, connect = self.connect_to_redshift()

        where_dict = {"table_schema": table_schema, "table_name": table_name}

        cursor = connect.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length
                          FROM information_schema.columns
                          WHERE table_schema = %(table_schema)s
                          AND table_name   = %(table_name)s
                          ORDER BY ordinal_position""", where_dict)

        columns_cur = cursor.fetchall()

        columns = []
        for row in columns_cur:
            columns.append(row["column_name"])

        cursor.close()

        return columns
