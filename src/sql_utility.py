"""this file contains the code to execute DB queries"""

import sys
import mysql.connector
import time
import configparser
import logging

class SqlUtil:
    def __init__(self):
        self.config_path = '../static_data.ini'

    def get_conn(self):
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(self.config_path)
        user = parser['mysql']['user']
        pas = parser['mysql']['pass']
        db = parser['mysql']['db']
        conn = mysql.connector.connect(host='localhost', database=db, user=user, password=pas)
        return conn

    def read_data(self):
        pass

    def execute_query(self,query,data=None):
        try:
            res = 0
            conn = self.get_conn()
            cursor = conn.cursor()
            if data:
                cursor.executemany(query, data)
            else:
                cursor.execute(query)
            if 'select' in query:
                res = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            return res
        except Exception as e:
            logging.exception('query execution failed')
            sys.exit(1)


if __name__ == "__main__":
    parser = configparser.ConfigParser(interpolation=None)
    parser.read('../static_data.ini')
    query=parser['mysql']['user_insert']
    print(query)
    su = SqlUtil()
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    test_data=[('abc','now()'),('bcd','now()')]
    su.execute_query(query,test_data)