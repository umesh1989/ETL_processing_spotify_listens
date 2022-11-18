"""this file is to drive the entire execution. i.e reading the input file, creating data sets and then triggering the
data ingestion in the data table"""

from sql_utility import SqlUtil
from load_data import LoadData
import configparser
import logging
import datetime
import sys

class Driver:
    def __init__(self):
        self.ld = LoadData()
        self.sq = SqlUtil()
        self.config_path = '../static_data.ini'

    @staticmethod
    def initiate_data_load(data, data_type):
        query = parser['mysql'][data_type]
        logging.info(query)
        res = dd.sq.execute_query(query, data)
        return res

    @staticmethod
    def poplate_listen_recs():
        bulk_queries=[]
        bulk_queries.append(parser['mysql']['update_users'])
        bulk_queries.append(parser['mysql']['update_tracks'])
        bulk_queries.append(parser['mysql']['insert_listen'])
        bulk_queries.append(parser['mysql']['truncate_temp'])
        for query in bulk_queries:
            logging.info(query)
            res = dd.sq.execute_query(query)
            logging.info('data load exited with status: {}'.format(res))


if __name__ == "__main__":
    logging.basicConfig(filename='../logs/exec_logs_' + datetime.datetime.now().strftime("%Y-%m-%d %H-%m-%S") + '.log',
                        format='%(asctime)s - %(message)s',level=logging.INFO)
    try:
        logging.info('execution started')
        dd = Driver()
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(dd.config_path)
        logging.info('reading input data file')
        file_path = parser['input']['input_path']
        data = dd.ld.read_file(file_path)
        users = data[0]
        tracks = data[1]
        listens = data[2]
        msg = 'total user: {}, total tracks: {}, total listen entries: {}'.format(len(users),len(tracks),len(listens))
        logging.info(msg)
        logging.info('initiating data load')
        user_res = dd.initiate_data_load(users,'user_insert')
        logging.info('user data load exited with status:{}'.format(user_res))
        track_res = dd.initiate_data_load(tracks, 'track_insert')
        logging.info('track data load exited with status:{}'.format(track_res))
        listen_res = dd.initiate_data_load(listens, 'listen_temp_insert')
        logging.info('listen data load exited with status:{}'.format(listen_res))
        logging.info('initiating listen data processing')
        dd.poplate_listen_recs()
        logging.info('program completed successfully')
    except Exception as e:
        logging.exception('program failed')
        sys.exit(1)


