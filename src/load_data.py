"""this file is to read the input data file and create list of data to be stored in database.
In this file json file is being parsed, data hash is being create for required keys and final data list is being
returned to the caller"""

import json
import hashlib
import sys
import time
import datetime
import logging


class LoadData:
    def __init__(self):
        pass

    @staticmethod
    def get_hash(in_text):
        hash_md5 = hashlib.md5(in_text.encode())
        hex_dig = hash_md5.hexdigest()
        return hex_dig

    def read_file(self,file_path):
        try:
            user_rec = []
            track_rec = []
            dup_dic = {}
            listen_rec = []
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            with open(file_path, 'r') as fp:
                for line in fp:
                    if line:
                        line = json.loads(line)
                        if 'user_name' in line:
                            user_name = line['user_name'].replace('[', '').replace(']', '')
                            user_hash = self.get_hash(user_name)
                            user_rec.append((user_name,now,user_hash))

                        if 'track_metadata' in line:
                            if 'track_name' in line['track_metadata']:
                                if line['track_metadata']['track_name']:
                                    track_name = line['track_metadata']['track_name'].lower()
                                    track_hash = self.get_hash(track_name)
                                    release_name = line['track_metadata']['release_name']
                                    artist_name = line['track_metadata']['artist_name']
                            if 'duration_ms' in line['track_metadata']['additional_info']:
                                duration_ms = line['track_metadata']['additional_info']['duration_ms']
                                duration_ms = (duration_ms/(1000*60)) % 60
                                duration_ms = round(duration_ms, 2)
                            else:
                                duration_ms = 0
                            track_rec.append((track_name, release_name, duration_ms, artist_name, track_hash))

                            if 'listening_from' in line['track_metadata']['additional_info']:
                                listen_from = line['track_metadata']['additional_info']['listening_from']
                            else:
                                listen_from = None
                        listened_at = line['listened_at']
                        listened_at = datetime.datetime.utcfromtimestamp(listened_at)
                        recording_msid = line['recording_msid']
                        if recording_msid in dup_dic:
                            dup_dic[recording_msid] +=1
                        else:
                            dup_dic[recording_msid] = 1
                        recording_msid = recording_msid.replace('-','')
                        listen_rec.append((user_name, track_name, listened_at, listen_from, track_hash, user_hash, recording_msid))
            logging.info('number unique listen entries: {}'.format(len(dup_dic)))
            logging.info('total number listen entries: {}'.format(len(listen_rec)))
            return user_rec,track_rec,listen_rec
        except Exception as e:
            logging.exception('failure in reading file')
            sys.exit(1)


if __name__ == "__main__":
    ld = LoadData()
    # ip='../input/sample1.json'
    ip = '/Users/umesh/Downloads/dataset.txt'
    ld.read_file(ip)
