from sql_utility import SqlUtil
import csv
import datetime
import matplotlib.pyplot as plt
import numpy as np

class SolveingQueries:
    def __init__(self):
        self.top_10_users = 'select u.user_name,a.tot_listen from  (select count(listen_id) as tot_listen, user_id from' \
                            ' listens group by 2 order by 1 desc limit 10)a inner join users u on u.user_id = a.user_id'
        self.active_user_1stMarch = 'select count(user_id),date(listend_at) from listens where ' \
                                    'date(listend_at) = \'2019-03-01\' group by 2'
        self.user_1st_song = 'select u.user_name, t.track_name ,b.listend_at from  ( select user_id, track_id, ' \
                             'listend_at from (select user_id,track_id,listend_at, dense_rank() over ' \
                             '(partition by user_id order by listend_at asc) as rnk from listens)a where rnk=1 )b ' \
                             'inner join tracks t on t.track_id = b.track_id inner join users u on u.user_id = b.user_id'
        self.top_10_users_headings = ['user_name','total_listens']
        self.active_user_1stMarch_headings = ['active_user_count', 'date']
        self.user_1st_song_headings = ['user_name', 'song_name', 'listened_on']

        self.su = SqlUtil()

    def write_csv(self,data,file_name,headings):
        with open('../output/'+file_name+'.csv', 'w') as ffp:
            writer = csv.writer(ffp)
            writer.writerow(headings)
            writer.writerows(data)
            ffp.close()

    def songs_count_by_day(self, min_date, max_date):
        query = 'select count(track_id), date(listend_at) from listens where date(listend_at) between \'{}\' and \'{}\' ' \
                'group by 2 order by 2'.format(min_date,max_date)
        res = self.su.execute_query(query)
        song_count = []
        dates = []
        for r in res:
            song_count.append(r[0])
            dates.append(datetime.datetime.strftime(r[1], '%d'))
        plt.figure(figsize=(20, 6))
        plt.plot(dates, song_count)
        plt.xlabel('year'+str(min_date[0:4]+' month'+str(min_date[6:7])))
        plt.ylabel('total songs listened')
        # plt.show()
        plt.savefig('../output/graphs/songs_bydate.png')

    def songs_count_by_plat(self, min_date, max_date):
        query = 'select count(track_id), listening_from, date(listend_at) from listens where date(listend_at) between ' \
                '\'{}\' and \'{}\' group by 2,3 order by 3'.format(min_date,max_date)
        res = self.su.execute_query(query)
        data={}
        spotify_play_count = ['']
        non_spotify_count =['']
        dates=['']
        for r in res:
            day = datetime.datetime.strftime(r[2], '%d').strip()
            if day not in data:
                if r[1] =='spotify':
                    data[day] = ['spotify-{}'.format(r[0])]
                else:
                    data[day] = ['nspotify-{}'.format( r[0])]
            else:
                pd = data[day]
                if r[1] =='spotify':
                    pd.append('spotify-{}'.format(r[0]))
                else:
                    pd.append('nspotify-{}'.format(r[0]))
                data[day] = pd
        dates.extend(data.keys())
        for key in data:
            for val in data[key]:
                if 'nspotify' in val:
                    non_spotify_count.append(val.split('-')[1])
                else:
                    spotify_play_count.append(val.split('-')[1])
        w=0.4
        bar_spoti = np.arange(len(dates))
        bar_non_spoti = [i+w for i in bar_spoti]
        plt.figure(figsize=(20, 10))
        plt.bar(bar_spoti, spotify_play_count,w, label="spotify")
        plt.bar(bar_non_spoti, non_spotify_count,w, label="non-spotify")
        plt.xlabel('year' + str(min_date[0:4] + ' month' + str(min_date[6:7])))
        plt.ylabel('total listens')
        plt.title('plays by platform')
        plt.xticks(bar_spoti+w/2,dates)
        plt.legend()
        # plt.show()
        plt.savefig('../output/graphs/songs_by_plat.png')


    def top_20_songs(self):
        query = 'select a.total_plays,track_name from (select count(listen_id) total_plays, track_id from listens group by 2 order by 1 desc limit 20)a inner join tracks t on t.track_id = a.track_id;'

        res = self.su.execute_query(query)
        play_count = []
        song_names=[]
        for r in res:
            play_count.append(r[0])
            song_names.append((r[1]))
        plt.figure(figsize=(20, 6))
        plt.plot(song_names, play_count)
        plt.xlabel('song name')
        plt.ylabel('total plays')
        # plt.show()
        plt.savefig('../output/graphs/top_20_songs.png')



if __name__ == "__main__":
    sq = SolveingQueries()
    sq.songs_count_by_day('2019-01-01','2019-01-31')
    sq.songs_count_by_plat('2019-01-01', '2019-01-31')
    sq.top_20_songs()
    res = sq.su.execute_query(sq.top_10_users)
    sq.write_csv(res,'top_10_users',sq.top_10_users_headings)
    res = sq.su.execute_query(sq.active_user_1stMarch)
    sq.write_csv(res, 'Active_users_on_2019_03_01', sq.active_user_1stMarch_headings)
    res = sq.su.execute_query(sq.user_1st_song)
    sq.write_csv(res, 'first_song_by_users', sq.user_1st_song_headings)



