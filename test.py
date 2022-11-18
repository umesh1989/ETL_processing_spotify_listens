# # import hashlib
# # import time
# # import datetime
# # import mysql.connector
# # #
# # #.strftime('%Y-%m-%d %H:%M:%S')
# #
# # tt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1555286560))
# # ts = str(datetime.datetime.utcfromtimestamp(1555286560))
# # print(tt)
# # print(ts)
# # print(type(ts))
# #
# # # q = 'insert into test (name) values (%s)'
# # # dd = [['abc'],['bcd']]
# # # print(dd)
# # # # aa = [{'name':'abc','age':23},{'name':'bcd','age':33}]
# # # conn = mysql.connector.connect(host='localhost', database='scalable', user='root', password='umesh_123')
# # # cursor = conn.cursor()
# # # res = cursor.executemany(q, dd)
# # # conn.commit()
# # # cursor.close()
# # # conn.close()
# #
# # #
# # # a = ['abc','cdff','ccsdd','ccsdd']
# # # #
# # # for w in a:
# # #     hash_object = hashlib.md5(w.encode())
# # #     hex_dig = hash_object.hexdigest()
# # #     hex_dig_int = int(hash_object.hexdigest(),16)
# # #     print(hex_dig)
# # #     print(len(hex_dig))
# # #     print(hex_dig_int)
# # # #
# # # #     # 191415658344158766168031473277922803570
# # #
# # # tt = 'hshsh/hshshs'
# # #
# # #
# # # # print(tts)
# #
# # # if 'artist_name' in line['track_metadata']:
# # #     artist_name = line['track_metadata']['artist_name']
# # #     print(artist_name)
# # #     list_art=[]
# # #     if '&' in artist_name:
# # #         list_art = artist_name.split('&')
# # #     elif '/' in artist_name:
# # #         list_art = artist_name.split('/')
# # #     elif ',' in artist_name:
# # #         list_art =  artist_name.split(',')
# # #     if len(list_art) >0:
# # #         for art in list_art:
# # #             artist_rec.append((art,now))
# # #     else:
# # #         artist_rec.append((artist_name,now))
# # # print(track_rec)
# #
# # # s = 'Love In the Time of Ecstacy'
# # # s1 = 'Love in the Time of Ecstacy'
# # #
# # # print(s1.lower(), s.lower())
# #
# #
# # import random
# # data = random.sample(range(0, 2444),32)
# # # random.shuffle(data)
# # print(data)
# # data[0] = 0
# # data[-1] = 2444
# # data.sort()
# # print(data)
# # print(len(data))
#
# arr = [3,6,2,9,-1,10]
# root =
# # left = []
# # right = []
# # largets = ''
# #
# # f_list = list(filter(lambda x: x>=0, arr))
# # print(f_list)
# #
# # for a in range(1,len(f_list)):
# #     if a%2==0:
# #         right.append(f_list[a])
# #     else:
# #         left.append(f_list[a])
# # print('left branch')
# # print(left)
# # print(sum(left))
# # print('right branch')
# # print(right)
# # print(sum(right))
# #
# # if sum(left)>sum(right):
# #     largets = 'left'
# # elif sum(left)<sum(right):
# #     largets = 'right'
# #
# # print(largets)

num = 21

res = "{0:b}".format(21)
count=0
for r in res:
    if r =='1':
       count+=1

print(res,type(res))

print(count)