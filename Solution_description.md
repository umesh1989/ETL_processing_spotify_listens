**Section1**

**Creating database**

I have used mysql as it was already installed in my machine hence it was handy.
Tables created as part of this assignment:

    mysql> show tables;
    +--------------------+
    | Tables_in_scalable |
    +--------------------+
    | listen_temp        |
    | listens            |
    | tracks             |
    | users              |
    +--------------------+

User table contains the user information. There is going to be one entry for every user we get recording for
sample:

    mysql> select * from users limit 3;
    +---------+--------------+---------------------+------------------------------------+
    | user_id | user_name    | added_on            | user_hash                          |
    +---------+--------------+---------------------+------------------------------------+
    |       1 | NichoBI      | 2022-11-08 18:53:34 | 0x39393264633666313266376336336131 |
    |    2959 | Bezvezenator | 2022-11-08 18:53:34 | 0x34303062623236356662656634393663 |
    |    4379 | andrew_j_w   | 2022-11-08 18:53:34 | 0x63313534383764653030633166323061 |
    +---------+--------------+---------------------+------------------------------------+
user hash is created on username(with the assumption that username is going to be unique). user hash is being used to handle duplicate user entries.

**Tracks table**

This table is going to contain every single track,(with the assumption that track name is going to be unique). 

sample:

    +----------+-----------------------------+--------------+---------------+---------------+------------------------------------+
    | track_id | track_name                  | release_name | duration_mins | artist_name   | track_hash                         |
    +----------+-----------------------------+--------------+---------------+---------------+------------------------------------+
    |        1 | love in the time of ecstacy | Good News    |             0 | Withered Hand | 0x62356530326133303563626335363630 |
    |        2 | cornflake                   | Good News    |             0 | Withered Hand | 0x32343865633066626234663631613564 |
    +----------+-----------------------------+--------------+---------------+---------------+------------------------------------+

here also we have track_hash to handle duplicate inserts.

**NOTE:** We can create a separate artist table and release table also and then map everything there with their primary keys(which I have not done in this implementation).

**Listen_temp table:**

listen temp table is an intermediate table where initially we will store all the data related to an entry in file and the map it with tracks and user to populate there ids do be stored in final listen table.
This table will be truncated after every execution after data is loaded in the final listen table.

    mysql> desc listen_temp;
    +----------------+--------------+------+-----+---------+----------------+
    | Field          | Type         | Null | Key | Default | Extra          |
    +----------------+--------------+------+-----+---------+----------------+
    | listen_id      | int          | NO   | PRI | NULL    | auto_increment |
    | user_id        | int          | YES  |     | NULL    |                |
    | user_name      | varchar(30)  | YES  |     | NULL    |                |
    | track_id       | int          | YES  |     | NULL    |                |
    | track_name     | varchar(100) | YES  |     | NULL    |                |
    | listend_at     | datetime     | YES  |     | NULL    |                |
    | listening_from | varchar(30)  | YES  |     | NULL    |                |
    | track_hash     | binary(16)   | YES  |     | NULL    |                |
    | user_hash      | binary(16)   | YES  |     | NULL    |                |
    | listen_hash    | binary(16)   | YES  | UNI | NULL    |                |
    +----------------+--------------+------+-----+---------+----------------+

**Listens table:**

Listen table is the final table where we are going to store all the mapped information and we are going to use this table to derive all the queries.

    mysql> select * from listens limit 2;
    +-----------+---------+----------+---------------------+----------------+------------------------------------+
    | listen_id | user_id | track_id | listend_at          | listening_from | listen_hash                        |
    +-----------+---------+----------+---------------------+----------------+------------------------------------+
    |         1 |       1 |        1 | 2019-04-15 00:02:40 | NULL           | 0x31653162326161306232646234326564 |
    |         2 |       1 |        2 | 2019-04-14 23:59:38 | NULL           | 0x32383330363263383735653234303661 |
    +-----------+---------+----------+---------------------+----------------+------------------------------------+

_Listen hash is created on recording_msid._

**create table queries for all the above tables.**

    create table users (user_id int not null auto_increment, user_name varchar(30), added_on datetime, user_hash binary(16), primary key (user_id), UNIQUE(user_hash));
    create table tracks (track_id int not null auto_increment, track_name varchar(100), release_name varchar(100),duration_mins float,artist_name varchar(200) ,track_hash binary(16), primary key (track_id), UNIQUE(track_hash));
    create table listen_temp (listen_id int not null auto_increment, user_id int,  user_name varchar(30), track_id int, track_name varchar(100), listend_at datetome, listening_from varchar(30),track_hash binary(16), user_hash binary(16), listen_hash binary(16), primary key (listen_id), UNIQUE(listen_hash));
    create table listens (listen_id int not null auto_increment, user_id int, track_id int, listend_at datetime, listening_from varchar(30), listen_hash binary(16), primary key (listen_id), UNIQUE(listen_hash));

**Note:** 
using hash values gives us edge while primarily loading data and updating data in listen table.
This process took 3,3.5 for processing 168302 records for user_ids and tracks_ids respectively.

**PS.** I assued the recording_msid would be unique for all the entries which is true, but there are duplicate records for two different recording_msids.
Currently, duplicate are handled by hash_of recording_msids. However, combine hash of user_name+listened_at can be used to produce entirely unique records(I have not implemented the user_name+listened_at hash approach in this.). 


**Section 2**

2.a:
The data that was provided contained couple of important fields like usernname, track_name, release_name, artist name etc..
all the keys can  be seen in the data also.

Following are the major key points(there can be many more):
1. who listened what?
2. when the song was listened.
3. artist who created the song.

**Couple of game changing attributes:**

**Tags:** There is a key for tags also, however, there was no data in the that key. Key tags can be populated we can use that to predict which tags =get listened more and can be used to promote a song.

**Duration:** this can help us in knowing songs of which duration are getting more listens.

**listening_from:** listening from is another key that is present, but we only have two values in it, spotify and null. if we can get more data in this we can target customer in a better way.

2.b:

As in the question it has been specifically asked to create pandas dataframe, for that either we need to read entire table in pandas which will a bit slow compared to processing data in spark dataframe
For the available data the tables that I have created are sufficient for answer most of the queries. We do not need a separate table unless we have a specific need to consume one table for reporting needs.
Below written queries can create dataframe in spark to answer:

**1. user listened how many song from which platform on a given day:**

    select user_id, listening_from,listend_at, count(listening_from), count(track_id) from listens group by 1,2,3;

Now if we take the example of one user results looks like below(filtering result for one user for the sake of example):

    mysql> select user_id, listening_from,date(listend_at), count(listening_from), count(track_id) from listens where user_id = 6853 group by 1,2,3;
    +---------+----------------+------------------+-----------------------+-----------------+
    | user_id | listening_from | date(listend_at) | count(listening_from) | count(track_id) |
    +---------+----------------+------------------+-----------------------+-----------------+
    |    6853 | NULL           | 2019-04-14       |                     0 |              62 |
    |    6853 | NULL           | 2019-04-13       |                     0 |               9 |
    |    6853 | NULL           | 2019-04-12       |                     0 |              33 |
    |    6853 | NULL           | 2019-04-10       |                     0 |              19 |
    |    6853 | NULL           | 2019-04-09       |                     0 |               1 |
    |    6853 | NULL           | 2019-04-07       |                     0 |              55 |
    |    6853 | NULL           | 2019-04-06       |                     0 |               4 |
    |    6853 | spotify        | 2019-04-05       |                     1 |               1 |
    |    6853 | NULL           | 2019-04-02       |                     0 |               7 |
    |    6853 | NULL           | 2019-04-01       |                     0 |               2 |
    |    6853 | NULL           | 2019-03-31       |                     0 |              14 |
    |    6853 | NULL           | 2019-03-30       |                     0 |              27 |
    |    6853 | NULL           | 2019-03-27       |                     0 |               8 |
    |    6853 | NULL           | 2019-03-25       |                     0 |              45 |
    |    6853 | NULL           | 2019-03-24       |                     0 |              29 |
    |    6853 | NULL           | 2019-03-22       |                     0 |              24 |
    |    6853 | NULL           | 2019-03-18       |                     0 |               5 |
    |    6853 | NULL           | 2019-03-17       |                     0 |              23 |
    |    6853 | NULL           | 2019-03-16       |                     0 |               5 |
    |    6853 | NULL           | 2019-03-15       |                     0 |               5 |
    |    6853 | NULL           | 2019-03-14       |                     0 |              32 |
    |    6853 | NULL           | 2019-03-10       |                     0 |              24 |
    |    6853 | NULL           | 2019-03-09       |                     0 |              22 |
    |    6853 | NULL           | 2019-03-07       |                     0 |               1 |
    |    6853 | NULL           | 2019-03-06       |                     0 |               5 |
    |    6853 | NULL           | 2019-03-05       |                     0 |               2 |
    |    6853 | NULL           | 2019-03-04       |                     0 |               6 |
    |    6853 | NULL           | 2019-03-02       |                     0 |              16 |
    |    6853 | NULL           | 2019-02-23       |                     0 |              27 |
    |    6853 | NULL           | 2019-02-20       |                     0 |              13 |
    |    6853 | NULL           | 2019-02-19       |                     0 |               9 |
    |    6853 | NULL           | 2019-02-18       |                     0 |               6 |
    |    6853 | NULL           | 2019-02-17       |                     0 |               4 |
    |    6853 | NULL           | 2019-02-12       |                     0 |               1 |
    |    6853 | NULL           | 2019-02-10       |                     0 |              80 |
    |    6853 | NULL           | 2019-02-09       |                     0 |               5 |
    |    6853 | NULL           | 2019-02-08       |                     0 |              27 |
    |    6853 | NULL           | 2019-02-06       |                     0 |              11 |
    |    6853 | NULL           | 2019-02-05       |                     0 |              19 |
    |    6853 | NULL           | 2019-02-04       |                     0 |              49 |
    |    6853 | NULL           | 2019-02-03       |                     0 |               2 |
    |    6853 | NULL           | 2019-02-01       |                     0 |               1 |
    |    6853 | NULL           | 2019-01-24       |                     0 |              18 |
    |    6853 | spotify        | 2019-01-23       |                     1 |               1 |
    |    6853 | NULL           | 2019-01-23       |                     0 |               2 |
    |    6853 | NULL           | 2019-01-22       |                     0 |              26 |
    |    6853 | NULL           | 2019-01-20       |                     0 |               1 |
    |    6853 | NULL           | 2019-01-14       |                     0 |               2 |
    +---------+----------------+------------------+-----------------------+-----------------+

I did not do a spark implementation as there can be N number of uses-cases to run queries for, and I believe queries submitted by me in the assignment are enough to judge my sql capabilities.

for the section **2.C** I wrote the script solving_queries.py where I executed below queries and stored the result in csv files in output dir. This will help in data consumption for any reporting need.

2.c.1

    select u.user_name,a.tot_listen from  (select count(listen_id) as tot_listen, user_id from listens group by 2 order by 1 desc limit 10)a inner join users u on u.user_id = a.user_id
2.c.2

    select count(user_id) from listens where date(listend_at) = '2019-03-01';

2.c.3

    select b.user_id,b.listend_at, t.track_name,u.user_name from  ( select user_id, track_id, listend_at from (select user_id,track_id,listend_at, dense_rank() over (partition by user_id order by listend_at asc) as rnk from listens)a where rnk=1 limit 10)b inner join tracks t on t.track_id = b.track_id inner join users u on u.user_id = b.user_id;

**Section 3:**

Couple of important metrics:
1. count of songs got listened in a day for a month
2. songs count by platform
3. top 20 songs of all time.
All the above graphs are getting created in solving_queries.py and getting stored in ../output/graphs

**PS.** 
I am not good with visualisation, so please pardon the poor graphs. However, _I can write sql(simple to complex) as and when it is needed to provide any type of data for metrics._

**Couple of other metrics that I will add is:**
1. song_count by tags(this will tell us, how songs are doing with the tags).
2. word cloud of tags(this will give us famous tags).

Above two metrics will help us in getting more listens for a specific song.


**Time taken to complete this entire assignment:**

I took roughly around 12-15 hours. I worked on this for 5 days spending approximately 2-3 hours a day.