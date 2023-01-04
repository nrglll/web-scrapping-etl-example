# extracting nba stats with web scrapping and creating an ETL process to upload it into a database 

# import required moduls
import requests
import pandas as pd
import json
import psycopg2
import os

# Player Table

# create an empty dictionary for the skeleton of player table
player_table = dict()

# define column names 
column_names = ['name', 
                'season',
                'team',
                'games',
                'gamesStarted',
                'ppg',
                'rpg',
                'apg',
                'mpg',
                'efficiency',
                'fg_perc',
                'p3_perc',
                'ft_perc',
                'offRebsPg',
                'defRebsPg',
                'stealsPg',
                'bpg',
                'turnoversPg',
                'pf'
               ]

# create empty lists for each columns to fill later from json
for item in column_names:
    player_table[item] = []
    
# define which years we will get 
years = [2017, 2018]

for y in years:
    
    # get url of the data for getting page numbers
    url = ("https://uk.global.nba.com/stats2/league/playerstats.json?conference=All&country=All&individual=All&locale=en&pageIndex=0&position=All&qualified=false&season={}&seasonType=2&split=All+Team&statType=points&team=All&total=perGame".format(y))

    # get the player data from json file
    r = requests.get(url)

    r = r.json()
    
    pages = range(0, int(float(r['payload']['paging']['totalPages']))+1)
    
    # get all data from each page
    for p in pages:

        # get url of the data for players
        url_player = ("https://uk.global.nba.com/stats2/league/playerstats.json?conference=All&country=All&individual=All&locale=en&pageIndex={}&position=All&qualified=false&season={}&seasonType=2&split=All+Team&statType=points&team=All&total=perGame".format(p,y))

        # get the player data from json file
        r = requests.get(url_player)

        r = r.json()

        # fill the data into the table 
        for i in r['payload']['players']:
            player_table['name'].append(i['playerProfile']['displayNameEn'])
            player_table['team'].append(i['teamProfile']['nameEn'])
            player_table['games'].append(i['statAverage']['games'])
            player_table['gamesStarted'].append(i['statAverage']['gamesStarted'])
            player_table['ppg'].append(i['statAverage']['pointsPg'])
            player_table['rpg'].append(i['statAverage']['rebsPg'])
            player_table['apg'].append(i['statAverage']['assistsPg'])
            player_table['mpg'].append(i['statAverage']['minsPg'])
            player_table['efficiency'].append(i['statAverage']['efficiency'])
            player_table['fg_perc'].append(i['statAverage']['fgpct'])
            player_table['p3_perc'].append(i['statAverage']['tppct'])
            player_table['ft_perc'].append(i['statAverage']['ftpct'])
            player_table['offRebsPg'].append(i['statAverage']['offRebsPg'])
            player_table['defRebsPg'].append(i['statAverage']['defRebsPg'])
            player_table['stealsPg'].append(i['statAverage']['stealsPg'])
            player_table['bpg'].append(i['statAverage']['blocksPg'])
            player_table['turnoversPg'].append(i['statAverage']['turnoversPg'])
            player_table['pf'].append(i['statAverage']['foulsPg'])
            
            # add season
            if y == 2017:
                player_table['season'].append('2017-2018')
            else: player_table['season'].append('2018-2019')

player_table = pd.DataFrame.from_dict(player_table)

print(player_table)

# Team Table

# create an empty dictionary for the skeleton of player table
team_table = dict()

# define column names 
column_names = ['team', 
                'season',
                'games',
                'fg_perc',
                'p3_perc',
                'ft_perc',
                'offRebsPg',
                'defRebsPg',
                'ppg',
                'rpg',
                'apg',
                'stealsPg',
                'bpg',
                'turnoversPg',
                'pf'
               ]

# create empty lists for each columns to fill later from json
for item in column_names:
    team_table[item] = []
    
# define which years we will get 
years = [2017, 2018]

for y in years:
    
    # no paging here

    # get url of the data for players
    url_team = ("https://uk.global.nba.com/stats2/league/teamstats.json?conference=All&country=All&individual=All&locale=en&pageIndex=0&position=All&qualified=false&season={}&seasonType=2&split=All+Team&statType=points&team=All&total=perGame".format(y))

    # get the player data from json file
    r = requests.get(url_team)

    r = r.json()

    # fill the data into the table 
    for i in r['payload']['teams']:
        team_table['team'].append(i['profile']['nameEn'])
        team_table['games'].append(i['statAverage']['games'])
        team_table['fg_perc'].append(i['statAverage']['fgpct'])
        team_table['p3_perc'].append(i['statAverage']['tppct'])
        team_table['ft_perc'].append(i['statAverage']['ftpct'])
        team_table['offRebsPg'].append(i['statAverage']['offRebsPg'])
        team_table['defRebsPg'].append(i['statAverage']['defRebsPg'])
        team_table['ppg'].append(i['statAverage']['pointsPg'])
        team_table['rpg'].append(i['statAverage']['rebsPg'])
        team_table['apg'].append(i['statAverage']['assistsPg'])
        team_table['stealsPg'].append(i['statAverage']['stealsPg'])
        team_table['bpg'].append(i['statAverage']['blocksPg'])
        team_table['turnoversPg'].append(i['statAverage']['turnoversPg'])
        team_table['pf'].append(i['statAverage']['foulsPg'])
        
        # add season
        if y == 2017:
            team_table['season'].append('2017-2018')
        else: team_table['season'].append('2018-2019')

team_table = pd.DataFrame.from_dict(team_table)

print(team_table)

print(  'Keys: ',
        'POSTGRES_DB: ', os.environ.get("POSTGRES_DB"), " ",
        'POSTGRES_USER: ', os.environ.get("POSTGRES_USER")," ",
        'POSTGRES_PASSWORD: ', os.environ.get("POSTGRES_PASSWORD"),
        'POSTGRES_HOST_ADD: ', os.environ.get("POSTGRES_HOST_ADD"), " ",
        'POSTGRES_PORT: ', os.environ.get("POSTGRES_PORT")
        )

# create a new connection to database

#establishing the connection
conn = psycopg2.connect(
     user=os.environ.get("POSTGRES_USER"), 
     password=os.environ.get("POSTGRES_PASSWORD"), 
     host=os.environ.get("POSTGRES_HOST_ADD"), 
     port= os.environ.get("POSTGRES_PORT")
    )
 
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()


# Create a database

# drop if exists before
cursor.execute("DROP DATABASE IF EXISTS nba_postgres_database;")
# create a database
cursor.execute("""CREATE DATABASE nba_postgres_database
                    WITH
                    OWNER = postgres
                    ENCODING = 'UTF8'
                    LC_COLLATE = 'English_United Kingdom.utf8'
                    LC_CTYPE = 'English_United Kingdom.utf8'
                    TABLESPACE = pg_default
                    CONNECTION LIMIT = -1 
                    IS_TEMPLATE = False;""") 
# CONNECTION LIMIT = -1  means there is no limit

#Closing the connection
conn.close()


# create a new connection to database

#establishing the connection
conn = psycopg2.connect(
    database=os.environ.get("POSTGRES_DB"), 
    user=os.environ.get("POSTGRES_USER"), 
    password=os.environ.get("POSTGRES_PASSWORD"), 
    host=os.environ.get("POSTGRES_HOST_ADD"), 
    port= os.environ.get("POSTGRES_PORT")
    )

conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()


# create tables in the database

# Player

# drop if exists before
cursor.execute("DROP TABLE IF EXISTS public.player;")

# create the player table
cursor.execute("""CREATE TABLE IF NOT EXISTS public.player
                    (
                        pid SERIAL,
                        name character varying(100) COLLATE pg_catalog."default" NOT NULL,
                        CONSTRAINT player_pkey PRIMARY KEY (pid)
                    )

                    TABLESPACE pg_default;""") 

# You can just write 'serial' instead of 'nextval('player_pid_seq'::regclass)'


# assign the owner 
cursor.execute("""ALTER TABLE IF EXISTS public.player
                    OWNER to postgres;""") 


# Team

# drop if exists before
cursor.execute("DROP TABLE IF EXISTS public.team;")

# create the team table
cursor.execute("""CREATE TABLE IF NOT EXISTS public.team
                    (
                        tid SERIAL,
                        team character varying(100) COLLATE pg_catalog."default" NOT NULL,
                        CONSTRAINT team_pkey PRIMARY KEY (tid)
                    )

                    TABLESPACE pg_default;""") 

# assign the owner 
cursor.execute("""ALTER TABLE IF EXISTS public.team
                    OWNER to postgres;""") 


# Season

# drop if exists before
cursor.execute("DROP TABLE IF EXISTS public.season;")

# create the season table
cursor.execute("""CREATE TABLE IF NOT EXISTS public.season
                    (
                        sid SERIAL,
                        season character varying(100) COLLATE pg_catalog."default" NOT NULL,
                        CONSTRAINT season_pkey PRIMARY KEY (sid)
                    )

                    TABLESPACE pg_default;""") 

# assign the owner 
cursor.execute("""ALTER TABLE IF EXISTS public.season
                    OWNER to postgres;""") 


# Player Stats

# drop if exists before
cursor.execute("DROP TABLE IF EXISTS public.player_stats;")

# create the player stats table
cursor.execute("""CREATE TABLE IF NOT EXISTS public.player_stats
                    (
                        stid SERIAL,
                        pid integer NOT NULL,
                        sid integer NOT NULL,
                        tid integer NOT NULL,
                        games integer,
                        gamesstarted integer,
                        ppg double precision,
                        rpg double precision,
                        apg double precision,
                        mpg double precision,
                        efficiency double precision,
                        fg_perc double precision,
                        p3_perc double precision,
                        ft_perc double precision,
                        offrebspg double precision,
                        defrebspg double precision,
                        stealspg double precision,
                        bpg double precision,
                        turnoverspg double precision,
                        pf double precision,
                        CONSTRAINT stid_pkey PRIMARY KEY (stid),
                        CONSTRAINT fk_pid_stid FOREIGN KEY (pid)
                            REFERENCES public.player (pid) MATCH SIMPLE
                            ON UPDATE NO ACTION
                            ON DELETE NO ACTION,
                        CONSTRAINT fk_sid_stid FOREIGN KEY (sid)
                            REFERENCES public.season (sid) MATCH SIMPLE
                            ON UPDATE NO ACTION
                            ON DELETE NO ACTION,
                        CONSTRAINT fk_tid_stid FOREIGN KEY (tid)
                            REFERENCES public.team (tid) MATCH SIMPLE
                            ON UPDATE NO ACTION
                            ON DELETE NO ACTION
                    )

                    TABLESPACE pg_default;""") 

# assign the owner 
cursor.execute("""ALTER TABLE IF EXISTS public.player_stats
                    OWNER to postgres;""") 


# Team Stats

# drop if exists before
cursor.execute("DROP TABLE IF EXISTS public.team_stats;")

# create the team stats table
cursor.execute("""CREATE TABLE IF NOT EXISTS public.team_stats
                    (
                        tstid SERIAL,
                        tid integer NOT NULL,
                        sid integer NOT NULL,
                        games integer,
                        fg_perc double precision,
                        p3_perc double precision,
                        ft_perc double precision,
                        offrebspg double precision,
                        defrebspg double precision,
                        ppg double precision,
                        rpg double precision,
                        apg double precision,
                        stealspg double precision,
                        bpg double precision,
                        turnoverspg double precision,
                        pf double precision,
                        CONSTRAINT tstid_pkey PRIMARY KEY (tstid),
                        CONSTRAINT fk_sid_tstid FOREIGN KEY (sid)
                            REFERENCES public.season (sid) MATCH SIMPLE
                            ON UPDATE NO ACTION
                            ON DELETE NO ACTION,
                        CONSTRAINT fk_tid_tstid FOREIGN KEY (tid)
                            REFERENCES public.team (tid) MATCH SIMPLE
                            ON UPDATE NO ACTION
                            ON DELETE NO ACTION
                    )

                    TABLESPACE pg_default;""") 

# assign the owner 
cursor.execute("""ALTER TABLE IF EXISTS public.team_stats
                    OWNER to postgres;""") 


# load data into main tables
for i in player_table.name:
    cursor.execute("INSERT INTO public.player (name) values ('" + i.replace("'", " ") + "');")
    

for i in team_table.team:
    cursor.execute("INSERT INTO  public.team (team) values ('" + i.replace("'", " ") + "');")
    
cursor.execute("INSERT INTO  public.season (season) values ('2017-2018');")
cursor.execute("INSERT INTO  public.season (season) values ('2018-2019');")


# load data into player stats

for index, player in player_table.iterrows():
    values = f"{index + 1}, {1 if player['season'] == '2017-2018' else 2}, {team_table.index[team_table['team'] == player['team']][0]+1}, {player['games']}, {player['gamesStarted']}, {player['ppg']},{player['rpg']},{player['apg']}, {player['mpg']}, {player['efficiency']},{player['fg_perc']},{player['p3_perc']},{player['ft_perc']},{player['offRebsPg']},{player['defRebsPg']}, {player['stealsPg']},{player['bpg']},{player['turnoversPg']},{player['pf']}"
    cursor.execute(f"INSERT INTO public.player_stats (pid, sid, tid, games, gamesStarted, ppg, rpg, apg, mpg, efficiency, fg_perc, p3_perc, ft_perc, offRebsPg, defRebsPg, stealsPg, bpg, turnoversPg, pf) values ({values});")


# load data into team stats

for index, team in team_table.iterrows():
    values = f"{index + 1}, {1 if team['season'] == '2017-2018' else 2}, {team['games']}, {team['fg_perc']},{team['p3_perc']},{team['ft_perc']},{team['offRebsPg']}, {team['defRebsPg']}, {team['ppg']},{team['rpg']},{team['apg']}, {team['stealsPg']},{team['bpg']},{team['turnoversPg']},{team['pf']}"
    cursor.execute(f"INSERT INTO public.team_stats (tid, sid, games, fg_perc, p3_perc, ft_perc, offRebsPg, defRebsPg, ppg, rpg, apg, stealsPg, bpg, turnoversPg, pf) values ({values});")


# An example query to get the best 3 players per team and some statistics about them

cursor.execute("""WITH team_avg AS 
                        (SELECT t.tid
                                , AVG(ps.ppg) AS team_avg
                        FROM player_stats as ps 
                            INNER JOIN team as t 
                                ON ps.tid = t.tid
                            GROUP BY 1),

                        whole_avg AS 
                        (SELECT AVG(ppg) AS whole_avg
                         FROM player_stats),

                         team_order AS 
                        (SELECT tid, pid
                                , AVG(ppg) AS player_avg
                                , ROW_NUMBER() OVER(PARTITION BY tid, sid ORDER BY AVG(ppg) DESC) as row_num
                        FROM player_stats
                            GROUP BY 1,2,sid)

                SELECT p.name, t.team
                        , AVG(ps.ppg) AS player_avg
                        , ((AVG(ps.ppg)/ta.team_avg)-1)*100 as performance_comparing_team
                        , ((AVG(ps.ppg)/wa.whole_avg)-1)*100 as performance_comparing_whole

                    FROM player_stats as ps 
                        INNER JOIN player as p 
                            ON ps.pid = p.pid
                        LEFT JOIN team_stats as ts 
                            ON ps.tid = ts.tid
                        LEFT JOIN team as t 
                            ON ts.tid = t.tid
                        LEFT JOIN team_order as ton 
                            ON ps.pid = ton.pid
                        LEFT JOIN team_avg as ta
                            ON ps.tid = ta.tid
                            ,whole_avg as wa
                    WHERE ps.sid = 2 and ton.row_num < 4
                    GROUP BY p.name,t.team,ta.team_avg, wa.whole_avg
                    ORDER BY player_avg DESC;""")
data = cursor.fetchall()
df = pd.DataFrame.from_records(data)

df.columns = ['name', 'team', 'player_avg', 'performance_comparing_team', 'performance_comparing_whole']

print(df)

# save data to local
df.to_csv('best3.csv', index=False)

#Closing the connection
conn.close()

