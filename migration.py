

import pyodbc
import pymongo
import collections


def connect():
    # Specifying the ODBC driver, server name, database, etc. directly
    cnxn = pyodbc.connect('DRIVER={MySQL ODBC 5.3 UNICODE Driver};SERVER=localhost;DATABASE=all_beters;uid=root')
    # Create a cursor from the connection
    return cnxn.cursor()


def connect_mongo():
    my_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = my_client["admin"]
    return db


def insert_leagues(cursor, mongo_client):
    unique_leagues = collections.defaultdict(int)
    leagues_to_insert = []

    # Fetch records
    cursor.execute("SELECT league, sport_id FROM vaja")
    leagues_res = cursor.fetchall()

    # build unique league documents

    for lg in leagues_res:
        unique_league_key = lg[0]
        unique_leagues[unique_league_key] += 1
        if unique_leagues[unique_league_key] == 1:
            leagues_to_insert.append({'Name': unique_league_key, 'Sport':lg[1]})

    # insert leagues
    mongo_client.leagues.insert_many(leagues_to_insert)


def get_leagues_map(mongo_client):
    league_map = {}  # holding ids from Mongo
    # get leagues with id and build map
    for doc in mongo_client.leagues.find():
        league_map[doc['Name']] = doc['_id']
    return league_map


def get_match_map(mongo_client):
    match_map = {}
    # get matches with ids and put them into map
    for doc in mongo_client.matches.find():
        mkey = str(doc["Liga"])+ doc["Ekipa1"]+ doc["Ekipa2"]+ str(doc["Date"])
        match_map[mkey] = doc["_id"]
    return match_map


def get_match_key_unique(league_map, match_row):
    return str(league_map[match_row[6]]) + match_row[7] + match_row[8] + str(match_row[9])


def insert_mathes(cursor, mongo_client, league_map):

    match_list = []
    unique_match_map = collections.defaultdict(int)

    # Fetch records for matches
    cursor.execute("SELECT * FROM vaja");
    records = cursor.fetchall();

    # Now we have id for leagues, prepare matches with league ids
    for row in records:
        unique_match_key = get_match_key_unique(league_map, row)
        unique_match_map[unique_match_key] += 1
        # if there is only one record of this type, then we can inserts this. (Do not duplicate)
        if unique_match_map[unique_match_key] == 1:
            match_list.append(
                {"Liga": league_map[row[6]], "Ekipa1": row[7], "Ekipa2": row[8], "Date": row[9], "Score": row[18]})

    # insert matches
    mongo_client.matches.insert_many(match_list)


def insert_bets(cursor, mongo_client, league_map, match_map):
    # holding bets
    bets_to_insert = []
    # Fetch records for matches
    cursor.execute("SELECT * FROM vaja limit 100");
    records = cursor.fetchall();

    league_map = get_leagues_map(mongo_client)

    # build bet record
    for bet in records:
        match_id = match_map[get_match_key_unique(league_map, bet)]  # get match id from mongo
        bets_to_insert.append({
            "Match": match_id,
            "BetterId": bet[1],
            "Type1Id": bet[3],
            "Type2": bet[4],
            "Type3": bet[5],
            "PickedQuota": float(bet[16]),
            "PickedOption": bet[17],
            "Status": bet[19],
        })
    print(bets_to_insert)

    # TODO link with bet percent
    # TODO link with options
    mongo_client.bets.insert_many(bets_to_insert)


def insert_options(cursor, mongo_client):
    # holding records to insert
    options_to_insert = []
    # Fetch records for matches
    cursor.execute("SELECT options1, options2, options3 FROM all_beters.vaja group by options1, options2, options3")
    records = cursor.fetchall()

    for opt in records:
        options_to_insert.append({
            "Option1": opt[0],
            "Option2": opt[1],
            "Option3": opt[2],
        })

    mongo_client.bet_options.insert_many(options_to_insert)


def get_option_map(mongo_client):
    # holding option map
    option_map_id ={}
    # get all options
    for doc in mongo_client.bet_options.find():
        optKey = doc["Option1"] + doc["Option2"]+ doc["Option3"]
        option_map_id[optKey] = doc["_id"]
    return option_map_id


def migrate(cursor,mongo_client):

    #  insert_leagues(cursor, mongo_client)
    leagues_map = get_leagues_map(mongo_client)
    #  print(leagues_map)
    #  insert_mathes(cursor, mongo_client, leagues_map)
    match_map = get_match_map(mongo_client)
    #  print(match_map)
    insert_bets(cursor, mongo_client, leagues_map, match_map)


if __name__ == '__main__':

    cursor = connect()
    mongo_client = connect_mongo()

    migrate(cursor, mongo_client)














