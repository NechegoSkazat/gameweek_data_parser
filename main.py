import asyncio
import json
import psycopg2

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport


async def main():
    transport = AIOHTTPTransport(
        url="https://api.sorare.com/graphql",
        # Here we can add authentication data to increase the number of requests per minute
        # headers = {"Authorization": "Bearer <TheUserAccessToken>"}
    )

    async with Client(transport=transport) as session:

        query = gql(
            """
            query {
                  so5 {
                    so5Fixture {
                      gameWeek
                      displayName
                      so5Leagues {
                        so5Leaderboards {
                          displayName
                          rewardedLineupsCount
                          rarityType
                          so5LineupsCount
                        }
                      }
                    }
                  }
                }
        """
        )

        result = await session.execute(query)
        game_week = result['so5']['so5Fixture']['gameWeek']
        leagues = {}
        for card in result["so5"]["so5Fixture"]["so5Leagues"]:
            for obj in card['so5Leaderboards']:
                league = {'displayName': obj["displayName"],
                          'rarityType': obj["rarityType"],
                          'rewardedLineupsCount': int(obj['rewardedLineupsCount']),
                          'so5LineupsCount': int(obj['so5LineupsCount'])}
                leagues[obj["displayName"]] = league

        cur = conn.cursor()
        cur.execute('SELECT * FROM rewards_rewards WHERE gw = %s', (game_week,))

        if cur.fetchone() is None:
            insert_rewards_data(cur, leagues, game_week)
        else:
            update_rewards_data(cur, leagues, game_week)
        cur.close()
        conn.close()

        with open("storage/backup.json", "w",
                  encoding="utf-8") as file:
            json.dump(leagues, file)


conn = psycopg2.connect(
    database="gw_rewards",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432")


def db_connection_decorator(func):
    def wrapper(*args):
        cur = conn.cursor()
        func(*args)
        cur.close()
        conn.close()
    return wrapper


@db_connection_decorator
def transfer_leagues_data(cur, leagues_info):
    sql = "INSERT INTO rewards_leagues (name, rarity) VALUES (%s, %s)"
    for obj in leagues_info:
        data = (leagues_info[obj]['displayName'], leagues_info[obj]['rarityType'])
        cur.execute(sql, data)
        conn.commit()


@db_connection_decorator
def insert_rewards_data(cur, rewards_info, current_gw):
    sql = "INSERT INTO rewards_rewards (gw, league_id, prize_pool, entrances) VALUES (%s, %s, %s, %s)"
    for obj in rewards_info:
        league_name = str(rewards_info[obj]["displayName"])
        print(league_name)
        cur.execute("SELECT ID FROM rewards_leagues WHERE NAME=(%s)", (league_name,))
        league_id = cur.fetchone()[0]
        print(league_id)
        data = (current_gw, league_id, rewards_info[obj]['rewardedLineupsCount'], rewards_info[obj]['so5LineupsCount'])
        cur.execute(sql, data)
        conn.commit()


@db_connection_decorator
def update_rewards_data(cursor, rewards_info, current_gw):
    sql = "UPDATE rewards_rewards SET prize_pool=(%s), entrances=(%s) WHERE league_id=(%s) and gw=(%s)"
    for obj in rewards_info:
        league_name = str(rewards_info[obj]["displayName"])
        prize_pool = rewards_info[obj]['rewardedLineupsCount']
        entrances = rewards_info[obj]['so5LineupsCount']
        cursor.execute("SELECT ID FROM rewards_leagues WHERE NAME=(%s)", (league_name,))
        league_id = cursor.fetchone()[0]
        print(league_id, league_name, prize_pool, entrances)
        data = (prize_pool, entrances, league_id, current_gw)
        cursor.execute(sql, data)
        conn.commit()


asyncio.run(main())
