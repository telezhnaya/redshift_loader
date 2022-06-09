import argparse
import dotenv
import os
import psycopg2
import time
import typing

from datetime import datetime

tables_base = [
    "account_changes",
    "action_receipts__actions",
    "action_receipts__outputs",
    "action_receipts",
    "blocks",
    "chunks",
    "data_receipts",
    "execution_outcomes__receipts",
    "execution_outcomes",
    "transactions",
]
tables_accounts = [
    "accounts",
    "access_keys"
]

# Value for mainnet. Should be configured for testnet and others
# start_timestamp = 1595350551591948000
# now_timestamp = int(time.time()) * 1000 * 1000 * 1000
day_duration = 24 * 60 * 60 * 1000 * 1000 * 1000


def run_base(connection, from_timestamp, to_timestamp):
    with connection.cursor() as cursor:
        for table in tables_base:
            print(f"Updating {table}")
            query = f"INSERT INTO {table} \n" \
                    f"SELECT * FROM apg.{table}\n" \
                    f"    WHERE block_timestamp >= {from_timestamp}\n" \
                    f"        AND block_timestamp < {to_timestamp}\n"
            cursor.execute(query)
            connection.commit()


def run_accounts(connection, from_height, to_height):
    with connection.cursor() as cursor:
        for table in tables_accounts:
            print(f"Updating {table}")
            # we need to delete all the updated lines and re-insert them again
            query = f"DELETE FROM {table}\n" \
                    f"WHERE deleted_by_block_height >= {from_height}\n" \
                    f"  AND deleted_by_block_height < {to_height}"
            cursor.execute(query)
            connection.commit()
            query = f"INSERT INTO {table} \n" \
                    f"SELECT * FROM apg.{table}\n" \
                    f"WHERE (created_by_block_height >= {from_height} AND created_by_block_height < {to_height})\n" \
                    f"   OR (deleted_by_block_height >= {from_height} AND deleted_by_block_height < {to_height})"
            cursor.execute(query)
            connection.commit()


def pretty_date(timestamp):
    date_timestamp = int(timestamp) / (1000 * 1000 * 1000)
    return datetime.utcfromtimestamp(date_timestamp).date()


def check_data_is_enough(connection, to_timestamp):
    with connection.cursor() as cursor:
        cursor.execute("select block_timestamp from apg.blocks order by block_timestamp desc limit 1")
        result, = cursor.fetchone()
        return int(result) > to_timestamp


def get_height(connection, to_timestamp):
    with connection.cursor() as cursor:
        query = f"select block_height from apg.blocks\n" \
                f"where block_timestamp < {to_timestamp}\n" \
                f"order by block_timestamp desc\n" \
                f"limit 1"
        cursor.execute(query)
        result, = cursor.fetchone()
        return int(result)


def fix_data(connection, height, timestamp):
    print(f"Deleting the data starting from {pretty_date(timestamp)}, block {height}")
    with connection.cursor() as cursor:
        for table in tables_base:
            print(f"Deleting lines after last incomplete update from {table}")
            query = f"DELETE FROM {table} WHERE block_timestamp >= {timestamp}\n"
            cursor.execute(query)
            connection.commit()
        # We might partially update the lines where the accounts were deleted,
        # but it's ok, it just means we will again drop these lines later and insert them
        for table in tables_accounts:
            print(f"Deleting lines after last incomplete update from {table}")
            query = f"DELETE FROM {table} WHERE created_by_block_height >= {height}\n"
            cursor.execute(query)
            connection.commit()


def get_last_update(cursor):
    cursor.execute("select * from _last_successful_load order by block_timestamp desc limit 1")
    result = cursor.fetchall()
    return int(result[0][0]), int(result[0][1]) if result else None


def init_first_update(cursor):
    cursor.execute("select block_height, block_timestamp from apg.blocks order by block_timestamp limit 1")
    result = cursor.fetchall()
    height, timestamp = int(result[0][0]), int(result[0][1])
    # rounding timestamp to have the ability get the updates each night
    timestamp -= timestamp % day_duration
    return height, timestamp


def check_and_get_last_update(connection):
    with connection.cursor() as cursor:
        result = get_last_update(cursor)
        if not result:
            result = init_first_update(cursor)
        height, timestamp = result

        for table in tables_base:
            cursor.execute(f"select block_timestamp from {table} order by block_timestamp desc limit 1")
            result = cursor.fetchall()
            if result and int(result[0][0]) > timestamp:
                print("WARN: last update was partial. Dropping the last piece of data")
                fix_data(connection, height, timestamp)
        return height, timestamp


def save_last_update(connection, to_height, to_timestamp):
    with connection.cursor() as cursor:
        query = f"INSERT INTO _last_successful_load values({to_height}, {to_timestamp})"
        cursor.execute(query)
        connection.commit()


# todo retries
# todo print timings and number of lines inserted
if __name__ == "__main__":
    dotenv.load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    with psycopg2.connect(DATABASE_URL) as connection:
        from_height, from_timestamp = check_and_get_last_update(connection)
        while True:
            print(f"Loading the data for {pretty_date(from_timestamp)}, block {from_height}")
            to_timestamp = from_timestamp + day_duration
            if not check_data_is_enough(connection, to_timestamp):
                print(f"Data is not enough to load the info till {pretty_date(to_timestamp)}")
                break
            to_height = get_height(connection, to_timestamp)
            run_base(connection, from_timestamp, to_timestamp)
            run_accounts(connection, from_height, to_height)
            save_last_update(connection, to_height, to_timestamp)
            print(f"Updated the data from {pretty_date(from_timestamp)} "
                  f"to {pretty_date(to_timestamp)} (blocks {from_height} - {to_height})")
            from_timestamp = to_timestamp
            from_height = to_height
