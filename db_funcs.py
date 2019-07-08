# All DB manipulation functions + DB worker code

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
import sqlite3
import sys
from time import sleep

import struct

# from client import start_client_instance, do_cmd, parse_raw_tx
from rpc_client import get_latest_version_from_ledger, get_raw_tx_lst, parse_raw_tx_lst, start_rpc_client_instance


#########
# Funcs #
#########
def connect_to_db(path):
    conn = sqlite3.connect(path)
    return conn.cursor(), conn


def get_latest_version(c):
    try:
        c.execute("SELECT MAX(version) FROM transactions")
        cur_ver = int(c.fetchall()[0][0])
    except:
        logger.exception("couldn't find any records; setting current version to 0")
        cur_ver = 0
    if not type(cur_ver) is int:
        cur_ver = 0
    return cur_ver


def parse_db_row(row):
    r = list(row)
    r[5] = struct.unpack('<Q', r[5])[0] / 1000000
    r[6] = struct.unpack('<Q', r[6])[0] / 1000000
    r[7] = struct.unpack('<Q', r[7])[0] / 1000000
    r[11] = struct.unpack('<Q', r[11])[0] / 1000000

    return r


def get_tx_from_db_by_version(ver, c):
    try:
        ver = int(ver)   # safety
    except:
        logger.info('potential attempt to inject: {}'.format(ver))
        ver = 1

    c.execute("SELECT * FROM transactions WHERE version = " + str(ver))
    res = c.fetchall()
    res = [parse_db_row(row) for row in res]

    if len(res) > 1:
        logger.info('possible duplicates detected in db, record version: {}'.format(ver))

    return res[0]


def get_all_account_tx(c, acct, page):
    offset = str(page * 100)
    c.execute("SELECT * FROM transactions WHERE (src = '"+acct+"') OR (dest = '"+acct+"')" +
              "ORDER BY version DESC LIMIT " + offset + ",100")
    res = c.fetchall()
    res = [parse_db_row(row) for row in res]

    return res


def init_db(c):
    # Create table if doesn't exist
    try:
        c.execute('''CREATE TABLE transactions
                             (version INTEGER NOT NULL PRIMARY KEY, expiration_date text, src text, dest text, 
                             type text, amount text, gas_price text, max_gas text, sq_num INTEGER, pub_key text,
                             expiration_unixtime INTEGER, gas_used text, sender_sig text, signed_tx_hash text,
                             state_root_hash text, event_root_hash text, code_hex text, program text)''')
    except:
        logger.info('reusing existing db')

        # Test DB version
        c.execute("SELECT * FROM transactions where version = 1")
        tmp = c.fetchone()
        if tmp is None:
            pass
        elif len(tmp) != 18:
            logger.critical("DB version mismatch! please delete the old db and allow the system to repopulate")
            sys.exit()


def tx_db_worker(db_path, rpc_server, mint_addr):
    while True:
        try:
            logger.info('transactions db worker starting')

            # create rpc connection
            try:
                start_rpc_client_instance(rpc_server, mint_addr)
            except:
                sleep(10)
                start_rpc_client_instance(rpc_server, mint_addr)

            # connect to DB
            c, conn = connect_to_db(db_path)  # returns cursor object
            init_db(c)

            # get latest version in the db
            cur_ver = get_latest_version(c)
            cur_ver += 1  # TODO: later handle genesis
            logger.info('starting update at version {}'.format(cur_ver))

            # start the main loop
            while True:
                try:
                    bver = get_latest_version_from_ledger()
                except:
                    sleep(1)
                    continue
                if cur_ver > bver:
                    sleep(1)
                    continue

                # batch update
                num = min(1000, bver - cur_ver)  # at most 5000 records at once
                tx_data = get_raw_tx_lst(cur_ver, num)

                # read records
                res = parse_raw_tx_lst(*tx_data)
                if len(res) == 0:
                    sleep(5)
                    continue

                # do the insertion
                db_data = [tuple(x.values()) for x in res]
                c.executemany("INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", db_data)

                # update counter to the latest version we inserted
                cur_ver = res[-1]['version']
                logger.debug('update to version: {} - success'.format(cur_ver))

                # Save (commit) the changes
                conn.commit()

                # update latest version to next
                cur_ver += 1

                # sleep relative to amount of rows fetched so we don't get a 429 error
                sleep(0.001 * num)

        except:
            logger.exception('Major error in tx_db_worker')
            sleep(2)
            logger.info('restarting tx_db_worker')
