# All DB manipulation functions + DB worker code

###########
# Imports #
###########
import sqlite3
import sys
import traceback
from time import sleep

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
        print(sys.exc_info())
        traceback.print_exception(*sys.exc_info())
        print("couldn't find any records setting current version to 0")
        cur_ver = 0
    if not type(cur_ver) is int:
        cur_ver = 0
    return cur_ver


def get_tx_from_db_by_version(ver, c):
    try:
        ver = int(ver)   # safety
    except:
        print('potential attempt to inject:', ver)
        ver = 1

    c.execute("SELECT * FROM transactions WHERE version = " + str(ver))
    res = c.fetchall()

    if len(res) > 1:
        print('possible duplicates detected in db, record version:', ver)

    return res[0]


def get_all_account_tx(c, acct, page):
    offset = str(page * 100)
    c.execute("SELECT * FROM transactions WHERE (src = '"+acct+"') OR (dest = '"+acct+"')" +
              "ORDER BY version DESC LIMIT " + offset + ",100")
    res = c.fetchall()

    return res


def init_db(c):
    # Create table if doesn't exist
    try:
        c.execute('''CREATE TABLE transactions
                             (version INTEGER NOT NULL PRIMARY KEY, expiration_date text, src text, dest text, 
                             type text, amount real, gas_price real, max_gas real, sq_num INTEGER, pub_key text,
                             expiration_unixtime INTEGER, gas_used real, sender_sig text, signed_tx_hash text,
                             state_root_hash text, event_root_hash text, code_hex text, program text)''')
    except:
        print('reusing existing db')

        # Test DB version
        c.execute("SELECT * FROM transactions where version = 1")
        tmp = c.fetchone()
        if tmp is None:
            pass
        elif len(tmp) != 18:
            print("DB version mismatch! please delete the old db and allow the system to repopulate")
            sys.exit()


def tx_db_worker(db_path='./tx_cache.db'):
    while True:
        try:
            print('transactions db worker starting')

            # create rpc connection
            start_rpc_client_instance()

            # connect to DB
            c, conn = connect_to_db(db_path)  # returns cursor object
            init_db(c)

            # get latest version in the db
            cur_ver = get_latest_version(c)
            cur_ver += 1  # TODO: later handle genesis
            print('starting update at version', cur_ver)

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
                num = min(5000, bver - cur_ver)  # at most 5000 records at once
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

                print('update to version:', cur_ver, 'success')

                # Save (commit) the changes
                conn.commit()

                # update latest version to next
                cur_ver += 1

        except:
            print('Major error in tx_db_worker, details:', sys.exc_info())
            traceback.print_exception(*sys.exc_info())
            print('restarting tx_db_worker')