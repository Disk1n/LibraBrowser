# All DB manipulation functions + DB worker code

###########
# Imports #
###########
import sqlite3
import sys
import traceback
from time import sleep

#from Browser import CLIENT_PATH, DB_PATH
from client import start_client_instance, do_cmd, get_version_from_raw, parse_raw_tx


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
        print('potential attempt to inject')
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
                             expiration_unixtime INTEGER)''')
    except:
        print('reusing existing db')

        # Test DB version
        c.execute("SELECT * FROM transactions where version = 1")
        if len(c.fetchone()) != 11:
            print("DB version mismatch! please run db_upgrade.py")
            sys.exit()


def tx_db_worker(client_path='~/libra/', db_path='./tx_cache.db'):
    while True:
        try:
            print('transactions db worker starting')
            p2 = start_client_instance(client_path)
            print('client instance for tx_db started')

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
                    s = do_cmd("q as 0", p = p2, delay = 0.3)
                    bver = get_version_from_raw(s)
                    bver = int(bver)
                except:
                    sleep(1)
                    continue
                if cur_ver > bver:
                    sleep(1)
                    continue

                if bver > cur_ver + 100:
                    # batch update
                    raw_tx = do_cmd("q tr " + str(cur_ver) + " 100 false", bufsize=300000, p=p2, delay=2)

                    end = raw_tx.index('\nTransaction')
                    start = end + 1
                    tx_lst = []

                    for x in range(100):
                        try:
                            end = raw_tx.index('\nTransaction', start)
                            next_str = raw_tx[start:end]
                        except:
                            break  # instead of handling the edge case and taking parsing risk just stop
                        tx_tuple = parse_raw_tx(next_str)
                        ver = tx_tuple[0]
                        tx_lst.append(tx_tuple)

                        start = end + 1

                    # do the insert
                    c.executemany("INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?,?);", tx_lst)

                    # update counter to the latest version we inserted
                    cur_ver = ver

                    print('batch update to version:', cur_ver, 'success')

                else:
                    # singular update
                    raw_tx = do_cmd("q tr " + str(cur_ver) + " 1 false", bufsize=10000, p=p2, delay=2)
                    tx_tuple = parse_raw_tx(raw_tx)
                    c.execute("INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?,?);", tx_tuple)

                # Save (commit) the changes
                conn.commit()

                # update latest version to next
                cur_ver += 1

                # nice print
                if (cur_ver - 1) % 10 == 0:
                    print('db updated to version:', cur_ver - 1)

        except:
            p2.communicate("q!")
            print('Major error in tx_db_worker, details:', sys.exc_info())
            traceback.print_exception(*sys.exc_info())
            print('restarting tx_db_worker')