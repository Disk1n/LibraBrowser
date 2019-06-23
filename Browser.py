#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# execute in production with: nohup python3 Browser.py &> browser.log < /dev/null &

###########
# Imports #
###########
from subprocess import Popen, PIPE
from time import sleep
import os
import re
import sys
import sqlite3
from multiprocessing import Process
from datetime import datetime
import traceback


##############
# Flask init #
##############
from flask import Flask, request, redirect, send_from_directory
app = Flask(__name__, static_url_path='')


###############
# Definitions #
###############
ctr = 0   # counter of requests since last init
DB_PATH = './tx_cache.db'  # path to the db we should load
CLIENT_PATH = '~/libra/'   # root directory of Libra client
c2 = None  # placeholder for connection object

header = '''<html><head><title>Libra Testnet Experimental Browser</title></head>
              <body><h3>Experimental Libra testnet explorer by <a href="https://twitter.com/gal_diskin">@gal_diskin</a> 
              special thanks to Daniel Prinz for his help</h3>
              <h3>Courtesy of <a href="https://www.firstdag.com">First Group</a></h3>
              I developed this to make testing easier. I have no patiance ATM to make it pretty / faster / more stable. 
              I might continue to develop this if I see it has value to others...
              If you liked this feel free to  let me know and send me some tokens on the testnet at: 
              <a href='/account/e945eec0f64069d4f171d394aa27881fabcbd3bb6bcc893162e60ad3d6c9feec'>
              e945eec0f64069d4f171d394aa27881fabcbd3bb6bcc893162e60ad3d6c9feec</a>
'''

index_template = open('index.html.tmpl', 'r', encoding='utf-8').read()

version_template = open('version.html.tmpl', 'r', encoding='utf-8').read()

version_error_template = header + "<h1>Couldn't read version details!<h1></body></html>"

stats_template = open('stats.html.tmpl', 'r', encoding='utf-8').read()

account_template = open('account.html.tmpl', 'r', encoding='utf-8').read()

invalid_account_template = header + '<h1>Invalid Account format!<h1></body></html>'


################
# Helper funcs #
################
def start_client_instance():
    c_path = os.path.expanduser(CLIENT_PATH + "target/debug/client")
    p = Popen([c_path, "--host", "ac.testnet.libra.org", "--port", "80",
               "-s", "./scripts/cli/trusted_peers.config.toml"], cwd=os.path.expanduser(CLIENT_PATH),
              shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, bufsize=0, universal_newlines=True)
    sleep(5)
    p.stdout.flush()
    print(os.read(p.stdout.fileno(), 10000))

    print('loading account')
    print(do_cmd("a r ./test_acct", p = p))
    sys.stdout.flush()

    return p


def do_cmd(cmd, delay=0.5, bufsize=50000, decode=True, p = None):
    p.stdin.write(cmd+'\n')
    p.stdin.flush()
    sleep(delay)
    p.stdout.flush()
    if decode:
        return os.read(p.stdout.fileno(), bufsize).decode('utf-8')
    else:
        return os.read(p.stdout.fileno(), bufsize)


def add_addr_links(s):
    r0 = re.sub(r"\n", r'<br>', s)
    r = re.sub(r"\s", r'&nbsp;', r0)
    r1 = re.sub('(sender:&nbsp;([0-9a-z]{64}))', r'<a href="/account/\2">\1</a>', r)
    r2 = re.sub('(ADDRESS:&nbsp;([0-9a-z]{64}))', r'<a href="/account/\2">\1</a>', r1)
    return r2


def is_valid_account(acct):
    if (not re.match("^[A-Za-z0-9]*$", acct)) or (len(acct) != 64):
        print("invalid Account:", acct)
        return False
    return True


def update_counters():
    global ctr
    ctr += 1
    print('counter:', ctr)
    sys.stdout.flush()


def parse_raw_tx(raw):
    ver = int(next(re.finditer(r'Transaction at version (\d+):', raw)).group(1))
    expiration_num = int(next(re.finditer(r'expiration_time: (\d+)s', raw)).group(1))
    expiration_num = min(expiration_num, 2147485547)  # handle values above max unixtime
    expiration = str(datetime.fromtimestamp(expiration_num))
    sender = next(re.finditer(r'sender: ([a-z0-9]+),', raw)).group(1)
    target = next(re.finditer(r'ADDRESS: ([a-z0-9]+)', raw)).group(1)
    t_type = next(re.finditer(r'transaction: ([a-z_-]+),', raw)).group(1)
    amount = str(int(next(re.finditer(r'U64: (\d+)', raw)).group(1)) / 1000000)
    gas_price = str(int(next(re.finditer(r'gas_unit_price: (\d+),', raw)).group(1)) / 1000000)
    gas_max   = str(int(next(re.finditer(r'max_gas_amount: (\d+),', raw)).group(1)) / 1000000)
    sq_num = next(re.finditer(r'sequence_number: (\d+),', raw)).group(1)
    pubkey = next(re.finditer(r'public_key: ([a-z0-9]+),', raw)).group(1)

    return ver, expiration, sender, target, t_type, amount, gas_price, gas_max, sq_num, pubkey, expiration_num


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


def tx_db_worker():
    while True:
        try:
            print('transactions db worker starting')
            p2 = start_client_instance()
            print('client instance for tx_db started')

            # connect to DB
            c, conn = connect_to_db(DB_PATH)  # returns cursor object

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


def get_version_from_raw(s):
    return next(re.finditer(r'(\d+)\s+$', s)).group(1)


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


def get_acct_info(raw_account_status):
    try:
        account = next(re.finditer(r'Account: ([a-z0-9]+)', raw_account_status)).group(1)
        balance = str(int(next(re.finditer(r'balance: (\d+),', raw_account_status)).group(1)) / 1000000)
        sq_num = next(re.finditer(r'sequence_number: (\d+),', raw_account_status)).group(1)
        sent_events = next(re.finditer(r'sent_events_count: (\d+),', raw_account_status)).group(1)
        recv_events = next(re.finditer(r'received_events_count: (\d+),', raw_account_status)).group(1)
    except:
        print(sys.exc_info())
        traceback.print_exception(*sys.exc_info())

    return account, balance, sq_num, sent_events, recv_events


def gen_tx_table_row(tx):
    res  = '<tr><td>'
    res += '<a href="/version/' + str(tx[0]) + '">' + str(tx[0]) + '</a></td><td>'  # Version
    res += str(tx[1]) + '</td><td>'                                                 # expiration date
    res += '<a href="/account/' + str(tx[2]) + '">' + str(tx[2]) + '</a> &rarr; '   # source
    res += '<a href="/account/' + str(tx[3]) + '">' + str(tx[3]) + '</a></td><td>'  # dest
    res += '<strong>' + str(tx[5]) + ' Libra</strong></td>'                         # amount
    res += '</tr>'

    return res


def get_all_account_tx(c, acct, page):
    offset = str(page * 100)
    c.execute("SELECT * FROM transactions WHERE (src = '"+acct+"') OR (dest = '"+acct+"')" +
              "ORDER BY version DESC LIMIT " + offset + ",100")
    res = c.fetchall()

    return res


def days_hours_minutes_seconds(td):
    return td.days, td.seconds//3600, (td.seconds//60) % 60, (td.seconds % 60)


def calc_stats(c, limit = None):
    # helper
    str_to_datetime = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")

    # time
    cur_time = datetime.now()

    # time expression
    if limit:
        n = int(cur_time.timestamp()) - limit
        t_str = ' and expiration_unixtime >= ' + str(n) + ' and expiration_unixtime < 2147485547'

    else:
        t_str = ''

    # first block
    c.execute("SELECT MIN(version) FROM transactions WHERE version > 0" + t_str)
    first_version = c.fetchall()[0][0]
    first_block_time = datetime.fromtimestamp(get_tx_from_db_by_version(first_version, c)[10])

    # get max block
    last_block = get_latest_version(c)
    print('last block = ', last_block)

    # deltas
    td = cur_time - first_block_time
    dhms = days_hours_minutes_seconds(td)
    blocks_delta = last_block - first_version + 1

    # mints
    c.execute("SELECT count(DISTINCT version) FROM transactions WHERE type = 'mint_transaction'" + t_str)
    mint_count = c.fetchall()[0][0]
    c.execute("SELECT DISTINCT version FROM transactions WHERE type = 'mint_transaction'" + t_str)
    mint_sum = sum([x[0] for x in c.fetchall()])

    # p2p txs
    c.execute("SELECT count(DISTINCT version) FROM transactions WHERE type = 'peer_to_peer_transaction'" + t_str)
    p2p_count = c.fetchall()[0][0]
    c.execute("SELECT DISTINCT version FROM transactions WHERE type = 'peer_to_peer_transaction'" + t_str)
    p2p_sum = sum([x[0] for x in c.fetchall()])

    # add 1 to account for the genesis block until it is added to DB
    c.execute("SELECT count(DISTINCT version) FROM transactions " +
              "WHERE (type != 'peer_to_peer_transaction') and (type != 'mint_transaction')" + t_str)
    other_tx_count = c.fetchall()[0][0]
    if limit is None:
        other_tx_count += 1  # TODO: this is for genesis block - remove later
    c.execute("SELECT DISTINCT version FROM transactions " +
              "WHERE (type != 'peer_to_peer_transaction') and (type != 'mint_transaction')" + t_str)
    other_sum = sum([x[0] for x in c.fetchall()])

    print('p2p + mint =', mint_count + p2p_count)

    # unique accounts
    c.execute("SELECT COUNT(DISTINCT dest) FROM transactions WHERE version > 0" + t_str)
    count_dest = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT src) FROM transactions WHERE version > 0" + t_str)
    count_src = c.fetchone()[0]

    r = (blocks_delta, *dhms, blocks_delta/td.total_seconds(), 100*mint_count/blocks_delta,
         100*p2p_count/blocks_delta, 100*other_tx_count/blocks_delta, mint_sum, p2p_sum, other_sum,
         count_dest, count_src)
    return r


##########
# Routes #
##########
@app.route('/')
def index():
    update_counters()
    c2, conn = connect_to_db(DB_PATH)

    bver = str(get_latest_version(c2))

    conn.close()
    return index_template.format(bver)


@app.route('/version/<ver>')
def version(ver):
    update_counters()
    c2, conn = connect_to_db(DB_PATH)

    bver = str(get_latest_version(c2))

    try:
        ver = int(ver)
        tx = get_tx_from_db_by_version(ver, c2)
    except:
        conn.close()
        return version_error_template

    conn.close()
    return version_template.format(bver, *tx)


@app.route('/account/<acct>')
def acct_details(acct):
    print(acct)
    update_counters()
    try:
        page = int(request.args.get('page'))
    except:
        page = 0

    if not is_valid_account(acct):
        return invalid_account_template

    c2, conn = connect_to_db(DB_PATH)
    bver = str(get_latest_version(c2))

    s = do_cmd("q as " + acct, p = p, bufsize=100000, delay=1)
    acct_info = get_acct_info(s)

    try:
        tx_list = get_all_account_tx(c2, acct, page)
        tx_tbl = ''
        for tx in tx_list:
            tx_tbl += gen_tx_table_row(tx)
    except:
        print(sys.exc_info())
        traceback.print_exception(*sys.exc_info())
        print('error in building table')

    next_page = "/account/" + acct + "?page=" + str(page + 1)

    conn.close()
    return account_template.format(bver, *acct_info, tx_tbl, next_page)


@app.route('/search')
def search_redir():
    tgt = request.args.get('acct')
    if len(tgt) == 64:
        print('redir to account', tgt)
        return redirect('/account/'+tgt)
    else:
        print('redir to tx', tgt)
        return redirect('/version/'+tgt)


@app.route('/stats')
def stats():
    update_counters()
    c2, conn = connect_to_db(DB_PATH)
    try:
        # get stats
        stats_all_time = calc_stats(c2)
        stats_24_hours = calc_stats(c2, limit = 3600 * 24)[5:]
        stats_one_hour = calc_stats(c2, limit = 3600)[5:]


        ret = stats_template.format(*stats_all_time, *stats_24_hours, *stats_one_hour)
    except:
        print(sys.exc_info())
        traceback.print_exception(*sys.exc_info())
        print('error in stats')

    conn.close()

    return ret


@app.route('/assets/<path:path>')
def send_asset(path):
    return send_from_directory('assets', path)


########
# Main #
########
if __name__ == '__main__':
    tx_p = Process(target = tx_db_worker)
    tx_p.start()

    p = start_client_instance()

    sleep(10)

    app.run(port=5000, threaded=False, host='0.0.0.0', debug=False)
