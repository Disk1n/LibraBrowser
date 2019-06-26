#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# execute in production with: nohup python3 Browser.py &> browser.log < /dev/null &

###########
# Imports #
###########
from time import sleep
import re
import sys
from multiprocessing import Process
import traceback

from rpc_client import get_acct_raw, get_acct_info, start_rpc_client_instance
from client import start_client_instance, do_cmd
from db_funcs import connect_to_db, get_latest_version, get_tx_from_db_by_version, get_all_account_tx, tx_db_worker
from stats import calc_stats


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

faucet_template = open('faucet.html.tmpl', 'r', encoding='utf-8').read()

faucet_alert_template = '<div class="text-center"><div class="alert alert-danger" role="alert"><p>{0}</p></div></div>'

invalid_account_template = header + '<h1>Invalid Account format!<h1></body></html>'


################
# Helper funcs #
################
def update_counters():
    global ctr
    ctr += 1
    print('counter:', ctr)
    sys.stdout.flush()


def is_valid_account(acct):
    if (not re.match("^[A-Za-z0-9]*$", acct)) or (len(acct) != 64):
        print("invalid Account:", acct)
        return False
    return True


def gen_tx_table_row(tx):
    res  = '<tr><td>'
    res += '<a href="/version/' + str(tx[0]) + '">' + str(tx[0]) + '</a></td><td>'  # Version
    res += str(tx[1]) + '</td><td>'                                                 # expiration date
    res += ('&#x1f91d;' if tx[4] == 'peer_to_peer_transaction' else '&#x1f6e0;') + '</td><td>'   # type
    res += '<p class="text-monospace">'
    res += '<a href="/account/' + str(tx[2]) + '">' + str(tx[2]) + '</a> &rarr; '          # source
    res += '<a href="/account/' + str(tx[3]) + '">' + str(tx[3]) + '</a></p></td><td>'  # dest
    res += '<strong>' + str(tx[5]) + ' Libra</strong></td>'                         # amount
    res += '</tr>'

    return res


def add_br_every64(s):
    x = len(s)
    i = 0
    res = ''
    while i+64 < x:
        res += s[i:i+64] + '<br>'
        i += 64
    res += s[i:]

    return res


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

    # for toggle raw view
    if request.args.get('raw') == '1':
        extra = """<tr>
                    <td><strong>Program Raw</strong></td>
                    <td><pre>{0}</pre></td>
                   </tr>""".format(tx[-1])
        not_raw = '0'
    else:
        extra = ''
        not_raw = '1'

    conn.close()
    return version_template.format(bver, *tx, add_br_every64(tx[12]), extra, not_raw, tx[-2].replace('<', '&lt;'))


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

    acct_state_raw = get_acct_raw(acct)
    acct_info = get_acct_info(acct_state_raw)
    print('acct_info', acct_info)

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


@app.route('/faucet', methods=['GET', 'POST'])
def faucet():
    update_counters()

    c2, conn = connect_to_db(DB_PATH)
    bver = str(get_latest_version(c2))

    message = ''
    if request.method == 'POST':
        try:
            acct = request.form.get('acct')
            print(acct)
            amount = request.form.get('amount')
            print(amount)
            if float(amount) < 0:
                message = 'Amount must be >= 0'
            elif not is_valid_account(acct):
                message = 'Invalid account format'
            else:
                do_cmd('a m ' + acct + ' ' + str(float(amount)), p = p)
                acct_link = '<a href="/account/{0}">{0}</a>'.format(acct)
                message = 'Sent ' + amount + ' <small>Libra</small> to ' + acct_link
        except:
            traceback.print_exception(*sys.exc_info())
            message = 'Invalid request logged!'

        if message:
            message = faucet_alert_template.format(message)

    return faucet_template.format(bver, message)


@app.route('/assets/<path:path>')
def send_asset(path):
    return send_from_directory('assets', path)


########
# Main #
########
if __name__ == '__main__':
    tx_p = Process(target=tx_db_worker, args=(DB_PATH, ))
    tx_p.start()

    start_rpc_client_instance()

    p = start_client_instance(CLIENT_PATH)

    sleep(1)

    app.run(port=5000, threaded=False, host='0.0.0.0', debug=False)
