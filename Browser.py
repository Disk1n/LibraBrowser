#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# execute in production with: nohup python3 Browser.py &> browser.log < /dev/null &

################
# Logging init #
################
import json
from logging.config import dictConfig

with open('logging.json', 'r') as f:
    dictConfig( json.load(f) )

###########
# Imports #
###########
import re
import sys
import os
import requests

from time import sleep

from rpc_client import get_acct_raw, get_acct_info, start_rpc_client_instance
from db_funcs import get_latest_version, get_tx_from_db_by_version, get_all_account_tx, TxDBWorker
from stats import calc_stats


##############
# Flask init #
##############
from flask import Flask, request, redirect, send_from_directory
from flask_caching import Cache

cache = Cache(config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 60})  # TODO: simple cache is not thread safe
app = Flask(__name__, static_url_path='')
cache.init_app(app)


###############
# Definitions #
###############
ctr = 0   # counter of requests since last init

with open('templates/index.tmpl.html', 'r', encoding='utf-8') as f:
    index_template = f.read()

with open('templates/version.tmpl.html', 'r', encoding='utf-8') as f:
    version_template = f.read()

with open('templates/forbidden.tmpl.html', 'r', encoding='utf-8') as f:
    forbidden_template = f.read()

with open('templates/stats.tmpl.html', 'r', encoding='utf-8') as f:
    stats_template = f.read()

with open('templates/account.tmpl.html', 'r', encoding='utf-8') as f:
    account_template = f.read()

with open('templates/faucet.tmpl.html', 'r', encoding='utf-8') as f:
    faucet_template = f.read()

faucet_alert_template = '<div class="text-center"><div class="alert alert-danger" role="alert"><p>{0}</p></div></div>'


################
# Helper funcs #
################
def update_counters():
    global ctr
    ctr += 1
    app.logger.info('counter: {}'.format(ctr))
    sys.stdout.flush()


def is_valid_account(acct):
    if (not re.match("^[A-Za-z0-9]*$", acct)) or (len(acct) != 64):
        app.logger.info("invalid Account: {}".format(acct))
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


def gen_error_page(ver = None):
    try:
        error = forbidden_template.format(ver)
    except:
        error = forbidden_template.format('???')
    return error


##########
# Routes #
##########
@app.route('/')
def index():
    update_counters()
    bver = str(get_latest_version())
    return index_template.format(bver)


@app.route('/version/<ver>')
@cache.cached(timeout=3600)  # versions don't change so we can cache long-term
def version(ver):
    update_counters()

    bver = str(get_latest_version())

    try:
        ver = int(ver)
        tx = get_tx_from_db_by_version(ver)
    except:
        return gen_error_page(bver), 404

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

    return version_template.format(bver, *tx, add_br_every64(tx[12]), extra, not_raw, tx[-2].replace('<', '&lt;'))


@app.route('/account/<acct>')
def acct_details(acct):
    app.logger.info('Account: {}'.format(acct))
    update_counters()
    bver = str(get_latest_version())

    try:
        page = int(request.args.get('page'))
    except:
        page = 0

    if not is_valid_account(acct):
        return gen_error_page(bver), 404


    acct_state_raw = get_acct_raw(acct)
    acct_info = get_acct_info(acct_state_raw)
    app.logger.info('acct_info: {}'.format(acct_info))

    try:
        tx_list = get_all_account_tx(acct, page)
        tx_tbl = ''
        for tx in tx_list:
            tx_tbl += gen_tx_table_row(tx)
    except:
        app.logger.exception('error in building table')
        return gen_error_page(bver), 404

    next_page = "/account/" + acct + "?page=" + str(page + 1)

    return account_template.format(bver, *acct_info, tx_tbl, next_page)


@app.route('/search')
def search_redir():
    tgt = request.args.get('acct')
    if len(tgt) == 64:
        app.logger.info('redir to account: {}'.format(tgt))
        return redirect('/account/'+tgt)
    else:
        app.logger.info('redir to tx: {}'.format(tgt))
        return redirect('/version/'+tgt)


@app.route('/stats')
@cache.cached(timeout=60)  # no point updating states more than once per minute
def stats():
    update_counters()
    try:
        # get stats
        stats_all_time = calc_stats()
        stats_24_hours = calc_stats(limit = 3600 * 24)[5:]
        stats_one_hour = calc_stats(limit = 3600)[5:]

        ret = stats_template.format(*stats_all_time, *stats_24_hours, *stats_one_hour)
    except:
        app.logger.exception('error in stats')
        try:
            bver = stats_all_time[0]
        except:
            bver = None
        return gen_error_page(bver), 404

    return ret


@app.route('/faucet', methods=['GET', 'POST'])
def faucet():
    update_counters()

    bver = str(get_latest_version())

    message = ''
    if request.method == 'POST':
        try:
            acct = request.form.get('acct')
            app.logger.info('acct: {}'.format(acct))
            amount = float(request.form.get('amount'))
            app.logger.info('amount: {}'.format(amount))
            if amount < 0:
                message = 'Amount must be >= 0'
            elif not is_valid_account(acct):
                message = 'Invalid account format'
            else:
                response = requests.get(
                    config['FAUCET_HOST'],
                    params={
                        'address': acct,
                        'amount': format(amount * 1e6, '.0f')
                    }
                )
                if response.status_code == 200:
                    message = 'Sent {0} <small>Libra</small> to <a href="/account/{1}">{1}</a>'.format(amount, acct)
        except:
            message = 'Invalid request logged!'
            app.logger.exception(message)

        if message:
            message = faucet_alert_template.format(message)

    return faucet_template.format(bver, message)


@app.route('/assets/<path:path>')
@cache.cached(timeout=3600)  # assets don't really change so can be cached for one hour
def send_asset(path):
    return send_from_directory('assets', path)


########
# Main #
########
if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)

    try:
        config = config[os.getenv("BROWSER")]
    except:
        config = config["PRODUCTION"]

    app.logger.info("system configuration: {}".format(json.dumps(config, indent=4)))

    TxDBWorker(config).start()

    start_rpc_client_instance(config['RPC_SERVER'], config['MINT_ACCOUNT'])

    sleep(1)

    app.run(port=config['FLASK_PORT'], threaded=config['FLASK_THREADED'],
            host=config['FLASK_HOST'], debug=config['FLASK_DEBUG'])
