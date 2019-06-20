#!/usr/bin/python3
# execute with: nohup python3 Browser.py &> browser.log < /dev/null &

###########
# Imports #
###########
from subprocess import Popen, PIPE
from time import sleep
import os
import re
import sys


##############
# Flask init #
##############
from flask import Flask, request, redirect, send_from_directory
app = Flask(__name__, static_url_path='')


###############
# Definitions #
###############
ctr = 0   # counter of requests since last init

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

index_template = open('index.html.tmpl', 'r').read()

version_template = header + '''<h1>Libra Testnet Version: {0}</h1> (this is somewhat equivalent to block height)
              <h2> <a href=/version/{2}>Previous Version</a> <a href=/version/{3}>Next Version</a> </h2>
              <h2>TX details at this version:</h2>{1}<br>
              </body></html>
'''

version_error_template = header + "<h1>Couldn't read version details!<h1></body></html>"

account_template = header + '''<h2><b>Details about account: {0} </b></h2>
                               <h2> {1} </h2>
                               <h2> {2} </h2>
                               <h2> Last tx details:</h2> {3} 
                               </body></html>
'''

invalid_account_template = header + '<h1>Invalid Account format!<h1></body></html>'


################
# Helper funcs #
################
def do_cmd(cmd, delay=0.5, bufsize=5000, decode=True):
    global p
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

##########
# Routes #
##########
@app.route('/')
def index():
    update_counters()

    s = do_cmd("q as 0", delay=1)
    bver = next(re.finditer(r'(\d+)\s+$', s)).group(1)
    print(bver)
    sys.stdout.flush()

    return index_template.format(bver)


@app.route('/version/<ver>')
def version(ver):
    update_counters()

    ver = int(ver)

    s = do_cmd("q txn_range " + str(ver) + " 1 true", delay=1, bufsize=10000)
    try:
        endtrash = next(re.finditer(r'}\s+.+$', s)).group(0)
        starttrash = next(re.finditer(r'[^:]+:', s)).group(0)
        tx = add_addr_links(s[len(starttrash):-len(endtrash)])
    except:
        return version_error_template

    return version_template.format(str(ver), tx, str(ver - 1), str(ver + 1))


@app.route('/account/<acct>')
def acct_details(acct):
    print(acct)
    update_counters()

    if not is_valid_account(acct):
        return invalid_account_template

    balance = do_cmd("q b "+acct)

    s = do_cmd("q s "+acct, delay=1)
    print('sq num raw=', s)
    sys.stdout.flush()
    sq_num = s[len('>> Getting current sequence number '):]

    last_tx_sq = int(sq_num[len('Sequence number is: '):]) - 1
    last_tx_sq = max(0, last_tx_sq)

    s = do_cmd("q ts "+acct+" "+str(last_tx_sq)+" true", delay=1, bufsize=10000)
    tx_details = s[len('>> Getting committed transaction by account and sequence number '):]

    return account_template.format(acct, balance, sq_num, add_addr_links(tx_details))


@app.route('/account')
def acct_redir():
    tgt = request.args.get('acct')
    print('redir to', tgt)
    return redirect('/account/'+tgt)


@app.route('/assets/<path:path>')
def send_asset(path):
    return send_from_directory('assets', path)


########
# Main #
########
if __name__ == '__main__':
    p = Popen(["target/debug/client", "--host", "ac.testnet.libra.org", "--port", "80",
               "-s", "./scripts/cli/trusted_peers.config.toml"],
              shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, bufsize=0, universal_newlines=True)

    sleep(5)
    p.stdout.flush()
    print(os.read(p.stdout.fileno(), 10000))
    sys.stdout.flush()

    do_cmd("a r ./test_acct")

    app.run(port=5000, threaded=False, host='0.0.0.0', debug=True)
