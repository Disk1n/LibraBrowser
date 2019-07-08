# Library to automate Libra client

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
import os
import re
import sys
from datetime import datetime
from subprocess import Popen, PIPE
from time import sleep


#########
# Funcs #
#########
def start_client_instance(client_path = '', account_file = ''):
    c_path = os.path.expanduser(client_path + "target/debug/client")
    args = [c_path, "--host", "ac.testnet.libra.org", "--port", "80",
        "-s", "./scripts/cli/trusted_peers.config.toml"]
    logger.info(' '.join(args))
    p = Popen(args, cwd=os.path.expanduser(client_path),
              shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, bufsize=0, universal_newlines=True)
    sleep(5)
    p.stdout.flush()
    logger.info(os.read(p.stdout.fileno(), 10000).decode('unicode_escape'))
    logger.info('loading account {}: {}'.format(account_file, do_cmd("a r " + account_file, p = p)))
    sys.stdout.flush()

    return p


def do_cmd(cmd, delay=0.5, bufsize=50000, decode=True, p=None):
    p.stdin.write(cmd+'\n')
    p.stdin.flush()
    sleep(delay)
    p.stdout.flush()
    if decode:
        return os.read(p.stdout.fileno(), bufsize).decode('utf-8')
    else:
        return os.read(p.stdout.fileno(), bufsize)


def get_version_from_raw(s):
    return next(re.finditer(r'(\d+)\s+$', s)).group(1)


def get_acct_info(raw_account_status):
    try:
        account = next(re.finditer(r'Account: ([a-z0-9]+)', raw_account_status)).group(1)
        balance = str(int(next(re.finditer(r'balance: (\d+),', raw_account_status)).group(1)) / 1000000)
        sq_num = next(re.finditer(r'sequence_number: (\d+),', raw_account_status)).group(1)
        sent_events = next(re.finditer(r'sent_events_count: (\d+),', raw_account_status)).group(1)
        recv_events = next(re.finditer(r'received_events_count: (\d+),', raw_account_status)).group(1)
    except:
        logger.exception('Error in getting account info')

    return account, balance, sq_num, sent_events, recv_events


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
