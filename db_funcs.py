# All DB manipulation functions + DB worker code

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, desc, func
from sqlalchemy.pool import StaticPool
from threading import Thread
import sys
from time import sleep

import struct

from rpc_client import get_latest_version_from_ledger, get_raw_tx_lst, parse_raw_tx_lst, start_rpc_client_instance

############
# Database #
############

metadata = MetaData()
txs = Table('transactions', metadata,
    Column('version', Integer, primary_key=True),
    Column('expiration_date', String),
    Column('src', String),
    Column('dest', String),
    Column('type', String),
    Column('amount', String),
    Column('gas_price', String),
    Column('max_gas', String),
    Column('sq_num', Integer),
    Column('pub_key', String),
    Column('expiration_unixtime', Integer),
    Column('gas_used', String),
    Column('sender_sig', String),
    Column('signed_tx_hash', String),
    Column('state_root_hash', String),
    Column('event_root_hash', String),
    Column('code_hex', String),
    Column('program', String),
)
engine = create_engine(
    'sqlite://',
    connect_args={'check_same_thread': False},
    poolclass = StaticPool,
    echo=False
)
metadata.create_all(engine)

#########
# Funcs #
#########

def get_latest_version():
    cur_ver = engine.execute(select([func.max(txs.c.version)])).scalar()
    if cur_ver is None:
        logger.info("couldn't find any records; setting current version to 0")
        cur_ver = 0
    return cur_ver

def parse_db_row(row):
    return [struct.unpack('<Q', r)[0] / 1000000 if i in (5,6,7,11) else r for i, r in enumerate(row)]

def get_tx_from_db_by_version(ver):
    try:
        ver = int(ver)   # safety
    except:
        logger.warning('potential attempt to inject: {}'.format(ver))
        ver = 1
    selected = engine.execute(select([txs]).where(txs.c.version == ver))
    res = parse_db_row(selected.fetchone())
    if selected.fetchone():
        logger.warning('possible duplicates detected in db, record version: {}'.format(ver))
    return res

def get_all_account_tx(acct, page):
    return map(
        parse_db_row,
        engine.execute(
            select([txs]).where(
                (txs.c.src == acct) | (txs.c.dest == acct)
            ).order_by(desc(txs.c.version)).limit(100).offset(page*100)
        )
    )

#############
# DB Worker #
#############

class TxDBWorker(Thread):
    def __init__(self, db_path, rpc_server, mint_addr):
        Thread.__init__(self)
        self.db_path = db_path
        started = False
        logger.info('transactions db worker starting')
        while not started:
            try:
                start_rpc_client_instance(rpc_server, mint_addr)
                started = True
            except:
                sleep(10)

    def run(self):
        while True:
            try:
                # get latest version in the db
                cur_ver = get_latest_version()
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
                    engine.execute(txs.insert(), res)

                    # update counter to the latest version we inserted
                    cur_ver = res[-1]['version']
                    logger.debug('update to version: {} - success'.format(cur_ver))

                    # update latest version to next
                    cur_ver += 1

                    # sleep relative to amount of rows fetched so we don't get a 429 error
                    sleep(0.001 * num)

            except:
                logger.exception('Major error in tx_db_worker')
                sleep(2)
                logger.info('restarting tx_db_worker')
