# All DB manipulation functions + DB worker code

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
from sqlalchemy import create_engine, engine_from_config, Table, Column, Integer, BigInteger, LargeBinary, String, MetaData, select, desc, func
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.serializer import dumps
from threading import Thread
import sys
from time import sleep, gmtime, strftime
import json
import struct
import gzip

from rpc_client import get_latest_version_from_ledger, get_raw_tx_lst, parse_raw_tx_lst, start_rpc_client_instance

#############
# Constants #
#############

columns = (
    Column('version', Integer, primary_key=True),
    Column('expiration_date', String),
    Column('src', String),
    Column('dest', String),
    Column('type', String),
    Column('amount', LargeBinary),
    Column('gas_price', LargeBinary),
    Column('max_gas', LargeBinary),
    Column('sq_num', Integer),
    Column('pub_key', String),
    Column('expiration_unixtime', BigInteger),
    Column('gas_used', LargeBinary),
    Column('sender_sig', String),
    Column('signed_tx_hash', String),
    Column('state_root_hash', String),
    Column('event_root_hash', String),
    Column('code_hex', String),
    Column('program', String),
)
metadata = MetaData()
txs = Table('transactions', metadata, *columns)

###########
# Globals #
###########

engine = None

#########
# Funcs #
#########

unpack = lambda x: struct.unpack('<Q', x)[0] / 1000000

def get_latest_version():
    global engine
    cur_ver = engine.execute(select([func.max(txs.c.version)])).scalar()
    if cur_ver is None:
        logger.info("couldn't find any records; setting current version to 0")
        cur_ver = 0
    return cur_ver

def parse_db_row(row):
    return [unpack(r) if i in (5,6,7,11) else r for i, r in enumerate(row)]

def get_tx_from_db_by_version(ver):
    global engine
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
    global engine
    return map(
        parse_db_row,
        engine.execute(
            select([txs]).where(
                (txs.c.src == acct) | (txs.c.dest == acct)
            ).order_by(desc(txs.c.version)).limit(100).offset(page*100)
        )
    )

def get_first_version(s_limit):
    global engine
    return engine.execute(
        s_limit(
            select(
                [func.min(txs.c.version)]
            ).where(
                txs.c.version > 0
            )
        )
    ).scalar()

def get_tx_cnt_sum(whereclause, s_limit):
    global engine
    selected = engine.execute(
        s_limit(
            select(
                [txs.c.amount]
            ).where(
                whereclause
            ).distinct(txs.c.version)
        )
    ).fetchall()
    return len(selected), sum(map(lambda r: unpack(r['amount']), selected))

def get_acct_cnt(acct, s_limit):
    global engine
    return engine.execute(
        s_limit(
            select(
                [func.count(acct.distinct())]
            ).where(
                txs.c.version > 0
            )
        )
    ).scalar()


#############
# DB Worker #
#############

class TxDBWorker(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.url = "{DB_DIALECT}+{DB_DRIVER}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**config)
        logger.info('sqlalchemy.url: {}'.format(self.url))
        self.db_backup_path = config['DB_BACKUP_PATH']
        running = False
        while not running:
            try:
                start_rpc_client_instance(config['RPC_SERVER'], config['MINT_ACCOUNT'])
                running = True
            except:
                sleep(10)

    def run(self):
        global engine
        while True:
            logger.info('transactions db worker starting')
            engine = create_engine(self.url)
            metadata.create_all(engine)
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
                        if cur_ver > bver + 50: # for safety due to typical blockchain behavior
                            sleep(1)
                            continue
                        file_path = '{}_{}.gz'.format(self.db_backup_path, strftime('%Y%m%d%H%M%S'))
                        logger.info('saving database to {}'.format(file_path))
                        with gzip.open(file_path, 'wb') as f:
                            f.write(dumps(engine.execute(select([txs])).fetchall()))
                        metadata.drop_all(engine)
                        metadata.create_all(engine)
                        break

                    # batch update
                    num = min(1000, bver - cur_ver)  # at most 5000 records at once
                    tx_data = get_raw_tx_lst(cur_ver, num)

                    # read records
                    res = parse_raw_tx_lst(*tx_data)
                    if not res:
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
