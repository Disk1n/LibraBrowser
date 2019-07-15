# All functions for stat generation

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
from datetime import datetime, timedelta
from db_funcs import get_tx_from_db_by_version, get_latest_version, engine, txs
from sqlalchemy import select, desc, func
import struct

#########
# Funcs #
#########

unpack = lambda x: struct.unpack('<Q', x)[0] / 1000000

def days_hours_minutes_seconds(td):
    return td.days, td.seconds//3600, (td.seconds//60) % 60, (td.seconds % 60)


def calc_stats(limit = None):
    # time
    cur_time = datetime.now()
    int_ts = int(cur_time.timestamp())
    s_limit = lambda s: (s
        .where(txs.c.expiration_unixtime >= int_ts - limit + 100)
        .where(txs.c.expiration_unixtime < int_ts + 600)
    ) if limit else s

    # first block
    first_version = engine.execute(
        s_limit(
            select(
                [func.min(txs.c.version)]
            ).where(
                txs.c.version > 0
            )
        )
    ).scalar()
    if not first_version:
        first_version = 1
    logger.info('first ver = {}'.format(first_version))

    # get max block
    last_block = get_latest_version()
    logger.info('last block = {}'.format(last_block))

    # deltas
    first_block_time = datetime.fromtimestamp(get_tx_from_db_by_version(first_version)[10])
    td = timedelta(0, limit) if limit else (cur_time - first_block_time)
    dhms = days_hours_minutes_seconds(td)
    blocks_delta = last_block - first_version + 1
    logger.info('deltas: {} {}'.format(td, blocks_delta))

    # mint p2p other
    def get_tx_cnt_sum(whereclause):
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
    mint_count, mint_sum = get_tx_cnt_sum(txs.c.type == 'mint_transaction')
    logger.info('mint {} {}'.format(mint_count, mint_sum))
    p2p_count, p2p_sum = get_tx_cnt_sum(txs.c.type == 'peer_to_peer_transaction')
    logger.info('p2p {} {}'.format(p2p_count, p2p_sum))
    other_count, other_sum = get_tx_cnt_sum((txs.c.type != 'mint_transaction') & (txs.c.type != 'peer_to_peer_transaction'))
    # add 1 to account for the genesis block until it is added to DB
    other_count += 1
    logger.info('others {} {}'.format(other_count, other_sum))

    # unique accounts
    def get_acct_cnt(acct):
        return engine.execute(
            s_limit(
                select(
                    [acct]
                ).where(
                    txs.c.version > 0
                ).distinct()
            ).count()
        ).fetchone()[0]
    count_dest = get_acct_cnt(txs.c.dest)
    count_src = get_acct_cnt(txs.c.src)

    return (blocks_delta, *dhms, blocks_delta/td.total_seconds(), 100*mint_count/blocks_delta,
        100*p2p_count/blocks_delta, 100*other_count/blocks_delta, mint_sum, p2p_sum, other_sum,
        count_dest, count_src)
