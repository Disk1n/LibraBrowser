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
from db_funcs import get_latest_version
from sqlalchemy import func
import struct
from models import Transaction

#########
# Funcs #
#########
unpack = lambda x: struct.unpack('<Q', x)[0] / 1000000

def days_hours_minutes_seconds(td):
    return td.days, td.seconds//3600, (td.seconds//60) % 60, (td.seconds % 60)


def calc_stats(session, limit = None):
    # time
    cur_time = datetime.now()
    int_ts = int(cur_time.timestamp())
    time_filter = lambda q: (q
        .filter(Transaction.expiration_unixtime >= int_ts - limit + 100)
        .filter(Transaction.expiration_unixtime < int_ts + 600)
    ) if limit else q

    # first block
    q = session.query(func.min(Transaction.version)).filter(Transaction.version > 0)
    first_version = time_filter(q).scalar()

    if not first_version:
        first_version = 1
    logger.info('first ver = {}'.format(first_version))

    # get max block
    last_block = get_latest_version(session)
    logger.info('last block = {}'.format(last_block))

    # deltas
    first_block_time = datetime.fromtimestamp(
        session.query(Transaction.expiration_unixtime).filter_by(version=first_version).scalar()
    )
    td = timedelta(0, limit) if limit else (cur_time - first_block_time)
    dhms = days_hours_minutes_seconds(td)
    blocks_delta = last_block - first_version + 1
    logger.info('deltas: {} {}'.format(td, blocks_delta))

    # mint p2p other
    q_amount = time_filter(session.query(Transaction.amount))

    q = q_amount.filter_by(type='mint_transaction')
    get_cnt_sum = lambda q: (q.count(), sum(unpack(v.amount) for v in q))
    mint_count, mint_sum = get_cnt_sum(q)
    logger.info('mint {} {}'.format(mint_count, mint_sum))
    
    q = q_amount.filter_by(type='peer_to_peer_transaction')
    p2p_count, p2p_sum = get_cnt_sum(q)
    logger.info('p2p {} {}'.format(p2p_count, p2p_sum))
    
    q = q_amount.filter((Transaction.type != 'mint_transaction') & (Transaction.type != 'peer_to_peer_transaction'))
    other_count, other_sum = get_cnt_sum(q)
    # add 1 to account for the genesis block until it is added to DB
    if first_version == 1:
        other_count += 1
    logger.info('others {} {}'.format(other_count, other_sum))

    # unique accounts
    count_dest, count_src = tuple(
        time_filter(session.query(col).filter(Transaction.version != 0)).distinct().count()
        for col in (Transaction.dest, Transaction.src)
    )

    return (blocks_delta, *dhms, blocks_delta/td.total_seconds(), 100*mint_count/blocks_delta,
        100*p2p_count/blocks_delta, 100*other_count/blocks_delta, mint_sum, p2p_sum, other_sum,
        count_dest, count_src)
