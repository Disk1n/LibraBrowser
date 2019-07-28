# DB worker code

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)


###########
# Imports #
###########
from sqlalchemy import create_engine, func
from sqlalchemy.ext.serializer import dumps
from threading import Thread
from time import sleep, gmtime, strftime
import gzip

from rpc_client import get_latest_version_from_ledger, get_raw_tx_lst, parse_raw_tx_lst
from models import Session, Base, Transaction, session_scope

#########
# Funcs #
#########
def get_latest_version(session):
    return session.query(func.max(Transaction.version)).scalar()


#############
# DB Worker #
#############

class TxDBWorker(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.url = "{DB_DIALECT}+{DB_DRIVER}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**config)
        logger.info('sqlalchemy.url: {}'.format(self.url))
        self.db_backup_path = config['DB_BACKUP_PATH']
        self.running = False

    def run(self):
        while True:
            logger.info('transactions db worker starting')
            engine = create_engine(self.url)
            Session.configure(bind=engine)
            Base.metadata.create_all(engine)

            # get latest version in the db
            with session_scope() as session:
                cur_ver = session.query(func.max(Transaction.version)).scalar()
            cur_ver = (cur_ver + 1) if cur_ver else 1  # TODO: later handle genesis
            
            try:
                logger.info('starting update at version {}'.format(cur_ver))
                # start the main loop
                while True:
                    try:
                        bver = get_latest_version_from_ledger()
                    except:
                        sleep(1)
                        continue
                    if cur_ver > bver + 50:
                        # +50 for safety due to chance we're not in sync with latest blockchain ver
                        file_path = '{}_{}.gz'.format(self.db_backup_path, strftime('%Y%m%d%H%M%S'))
                        logger.info('saving database to {}'.format(file_path))
                        with gzip.open(file_path, 'wb') as f, session_scope() as session:
                            f.write(dumps(session.query(Transaction)))
                        Base.metadata.drop_all(engine)
                        Base.metadata.create_all(engine)
                        break
                    elif cur_ver > bver:
                        sleep(1)
                        continue

                    # batch update
                    num = min(1000, bver - cur_ver)  # at most 5000 records at once
                    tx_data = get_raw_tx_lst(cur_ver, num)

                    # read records
                    res = parse_raw_tx_lst(*tx_data)
                    if not res:
                        sleep(5)
                        continue

                    # do the insertion
                    with session_scope() as session:
                        session.add_all(Transaction(**v) for v in res)
                    # update counter to the latest version we inserted
                    cur_ver = res[-1]['version']
                    logger.debug('update to version: {} - success'.format(cur_ver))

                    # update latest version to next
                    cur_ver = cur_ver + 1

                    # sleep relative to amount of rows fetched so we don't get a 429 error
                    sleep(0.001 * num)
                    self.running = True

            except:
                logger.exception('Major error in tx_db_worker')
                sleep(2)
