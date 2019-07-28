from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, LargeBinary, String
from contextlib import contextmanager

Session = sessionmaker()

Base = declarative_base()

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class Transaction(Base):
    __tablename__ = 'transactions'

    version = Column(Integer, primary_key=True)
    expiration_date = Column(String)
    src = Column(String)
    dest = Column(String)
    type = Column(String)
    amount = Column(LargeBinary)
    gas_price = Column(LargeBinary)
    max_gas = Column(LargeBinary)
    sq_num = Column(Integer)
    pub_key = Column(String)
    expiration_unixtime = Column(BigInteger)
    gas_used = Column(LargeBinary)
    sender_sig = Column(String)
    signed_tx_hash = Column(String)
    state_root_hash = Column(String)
    event_root_hash = Column(String)
    code_hex = Column(String)
    program = Column(String)

    def __repr__(self):
        return '<Transaction(version = {0.version})>'.format(self)
