__author__ = "haaroony"

'''
PostGreSQL tables represented as classes
Its easier to call and write this way, trust me
'''

from sqlalchemy import Column, String, Integer, BigInteger, ARRAY, JSON, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
base = declarative_base()

class Blocks(base):
    __tablename__ = 'blocks'
    height = Column('height', BigInteger, primary_key=True)
    tx_hash = Column('hash', String)
    size = Column('size', Integer)
    version = Column('version', Integer)
    merkleroot = Column('merkleroot', String)
    tx = Column('tx', ARRAY(String))
    time = Column('time', BigInteger)
    nonce = Column('nonce', String)
    difficulty = Column('difficulty', BigInteger)
    chainwork = Column('chainwork', String)
    previousblockhash = Column('previousblockhash', String)
    nextblockhash = Column('nextblockhash', String)

class Transactions(base):
    __tablename__ = 'transactions'
    tx_hash = Column('tx_hash', String,  primary_key=True)
    tx_id = Column('txid', String)
    version = Column('version', Integer)
    size = Column('size',Integer)
    vsize = Column('vsize', Integer)
    locktime = Column('locktime', Integer)
    blockhash = Column('blockhash', String)
    tx_hex = Column('hex', String)
    time = Column('time', Integer)
    blocktime = Column('blocktime', Integer)
    tx_in_count = Column('tx_in_count', Integer)
    tx_out_count = Column('tx_out_count', Integer)

class Coingen(base):
    __tablename__ = "coingen"
    # db id
    db_id = Column('db_id', Integer, primary_key=True, autoincrement=True)
    tx_hash = Column('tx_hash', String, ForeignKey(Transactions.tx_hash))
    coinbase = Column('coinbase', String)
    sequence = Column('sequence', BigInteger)
    transaction = relationship('Transactions', foreign_keys='Coingen.tx_hash' )

class Vin(base):
    __tablename__ = "vin"
    # db id
    db_id = Column('db_id', Integer, primary_key=True, autoincrement=True)
    # this is the tx where the vin goes into
    tx_hash = Column('txid', String, ForeignKey(Transactions.tx_hash))
    # this is the source of the vin, the vout it came from
    prevout_hash = Column('tx_hash', String)
    # this is vin["vout"]
    prevout_n = Column('vout', Integer)
    sequence = Column('sequence', BigInteger)
    # script = Column('script', String)
    transaction = relationship('Transactions', foreign_keys='Vin.tx_hash', backref="vin")

class Vout(base):
    __tablename__ = "vout"
    # db id
    db_id = Column('db_id', Integer, primary_key=True, autoincrement=True)
    # this is the tx where the vout came from
    tx_hash = Column('hash', String, ForeignKey(Transactions.tx_hash))
    value = Column('value', Float)
    # vout["n"]
    tx_n = Column('n', Integer)
    # scriptPubKey addresses
    pubkey = Column('pubkey', ARRAY(String))
    transaction = relationship('Transactions', foreign_keys='Vout.tx_hash', backref="vout")
