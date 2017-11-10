__author__ = 'haaroony'

import sqlalchemy
from sqlalchemy.orm import sessionmaker
import config
from table_classes import Blocks
from sqlalchemy import desc, func, exc
import sys

def connect(user, password, db, host='localhost', port=5432):
    '''
    Connects to postgresql server
    Returns a connection and a metadata object
    '''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta

def addObjects(objects):
    print("Committing " +  str(len(objects)) + " objects to db")
    con, meta = connect(config.POSTGRES_USER, config.POSTGRES_PASS, config.POSTGRES_DB)
    Session = sessionmaker(con)
    session = Session()
    try:
        session.bulk_save_objects(objects)
        session.commit()
    except sqlalchemy.exc.IntegrityError:
        for obj in objects:
            session.rollback()
            session.merge(obj)
            session.commit()
    except exc.SQLAlchemyError:
        print("Sql alchemy error")
        session.rollback()
        sys.exit("Encountered general SQLAlchemyError.  Call an adult!")
    session.close()

def getLatestBlockheight():
    print("Finding the latest block")
    con, meta = connect(config.POSTGRES_USER, config.POSTGRES_PASS, config.POSTGRES_DB)
    Session = sessionmaker(con)
    session = Session()
    result = session.query(func.max(Blocks.height)).first()
    return str(result[0])
