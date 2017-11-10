__author__ = "haaroony"

'''
Setup script
- Run this to setup the tables
- You must have the database and user already created
'''

import config
import dbLib
from table_classes import base

if __name__ == '__main__':
    # connect to database
    print("Connecting to database")
    con, meta = dbLib.connect(config.POSTGRES_USER, config.POSTGRES_PASS, config.POSTGRES_DB)
    # create the tables using the classs
    print("Dropping all previous tables")
    base.metadata.drop_all(con.engine)
    print("Creating tables")
    base.metadata.create_all(con.engine)
    print("Done")
