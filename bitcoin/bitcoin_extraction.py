from sqlalchemy.orm import sessionmaker

__author__ = "haaroony"

# blocks
# transactions


# set up a postgres server and server tools for sqlalchemy


# It needs to print nice things to screen
# Check if the CLI is active
# check the latest block
# check the latest block in the database
# pull the next block from the cli
# if there is an error say the error message
# break
# continue this for all the blocks

import simplejson
import bitcoin
import bitcoin.rpc
import config
import table_classes
import sys
from socket import error as socket_error
import errno
from sqlalchemy import exc
import dbLib


def checkBitcoinCLI():
    """
    Checks if the Bitcoind is running
    Prints stuff, returns nothing or exits the program
    """
    print("Setting environment variable to mainnet")
    bitcoin.SelectParams("mainnet")
    try:
        proxy = bitcoin.rpc.RawProxy(
                service_port = config.BITCOIN_RPC_PORT,
                btc_conf_file = config.BITCOIN_CONF_LOC)
        info = proxy.getinfo()
        print("Bitcoind running")
        print("Total blocks in Bitcoin : ", info["blocks"])
        print("Current relay fee: ", str(info["relayfee"]))
        print("Current transaction fee: ", str(info["paytxfee"]))
        print("Version: ", str(info["version"]))
    except IOError as e:
        print("IO Error, No config file found in ", config.BITCOIN_CONF_LOC)
        print("Cannot run without config, exiting program")
        sys.exit(1)
    except ValueError as e:
        print("Error parsing configuration file")
        print(e.message)
        print("Exiting")
        sys.exit(1)
    except socket_error as serr:
        if serr.errno != errno.ECONNREFUSED:
            # different error
            raise serr
        else:
            # connection refused
            print("ERROR: Connection refused")
            print("Likely Bitcoind is not running")
            print("Please run Bitcoind or check config")
            print("Exiting...")
            sys.exit(1)

def checkPostGreSQL():
    '''
    Checks if the postgresql server is up and running
    If not it just exits
    returns nothing
    '''
    try:
        # check if there is a server connection
        con, meta = dbLib.connect(config.POSTGRES_USER, config.POSTGRES_PASS, config.POSTGRES_DB)
        # check if tables exist
        # check for table blocks and transactions
        if con.has_table("blocks") and con.has_table("transactions"):
            print("Table \'blocks\' and \'transactions\' are present")
        else:
            print("Table \'blocks\' or \'transactions\' is missing, please check database")
            print("Exiting")
            sys.exit(1)
        # print number of blocks present
        # print number of transactions present
    except exc.SQLAlchemyError as e:
        print("SQLAlchemy error: ", e)
        print("Exiting")
        sys.exit(1)

def rpcConnection():
    return bitcoin.rpc.RawProxy(
        service_port= config.BITCOIN_RPC_PORT,
        btc_conf_file= config.BITCOIN_CONF_LOC)

def updateDatabase(currentBlockHeight):
    '''
    Updates the database with the latest blocks from the blockchain
    :param currentBlockHeight: the latest block height in the database, one to start from
    :return: nothing
    '''
    # Run RPC and get latest block height
    print("Updating database")
    print("Using mainnet")
    bitcoin.SelectParams("mainnet")
    print("Creating RPC connection")
    rpc = rpcConnection()
    print("Getting the latest block from the network")
    latestBlock = int(rpc.getblockcount())
    print("Latest block is : "+ str(latestBlock))

    # print stuff
    if currentBlockHeight == "None":
        currentBlockHeight = -1
    print("Collecting blocks from range " + str(currentBlockHeight) + " to " + str(latestBlock))
    for blockHeight in range(int(currentBlockHeight)+1, latestBlock+1, 1):
        print("Block " + str(blockHeight) +  "/" + str(latestBlock))
        objects = []
        rpc = rpcConnection()
        block = rpc.getblock(rpc.getblockhash(blockHeight))
        #block = rpc.getblock(str(blockHeight))
        previousblockhash = ""
        if "previousblockhash" in block.keys():
            previousblockhash = block["previousblockhash"]
        nextblockhash = ""
        if "nextblockhash" in block.keys():
            nextblockhash = block["nextblockhash"]
        databaseBlock = table_classes.Blocks(
            height = block["height"],
            tx_hash = block["hash"],
            size = block["size"],
            version = block["version"],
            merkleroot = block["merkleroot"],
            tx = list(block["tx"]),
            time = block["time"],
            nonce = block["nonce"],
            difficulty = block["difficulty"],
            chainwork = block["chainwork"],
            previousblockhash = previousblockhash,
            nextblockhash = nextblockhash,
        )
        objects.append(databaseBlock)
        #print("Will add block once we have all transactions")
        # do the same for transactions
        # print(str(len(block["tx"])) +" transactions found in block")
        for tx_hash in block["tx"]:
            skip = 0
            rpc = rpcConnection()
            try:
                tx = rpc.getrawtransaction(tx_hash, 1)
            except bitcoin.rpc.InvalidAddressOrKeyError:
                print("Could not find TX, no information")
                print("Setting value to False")
                skip=1

            if skip:
                transaction = table_classes.Transactions(
                    tx_hash=tx_hash,
                    version=0,
                    size =0,
                    vsize =0,
                    locktime=0,
                    blockhash=block["hash"],
                    time=0,
                    blocktime=0
                )
                objects.append(transaction)
                continue
            else:
                transaction = table_classes.Transactions(
                    tx_hash = tx_hash,
                    tx_id = tx["txid"],
                    version = tx["version"],
                    size = tx["size"],
                    vsize =tx["vsize"],
                    locktime = tx["locktime"],
                    blockhash = tx["blockhash"],
                    tx_hex = tx["hex"],
                    time = tx["time"],
                    blocktime = tx["blocktime"],
                    tx_in_count = len(tx["vin"]),
                    tx_out_count = len(tx["vout"])
                )
            objects.append(transaction)
            # now check the values to see if its a coingen, vin, vout and vjoinsplit fieldssss
            if len(tx["vin"]):
                for val in tx["vin"]:
                    if "coinbase" in val.keys():
                        # we found a coingen transaction, create the object and add it
                        coingen = table_classes.Coingen(
                            tx_hash = tx_hash,
                            coinbase = val["coinbase"],
                            sequence = val["sequence"]
                        )
                        objects.append(coingen)
                    else:
                        # else its a normal vin duh
                        vin = table_classes.Vin(
                            tx_hash = tx_hash,
                            prevout_hash = val["txid"],
                            prevout_n = val["vout"],
                            sequence = val["sequence"]
                        )
                        objects.append(vin)
            if len(tx["vout"]):
                for val in tx["vout"]:
                    pubkey = []
                    if "addresses" in val["scriptPubKey"]:
                        pubkey = val["scriptPubKey"]["addresses"]
                    vout = table_classes.Vout(
                        tx_hash =  tx_hash,
                        value = val["value"],
                        tx_n = val["n"],
                        pubkey = pubkey
                        #script =
                    )
                    objects.append(vout)
        dbLib.addObjects(objects)
        # print("Block complete")
    print("Processed all blocks. Closing program")


def checkStatus():
    """
    Checks if the Bitcoin process and PostGreSQL databases are active
    """
    checkBitcoinCLI()
    checkPostGreSQL()
    currentBlockHeight = dbLib.getLatestBlockheight()
    print("Current block height in database ", currentBlockHeight)
    updateDatabase(currentBlockHeight)


if __name__ == '__main__':
    """
    Initiate program
    """
    print("******** Bitcoin blocks and transaction updater *********")
    print("INFO: \n")
    print("> This program will check the local PostGreSQL database")
    print("> and update the blocks and respective transactions.")
    print("> Once complete the program will close.")
    print("> It is meant to be run as a chron job, not as a standalone application.\n")
checkStatus()