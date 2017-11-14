To run this code you need to have python2 and install the requirements.txt using pip.
You must also have a local bitcoin node running with all blocks and transactions indexed. 
It must be updated to the latest block, with rpc and rpc passwords. 
Then edit the config with such details.
Next you must have a psql server running, then run the setup.py script.
Setup.py will remove the existing database and add a clean one with all the tables.
Then run bitcoin_extraction.py
bobs your uncle



