# LibraBrowser
A Block Explorer for the Libra Blockchain TestNet. See: https://librabrowser.io

## Features
* [Account View](https://librabrowser.io/account/e945eec0f64069d4f171d394aa27881fabcbd3bb6bcc893162e60ad3d6c9feec) 
* [Version View](https://librabrowser.io/version/1), including gas spend and program info as well as information useful to debug the network
* [A Faucet](https://librabrowser.io/faucet) that sends the funds as p2p transaction
* [Network Statistics](https://librabrowser.io/stats)
* RPC based client to read data
* DB store of transactions
* Search by account or version
* Simple Libra client automation (soon to be deprecated)

## Installation
1. Install Libra per official instructions
2. Run: pip3 install grpcio grpcio-tools hexdump Flask Flask-Caching sqlalchemy psycopg2
3. Open the official client, create an account and save the account to disk (should be set in ACCOUNT_FILE setting)
4. Edit config.json and make sure that settings match your environment (in particular CLIENT_PATH)

## Database
* Default config assumes a [postgresql](https://wiki.postgresql.org/wiki/Main_Page) database with:
    * username = postgres
    * password = postgres
    * host = localhost
    * port = 5432
    * database name = libra_browser
    * Please see [SQLAlchemy Docs](https://docs.sqlalchemy.org/en/13/core/engines.html) for configuration options
* make sure that /etc/postgresql/<PGSQL_VERSION>/main/pg_hba.conf has the configuration of password, i.e. auth method md5 and not peer:
> local   all             postgres                                md5
* To create the DB after installing postgresql you can run: sudo -u postgres createdb libra_browser

## Running the project
At the root project folder execute the command:
> python3 Browser.py

Or to execute and leave it to run with output redirected to a file execute:
> nohup python3 Browser.py &> browser.log < /dev/null &  
> tail -f browser.log     #if you want to see the logs

To use "DEVELOPMENT" mode settings set the environment variable "BROWSER=DEVELOPMENT" 

## Contributing
[Please see Contributing.md](https://github.com/Disk1n/LibraBrowser/blob/master/CONTRIBUTING.md)

## Credits
rpc support is based on: https://github.com/egorsmkv/libra-grpc-py  
Contributors: [@gdbaldw](https://github.com/gdbaldw)  [@lucasverra](https://github.com/lucasverra)

