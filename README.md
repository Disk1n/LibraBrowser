# LibraBrowser
A browser for the Libra Blockchain TestNet. See: https://librabrowser.io

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
2. make sure the CLIENT_PATH variable is correct
3. pip3 install grpcio grpcio-tools hexdump

## Running the project
At the root project folder execute the command:
> python3 Browser.py

## Credits
rpc support is based on: https://github.com/egorsmkv/libra-grpc-py 
