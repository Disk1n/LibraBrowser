# Library to fetch information from Libra via rpc
# based on https://github.com/egorsmkv/libra-grpc-py/

##########
# Logger #
##########
import logging
logger = logging.getLogger(__name__)

###########
# Imports #
###########
import grpc

from lib.admission_control_pb2_grpc import AdmissionControlStub
from lib.get_with_proof_pb2 import UpdateToLatestLedgerRequest, GetAccountStateRequest, \
    RequestItem, GetTransactionsRequest
from lib.transaction_pb2 import RawTransaction

import struct
from hexdump import hexdump

import sys
from datetime import datetime


###########
# Globals #
###########
SERVER_ADDRESS = ''
MINT_ACCOUNT = ''
stub = None
last_version_seen = 0


#########
# Funcs #
#########
def start_rpc_client_instance(rpc_server, mint_addr):
    global last_version_seen
    global stub
    global SERVER_ADDRESS
    global MINT_ACCOUNT

    SERVER_ADDRESS = rpc_server
    MINT_ACCOUNT = mint_addr

    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = AdmissionControlStub(channel)

    last_version_seen = get_latest_version_from_ledger()

    return stub


def get_latest_version_from_ledger():
    global last_version_seen
    global stub

    request = UpdateToLatestLedgerRequest(client_known_version=last_version_seen, requested_items=[])
    response = stub.UpdateToLatestLedger(request)
    ledger_info = response.ledger_info_with_sigs.ledger_info

    last_version_seen = ledger_info.version
    logger.debug('last version seen: {}'.format(last_version_seen))
    return last_version_seen


def get_acct_raw(acct):
    global last_version_seen
    global stub

    account = GetAccountStateRequest(address=bytes.fromhex(acct))
    item = RequestItem(get_account_state_request=account)
    request = UpdateToLatestLedgerRequest(client_known_version=last_version_seen, requested_items=[item])
    response = stub.UpdateToLatestLedger(request)

    return response.response_items[0].get_account_state_response


def get_acct_info(state):
    try:
        #struct inferred from: https://github.com/libra/libra/blob/master/types/src/account_config.rs#L129
        acct_state = struct.unpack_from('32sQQQQ', state.account_state_with_proof.blob.blob,
                           len(state.account_state_with_proof.blob.blob) - struct.calcsize('32sQQQQ'))

        account = bytes.hex(acct_state[0])
        balance = acct_state[1] / 1000000
        recv_events = acct_state[2]
        sent_events = acct_state[3]
        sq_num = acct_state[4]
    except:
        logger.exception()

    return account, balance, sq_num, sent_events, recv_events


def get_raw_tx_lst(version, limit):
    global last_version_seen
    global stub

    tx_req = GetTransactionsRequest(start_version=version, limit=limit, fetch_events=True)
    item = RequestItem(get_transactions_request=tx_req)
    request = UpdateToLatestLedgerRequest(client_known_version=last_version_seen, requested_items=[item])
    response = stub.UpdateToLatestLedger(request)

    infos = response.response_items[0].get_transactions_response.txn_list_with_proof.infos
    raw = response.response_items[0].get_transactions_response.txn_list_with_proof
    events = response.response_items[0].get_transactions_response.txn_list_with_proof.events_for_versions

    tx_struct = []
    for x in raw.transactions:
        y = RawTransaction()
        y.ParseFromString(x.raw_txn_bytes)
        tx_struct.append(y)

    return tx_struct, infos, raw, events


def parse_raw_tx_lst(struct_lst, infos, raw, events):
    cur_ver = raw.first_transaction_version.value
    res = []

    for x in zip(infos, struct_lst, raw.transactions): #, events):
        info = x[0]
        tx = x[1]
        r = x[2]
        tmp = dict()

        tmp['version'] = cur_ver
        cur_ver += 1

        tmp['expiration'] = str(datetime.fromtimestamp(min(tx.expiration_time, 2147485547)))
        tmp['sender'] = bytes.hex(tx.sender_account)
        tmp['target'] = bytes.hex(tx.program.arguments[0].data)
        tmp['t_type'] = 'peer_to_peer_transaction' if tmp['sender'] != MINT_ACCOUNT else 'mint_transaction'
        tmp['amount'] = tx.program.arguments[1].data
        tmp['gas_price'] = struct.pack("<Q", tx.gas_unit_price)
        tmp['gas_max'] = struct.pack("<Q", tx.max_gas_amount)
        tmp['sq_num'] = tx.sequence_number
        tmp['pubkey'] = bytes.hex(r.sender_public_key)
        tmp['expiration_num'] = min(tx.expiration_time, 2**63 - 1)

        tmp['gas_used'] = struct.pack("<Q", info.gas_used)

        tmp['sender_sig'] = bytes.hex(r.sender_signature)
        tmp['signed_tx_hash'] = bytes.hex(info.signed_transaction_hash)
        tmp['state_root_hash'] = bytes.hex(info.state_root_hash)
        tmp['event_root_hash'] = bytes.hex(info.event_root_hash)
        tmp['code'] = hexdump(tx.program.code, result='return')
        tmp['program'] = str(tx.program)
        #tmp['events'] = events

        res.append(tmp)

    return res
