import time
from datetime import datetime
from tqdm import tqdm
# disable locks due to random deadlocks
tqdm.get_lock().locks = []
from websockets import ConnectionClosedError
from web3 import Web3

from configs import (
    INFURA_URL,
    INFURA_PROVIDER,
    ETH_USD_PRICE_CONTRACT_ADDR,
    CHAINLINK_ABI
)

convert_unixtime = lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
filename = lambda rnd, other_info: 'eth_usd_price_round_' + '{}_{}.csv'.format(rnd, other_info)
FILE_PREFIX = 'eth_usd_price_round'


def connect():
    web3 = Web3(Web3.WebsocketProvider(INFURA_PROVIDER))
    contract = web3.eth.contract(ETH_USD_PRICE_CONTRACT_ADDR, abi=CHAINLINK_ABI)
    return web3, contract


def approx_earliest_eth_block(rnd):
    '''
    Return approximately the first Ethereum block we care about,
    namely one that was mined not long *before* round `rnd` in our contract.
    '''
    web3, contract = connect()
    
    ts_response = contract.functions.getTimestamp(rnd).call()
    current_time = time.time()
    latest_block = web3.eth.getBlock('latest')
    
    block_time = 15
    seconds_in_day = 86400
    
    first_block = latest_block
    # check if `first_block` was mined before `ts_first_response`
    while first_block['timestamp'] - ts_response > 0:
        diff_days = (first_block['timestamp'] - ts_response) // seconds_in_day
        if not diff_days:
            diff_days = 0.5
        blocks_ago = diff_days * (seconds_in_day / block_time)
        first_block_num = int(first_block['number'] - blocks_ago)
        first_block = web3.eth.getBlock(first_block_num)
        
    print('First block: {}.\nRound {} USD-ETH price response: {}.'.format(
        convert_unixtime(first_block['timestamp']),
        rnd,
        convert_unixtime(ts_response)
    ))
    
    return first_block


def get_all_entries(event, first_block, last_block=None, chunk_size=500, query=None):
    '''
    We query the blockchain in chunks to mitigate disconnection issues.

    event - `web3._utils.datatypes.Event` type
    first_block - block number where we start querying
    chunk_size - optional; number of contingent blocks we query at a given time
    query - optional;
        specifies which data we want returned from `web3.utils.filters.Filter.get_all_entries()`;
        list of tuples, where elements of tuples are keys in recursive order;
        e.g. [(k1, k2), (k1, k3), (k4,)] will access elements `entry` in array returned from
        `get_all_entries()` in the following manner: entry[k1][k2], entry[k1][k3], entry[k4];
        and return a dict in the form:
            {
                k2: [...],
                k3: [...],
                k4: [...]
            }
    '''
    if not query:
        result = []
    else:
        result = {}
        for t in query:
            result[t[-1]] = []

    web3, contract = connect()

    if not last_block:
        latest_block = web3.eth.getBlock('latest')['number']
    else:
        latest_block = last_block
    # query blocks in chunks
    nchunks = (latest_block - first_block) // chunk_size + 1
    print("Querying blockchain in chunks of {} blocks. {} chunks total.".format(chunk_size, nchunks))
    time.sleep(1)

    for chunk in tqdm(range(nchunks)):

        start_block = first_block + chunk_size * chunk

        # this shit disconnects often so ...
        # max 100 retries ...
        for i in range(100):
            try:
                filter_ = event.createFilter(
                    fromBlock=start_block, # inclusive
                    toBlock=start_block + chunk_size, # inclusive
                )
                entries = filter_.get_all_entries()
                break
            except (ConnectionClosedError, TimeoutError):
                if i % 10 == 0:
                    print('retrying x{} ...'.format(i))
                time.sleep(1)
                web3, contract = connect()
                event = contract.events.ResponseReceived()
                continue
        else:
            print('Max retries attempted ... still failed')
            print('Returning partially retrieved data.')
            return result

        for i,entry in enumerate(reversed(entries)):
            if not query:
                result.append(entry)
            else:
                for t in query:
                    d = entry
                    for k in t:
                        d = d[k]
                    result[t[-1]].append(d)

    print('Done.')
    return result


if __name__ == "__main__":
    # testing
    web3, contract = connect()
    res = approx_earliest_eth_block(1)
    print(res['number'])
    start = approx_earliest_eth_block(200)['number']
    end = approx_earliest_eth_block(202)['number']
    res = get_all_entries(
        contract.events.ResponseReceived(),
        start,
        last_block=end,
        query=(
            ('args', 'response'),
            ('args', 'answerId'),
            ('args', 'sender'),
            ('blockNumber',)
        ),
        chunk_size=500
    )
    print(res)
