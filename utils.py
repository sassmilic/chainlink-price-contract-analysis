import bisect
import glob
import json
import math
import os
import pandas as pd
import re
import requests
import time
from datetime import datetime, timedelta
from tinydb import TinyDB, Query, where
from tqdm import tqdm
# disable locks due to random deadlocks
tqdm.get_lock().locks = []
from websockets import ConnectionClosedError
from web3 import Web3

from configs import (
    INFURA_URL,
    INFURA_PROVIDER,
    ETH_USD_PRICE_CONTRACT_ADDR,
    CHAINLINK_ABI,
    CRYPTO_COMPARE_API_URL,
    CRYPTO_COMPARE_API_HEADER
)

convert_unixtime = lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
filename = lambda rnd, other_info: 'eth_usd_price_round_' + '{}_{}.csv'.format(rnd, other_info)
FILE_PREFIX = 'eth_usd_price_round'

### LOAD TINY DATABASE ###

DB = TinyDB('db.json')

# database tables
ORACLE_RESPONSE_TABLE = 'oracle_responses'
ETH_USD_PRICE_TABLE = 'eth-usd-prices'

def connect():
    web3 = Web3(Web3.WebsocketProvider(INFURA_PROVIDER))
    contract = web3.eth.contract(ETH_USD_PRICE_CONTRACT_ADDR, abi=CHAINLINK_ABI)
    return web3, contract

def tinydb_table_to_dataframe(table):
    d = {}
    for i,row in enumerate(table):
        if i == 0:
            d = {k: [] for k in row.keys()}
        for k in d.keys():
            d[k].append(row[k])
    return pd.DataFrame(d)

def dataframe_to_tinydb_rows(df):
    d = df.to_dict()
    return [{k: d[k][i] for k in d.keys()} for i in range(df.shape[0])]

def load_all_price_response_data():
    # assumes table 'oracle_responses' in `DB` in not empty
    web3, contract = connect()
    
    table = DB.table(ORACLE_RESPONSE_TABLE)
   
    # check if data is already up-to-date
    latest_round = contract.functions.latestRound().call()
    print('Latest round: {}'.format(latest_round))
  
    last_round = max(row['answer_id'] for row in table)
    print('Last saved round: {}.'.format(last_round))
    if last_round == latest_round:
        # return dataframe
        return tinydb_table_to_dataframe(table)

    first_block = approx_earliest_eth_block(last_round + 1)['number']
    entries = get_all_entries(
        contract.events.ResponseReceived(),
        first_block,
        query=(
            ('args', 'response'),
            ('args', 'answerId'),
            ('args', 'sender'),
            ('blockNumber',)
        ),
        chunk_size=500
    )
    
    df = pd.DataFrame(entries)
    df.rename(columns={
        'response': 'price',
        'answerId': 'answer_id',
        'sender': 'oracle'}
    )
    rows = dataframe_to_tinydb_rows(df)
    print('Inserting new rows...')
    table.insert_multiple(rows)

    _augment_price_response_data(table)

    rows = dataframe_to_tinydb_rows(df)
    table.insert_multiple(rows)

    return tinydb_table_to_dataframe(table)

def _augment_price_response_data(table):

    web3, contract = connect()

    print('Augmenting entries...')

    def _add_timestamps():
        def transform(row):
            pbar.update(1)
            ts =  web3.eth.getBlock(row['blockNumber'])['timestamp']
            row['timestamp'] = ts
        return transform

    def _add_human_readable_date():
        def transform(row):
            row['date'] = convert_unixtime(row['timestamp'])
        return transform

    def _add_price_float():
        def transform(row):
            row['price_float'] = row['price'] / 1e8
        return transform

    Response = Query()

    print('Adding timestamps...')
    time.sleep(1)
    # takes a while; add progressbar
    print(table)
    pbar = tqdm(total=sum(1 for row in table if 'timestamp' in row))
    table.update(_add_timestamps(), ~Response.timestamp.exists())

    # convert timestamp to human readable time
    table.update(_add_human_readable_date(), ~Response.date.exists())

    # convert price into float
    table.update(_add_price_float(), ~Response.price_float.exists())

    print('Done.')

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

def load_all_price_data(from_ts=None, to_ts=None):
    '''
    Loads price data from tinydb.
    Also, queries for new data and saves it to tinydb.
    '''

    if not to_ts:
        to_ts = int(time.time())
    if not from_ts:
        # crypto compare data goes back one week
        from_ts = to_ts - timedelta(days=8).total_seconds()
        from_ts = int(from_ts)

    table = DB.table(ETH_USD_PRICE_TABLE)
    
    timestamps = [row['time'] for row in table]
    timestamps = [from_ts] + timestamps + [to_ts]
    missing_intervals = __missing_intervals(timestamps, 60)
    
    print('Data missing for following intervals:')
    for mi in missing_intervals:
        print('({}, {})'.format(*[convert_unixtime(t) for t in mi]))

    for from_ts, to_ts in missing_intervals:
        df = get_prices(from_ts, to_ts)
        print('{} data points gathered from CryptoCompare API'.format(df.shape[0]))
        rows = dataframe_to_tinydb_rows(df)
        # insert into table
        __insert_non_duplicate(table, rows, 'time')

    df = tinydb_table_to_dataframe(table)
    # remove potential duplicates
    df.drop_duplicates(keep=False, inplace=True)
    return df

def __missing_intervals(l, length):
    '''
    Return a list of tuples representing missing intervals in `l`
    longer than `length`.
    '''
    l.sort()
    ints = []
    for i in range(len(l) - 1):
        if l[i + 1] - l[i] > length:
            ints.append((l[i], l[i + 1]))
    return __merge_intervals(ints)

def __merge_intervals(arr):
    # a leetcode problem!
    arr.sort(key=lambda x: x[0])
    # array to hold the merged intervals
    merged = []
    s = None
    max_ = -math.inf
    for i in range(len(arr)):
        t = arr[i]
        if t[0] > max_ + 1:
            if i != 0:
                merged.append([s, max_])
            max_ = t[1]
            s = t[0]
        else:
            if t[1] > max_ + 1:
                max_ = t[1]

    if max_ != math.inf and [s, max_] not in merged:
        merged.append([s, max_])
    
    return merged

def __insert_non_duplicate(table, rows, key):
    '''
    Only insert rows in `rows` whose key `key' isn't already in table.
    Note: might take a while
    '''
    print('Inserting rows in table...')

    rows_to_insert = []

    count = 0
    keys = set(row[key] for row in table)
    for row in tqdm(rows):
        if not row[key] in keys:
            rows_to_insert.append(row)
    table.insert_multiple(rows_to_insert)
    print('{} rows inserted.'.format(len(rows_to_insert)))

def get_prices(from_ts, to_ts, fsym='ETH', tsym='USD', limit=2000):
    # NOTE: cryptocompare doesn't allow queries for prices older than a week
    print('Querying dates {} to {}.'.format(convert_unixtime(from_ts), convert_unixtime(to_ts)))

    res = {
        'time': [],
        'high': [],
        'low': [],
        'open': [],
        'close': [],
        'volumefrom': [],
        'volumeto': []
    }

    r = requests.get(CRYPTO_COMPARE_API_URL.format(
            fsym=fsym,
            tsym=tsym,
            to_ts=to_ts,
            limit=limit),
        headers=CRYPTO_COMPARE_API_HEADER)
    
    d = json.loads(r.content.decode('utf-8'))

    if d.get('Response') == 'Error':
        print(d['Message'])
        return pd.DataFrame(res)

    for x in d['Data']['Data']:
        if x['time'] < from_ts:
            continue
        for k in res.keys():
            res[k].append(x[k])

    if res['time'] and min(res['time']) > to_ts:
        # need to keep querying to get all data in range
        df_ = get_prices(from_ts, min(res['time']) - 1, fsym, tsym, limit)
        df = pd.concat([pd.DataFrame(res), df_])
        df.reset_index(inplace=True, drop=True)
        return df

    return pd.DataFrame(res)
       
def load_round_metric_data():
    '''
    For each round, we want:
        length of round
        standard deviation of reporting times
        price volatility (between start and end of round)
        how much does this explain the variability in the responses?
    '''
    web3, contract = connect()
    
    latest_round = contract.functions.latestRound().call()

    # get start and end times of rounds
    first_block = approx_earliest_eth_block(last_round + 1)['number']
    start = get_all_entries(
        contract.events.NewRound(),
        first_block,
        query=(('args', 'roundId'), ('blockNumber',)),
        chunk_size=2000
    )

    print('Converting block numbers to timestamps...')
    start['ts_start'] = []
    end = {'roundId':[], 'ts_end': []}
    for i,rnd in enumerate(tqdm(start['roundId'])):
        start['ts_start'].append(web3.eth.getBlock(start['blockNumber'][i])['timestamp'])
        end['roundId'].append(rnd)
        end['ts_end'].append(contract.functions.getTimestamp(rnd).call())

    # change column names
    df_start = pd.DataFrame(start).set_index('roundId')
    df_end = pd.DataFrame(end).set_index('roundId')
    df_ts = df_start.join(df_end)

    if files:
        df_ts = pd.concat([df_old, df_ts])

    if save:
        latest = latest_block = web3.eth.getBlock('latest')['number']
        df_ts.to_csv('start_end_timestamps_round{}.csv'.format(latest))

    return df_ts


if __name__ == "__main__":
    # testing
    df = load_all_price_response_data()
    print(df)
    #to_ts = int(time.time())
    #from_ts = to_ts - 60*60*24*8
    #get_prices(from_ts, to_ts)
    #print(load_all_price_data())
