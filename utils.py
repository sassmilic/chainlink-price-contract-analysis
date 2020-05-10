import bisect
import glob
import json
import math
import numpy as np
import os
import pandas as pd
import quantumrandom
import re
import requests
import sys
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from tinydb import TinyDB, Query, where
from tqdm.notebook import tqdm
#from tqdm import tqdm
# disable locks due to random deadlocks
tqdm.get_lock().locks = []
from websockets import ConnectionClosedError
from web3 import Web3

from configs import (
    INFURA_URL,
    INFURA_PROVIDER,
    ETH_USD_PRICE_CONTRACT_ADDR,
    CHAINLINK_ABI,
    CRYPTO_COMPARE_API_KEY,
    CRYPTO_COMPARE_API_URL,
    CRYPTO_COMPARE_API_HEADER
)

if not INFURA_URL:
    warnings.warn("Infura URL is missing. Go here to sign up for an account: https://infura.io/")

if not INFURA_PROVIDER:
    warnings.warn("Infura URL is missing. Go here to sign up for an account: https://infura.io/")

if not CRYPTO_COMPARE_API_KEY:
    warnings.warn("CryptoCompare API key is missing. Go here to set up an account: https://min-api.cryptocompare.com/pricing")

convert_unixtime = lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
filename = lambda rnd, other_info: 'eth_usd_price_round_' + '{}_{}.csv'.format(rnd, other_info)
FILE_PREFIX = 'eth_usd_price_round'

ORACLE_RESPONSE_FILE = 'oracle_responses.csv'
ETH_USD_PRICES_FILE = 'eth-usd_prices.csv'
ROUND_STATS_FILE = 'price_contract_round_stats.csv'

def connect():
    web3 = Web3(Web3.WebsocketProvider(INFURA_PROVIDER))
    contract = web3.eth.contract(ETH_USD_PRICE_CONTRACT_ADDR, abi=CHAINLINK_ABI)
    return web3, contract

def load_oracle_responses(save=True, pbar=True):
    # assumes table 'oracle_responses' in `DB` in not empty
    web3, contract = connect()

    # TODO: remove responses received after end of round

    try:
        df = pd.read_csv(ORACLE_RESPONSE_FILE, index_col=0)
    except IOError:
        df = pd.DataFrame()
            
    # check if data is already up-to-date
    latest_round = contract.functions.latestRound().call()
    print('Latest round: {}'.format(latest_round))
 
    if not df.empty:
        last_round = df.answer_id.max()
        print('Last saved round: {}.'.format(last_round))
    else:
        last_round = 0

    if last_round == latest_round:
        return df

    ts = contract.functions.getTimestamp(last_round + 1).call()
    if ts == 0:
        # current round not over
        return df

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
        chunk_size=500,
        pbar=pbar
    )
    
    df_new = pd.DataFrame(entries)
    df_new.rename(
        columns={
            'response': 'price',
            'answerId': 'answer_id',
            'sender': 'oracle'},
        inplace=True
    )

    # avoid potential duplicates
    df_new = df_new[df_new['answer_id'] > last_round]

    __augment_price_response_data(df_new)

    df = pd.concat([df, df_new], ignore_index=True)
    df.sort_values(by='answer_id', inplace=True)
    df.reset_index(drop=True, inplace=True)

    if save:
        # rewrite file
        # TODO: is there a way to do this safetly,
        # i.e. atomically?
        df.to_csv(ORACLE_RESPONSE_FILE)
    
    return df

def __augment_price_response_data(df, pbar=True):

    web3, contract = connect()

    print('Augmenting entries...')

    def add_timestamp(row):
        if pbar:
            progress.update()
        ts = web3.eth.getBlock(row['blockNumber'])['timestamp'] 
        return ts

    def add_date(row):
        return convert_unixtime(int(row['timestamp']))

    def add_price_float(row):
        return int(row['price']) / 1e8

    print('Adding timestamps...')
    time.sleep(1)
    
    if pbar:
        progress = tqdm(total=df.shape[0])

    df['timestamp'] = df.apply(lambda row: add_timestamp(row), axis=1)

    # convert timestamp to human readable time
    df['date'] = df.apply(lambda row: add_date(row), axis=1)

    # convert price into float
    df['price_float'] = df.apply(lambda row: add_price_float(row), axis=1)
    
    print('Done.')

def approx_earliest_eth_block(rnd):
    '''
    Return approximately the first Ethereum block we care about,
    namely one that was mined not long *before* round `rnd` in our contract.
    '''
    #TODO: delete prints
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

def get_all_entries(event, first_block, last_block=None, chunk_size=500, query=None, pbar=True):
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

    if pbar:
        g = tqdm(range(nchunks))
    else:
        g = range(nchunks)

    for chunk in g:

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

def load_price_data(from_ts=None, to_ts=None, save=True):
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

    try:
        df = pd.read_csv(ETH_USD_PRICES_FILE)#, index_col=0)
        timestamps = [from_ts] + [ts for ts in df.index if from_ts < ts and ts < to_ts] + [to_ts]
        missing_intervals = __missing_intervals(timestamps, 60)
    except IOError:
        df = pd.DataFrame()
        missing_intervals = [(from_ts, to_ts)]
    
    print('Data missing for following intervals:')
    for mi in missing_intervals:
        print('({}, {})'.format(*[convert_unixtime(t) for t in mi]))

    for from_ts, to_ts in missing_intervals:
        df_ = get_prices(from_ts, to_ts)
        print('{} data points gathered from CryptoCompare API'.format(df_.shape[0]))
        df = pd.concat([df, df_])
    
    # just in case, sort and drop dupes
    df.sort_values(by=['time'], inplace=True)
    df.drop_duplicates(subset=['time'], inplace=True)

    if save:
        # rewrite file
        # TODO: is there a way to do this safetly,
        # i.e. atomically?
        df.to_csv(ETH_USD_PRICES_FILE, index=False)

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

def get_prices(from_ts, to_ts, fsym='ETH', tsym='USD', limit=2000):
    # NOTE: cryptocompare doesn't allow queries for prices older than a week
    print('Querying dates {} to {}.'.format(convert_unixtime(from_ts), convert_unixtime(to_ts)))

    keys = ['time', 'high', 'low', 'open', 'close', 'volumefrom', 'volumeto']
    res = {k:[] for k in keys}
    
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

    if res['time'] and min(res['time']) > from_ts:
        # need to keep querying to get all data in range
        df_ = get_prices(from_ts, min(res['time']) - 1, fsym, tsym, limit)
        df = pd.concat([pd.DataFrame(res), df_])
        #df.set_index('time', inplace=True)
        return df

    df = pd.DataFrame(res)
    # ensure column order
    #df.set_index('time', inplace=True)
    print('HERE')
    return df

def load_round_metrics(price_df, response_df, save=True, pbar=True):
    '''
    For each round, we want:
        length of round
        standard deviation of reporting times
        price volatility (between start and end of round)
        how much does this explain the variability in the responses?
    '''
    web3, contract = connect()

    try:
        df = pd.read_csv(ROUND_STATS_FILE, index_col=0)
        last_round = max(df.index)
    except IOError:
        df = pd.DataFrame()
        last_round = 0

    latest_round = contract.functions.latestRound().call()

    if latest_round == last_round:
        return df

    ts = contract.functions.getTimestamp(last_round + 1).call()
    if ts == 0:
        # current round not over
        return df

    # otherwise, update data
    
    # - get start times of rounds
    # - we let the timestamp of the block with `NewRound` event be the start time
    
    print('Getting start times of rounds...')
    first_block = approx_earliest_eth_block(last_round + 1)['number']
    
    start = get_all_entries(
        contract.events.NewRound(),
        first_block,
        query=(('args', 'roundId'), ('blockNumber',)),
        chunk_size=2000
    )

    print('Converting block numbers to timestamps...')
    
    # end times of rounds are computed by calling event `getTimestamp` 
    start['ts_start'] = []
    end = {'round_id':[], 'ts_end': []}

    if pbar:
        g =  enumerate(tqdm(start['roundId']))
    else:
        g =  enumerate(start['roundId'])

    for i,rnd in g:
        start['ts_start'].append(web3.eth.getBlock(start['blockNumber'][i])['timestamp'])
        end['round_id'].append(rnd)
        end['ts_end'].append(contract.functions.getTimestamp(rnd).call())

    # change column names
    df_start = pd.DataFrame(start)
    df_end = pd.DataFrame(end)
    df_start.rename(
        columns={
            "roundId": "round_id",
            "blockNumber": "block_number"
        },
        inplace=True
    )
    df_start.set_index('round_id', inplace=True)
    df_end.set_index('round_id', inplace=True)
    df_ts = df_start.join(df_end)

    # concat with previous data
    df_ts = pd.concat([df, df_ts])
    df_ts.sort_index(inplace=True)
    #df_ts.sort_values(by=['round_id'], inplace=True)

    __augment_round_data(df_ts, price_df, response_df)

    if save:
        df_ts.to_csv(ROUND_STATS_FILE)

    return df_ts

def __augment_round_data(df_ts, price_df, response_df):
    '''
    For each round, we want:
        - length of round
        - standard deviation of reporting times
        - price volatility (between start and end of round)
        - how much does this explain the variability in the responses?

    All in-place, returns None.
    '''

    web3, contract = connect()

    print('Augmenting round data...')

    def add_price_volatility(row):
        rnd = row.name # round id
        start = row['ts_start']
        end = row['ts_end']
        df = price_df[(start <= price_df['time']) & (price_df['time'] <= end)]
        # compute std of prices in this time range 
        vol = df['avg_price'].values.std()
        return vol

    def approx_avg_price(row):
        keys = ['high', 'low', 'open', 'close']
        avg = sum(row[k] for k in keys) / len(keys)
        return avg

    #print('Computing approximate average price...')
    price_df['avg_price'] = price_df.apply(lambda row: approx_avg_price(row), axis=1)

    print('Computing length of rounds...')
    df_ts['round_length'] = df_ts['ts_end'] - df_ts['ts_start']
   
    print('Computing standard deviation of response times...')
    # if oracle reponded more than once, pick last response
    df_ts['response_std_ts'] = response_df.\
        sort_values(by=['timestamp']).\
        drop_duplicates(subset=['answer_id', 'oracle'], keep='last').\
        groupby('answer_id').agg(np.std)['timestamp'] 

    df = response_df.\
        sort_values(by=['timestamp']).\
        drop_duplicates(subset=['answer_id', 'oracle'], keep='last').\
        groupby('answer_id').agg(np.std)

    print('Computing standard deviation of prices...')
    # if oracle reponded more than once, pick last response
    df_ts['response_std_price'] = response_df.\
        sort_values(by=['timestamp']).\
        drop_duplicates(subset=['answer_id', 'oracle'], keep='last').\
        groupby('answer_id').agg(np.std)['price_float'] 

    print('Computing price volatility...')
    # make sure sorted
    df_ts.sort_values(by=['ts_start'], inplace=True)
    #df_ts.sort_index(inplace=True)
    df_ts['volatility'] = df_ts.apply(lambda row: add_price_volatility(row), axis=1)

def generate_random_gaussians(sample_size, ndists, verbose=True):
    if verbose:
        print('Drawing {} random samples (n={}) from Gaussian'.format(ndists, sample_size))
    d = defaultdict(list)
    for i in range(ndists):
        # we even seed with a quantum random number generator!
        seed = quantumrandom.get_data()
        np.random.seed(seed)
        x = np.random.normal(size=sample_size)
        for j in range(21):
            d['i'].append(i)
            d['x'].append(x[j])
    return pd.DataFrame(d)

if __name__ == "__main__":
    ###
    ### testing
    ###
    #oracle_df = load_oracle_responses()
    price_df = load_price_data()
    #print(price_df)
    #df = load_round_metrics(price_df, oracle_df)
    print(price_df)
    #print(round_df)
