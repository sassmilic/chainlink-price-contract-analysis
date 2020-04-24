import bisect
import glob
import json
import math
import os
import pandas as pd
import re
import requests
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
    CHAINLINK_ABI,
    CRYPTO_COMPARE_API_URL,
    CRYPTO_COMPARE_API_HEADER
)

convert_unixtime = lambda ts: datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
filename = lambda rnd, other_info: 'eth_usd_price_round_' + '{}_{}.csv'.format(rnd, other_info)
FILE_PREFIX = 'eth_usd_price_round'

def connect():
    web3 = Web3(Web3.WebsocketProvider(INFURA_PROVIDER))
    contract = web3.eth.contract(ETH_USD_PRICE_CONTRACT_ADDR, abi=CHAINLINK_ABI)
    return web3, contract

def load_all_price_response_data():
    web3, contract = connect()
    
    # check if data is already saved
    latest_round = contract.functions.latestRound().call()
    print('Latest round: {}'.format(latest_round))
    
    files = glob.glob('{}*'.format(FILE_PREFIX))
    if files:
        patt = filename('(\d+)', 'price_responses')
        latest_file = max(files, key=lambda fname: int(re.match(patt, fname).group(1)))
        df_old = pd.read_csv(latest_file)
        if 'Unnamed: 0' in df_old.columns:
            df_old = df_old.drop(columns=['Unnamed: 0'])
    
    # check if saved data is up-to-date
    up_to_date = True
    if files:
        last_round = int(re.match(patt, latest_file).group(1))
        if latest_round != last_round:
            up_to_date = False
    else:
        last_round = 0
        up_to_date = False

    # if not, update data
    if up_to_date:
        df = df_old
    else:
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
        df = _augment_price_response_data(entries)

    # concat with previous data
    if files and not up_to_date:
        df_answers = pd.concat([df_old, df])
    else:
        df_answers = df

    # just in case
    df_answers.sort_values(by=['answer_id'], inplace=True)
    df_answers.drop_duplicates(inplace=True)
    df_answers.reset_index(drop=True, inplace=True)
    # remove entries with answer_id <= last_round
    df = df[df.answer_id > last_round]

    # save
    df_answers.to_csv(filename(str(latest_round), 'price_responses'))
    print('Done.')
    return df_answers

def _augment_price_response_data(entries):

    web3, contract = connect()

    print('Augmenting dataframe...')
    df = pd.DataFrame(entries)
    df = df.rename(columns={'response': 'price', 'answerId': 'answer_id', 'sender': 'oracle'})

    # add timestamps
    # beware of potential disconnection
    print('Adding timestamps...')
    tss = []
    for bnum in tqdm(df['blockNumber']):
        ts = web3.eth.getBlock(bnum)['timestamp']
        tss.append(ts)
    df['timestamp'] = pd.Series(tss)

    # convert timestamp to human readable time
    df['date'] = df['timestamp'].apply(convert_unixtime)

    # convert price into float
    df['price_float'] = df['price'].apply(lambda p: p / 1e8)

    return df

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

def load_all_price_data(from_ts, to_ts):
    # check if data is already saved
    if not glob.glob('ETH-USD_prices*'):
        return get_prices(from_ts, to_ts)
    
    ints, df_saved = load_saved_price_data()
    df_saved = df_saved[(from_ts <= df_saved.time) & (df_saved.time <= to_ts)]

    # intervals `ints` do not overlap and are sorted
    # find uncomputed timeframe between intervals
    df = pd.DataFrame()

    # gather data even further back, if possible
    if from_ts < ints[0][0]:
        df = get_prices(from_ts, ints[0][0])

    for i in range(len(ints) - 1):
        from_, to_ = ints[i][1] + 1, ints[i+1][0] - 1
        df_ =  get_prices(from_, to_)
        df = pd.concat([df, df_])

    # new data
    from_, to_ = ints[-1][1] + 1, int(time.time())
    df_ =  get_prices(from_, to_)
    df = pd.concat([df, df_])

    # concat new data with old data
    df = pd.concat([df_saved, df])
    df.sort_values(by=['time'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df

def load_saved_price_data():
    # example file name:
    # ETH-USD_prices_1587033516_1587724716.csv
    df_final = pd.DataFrame()
    ts_ints = []
    patt = 'ETH-USD_prices_(\d+)_(\d+).csv'
    # file name to ts interval
    files = {}
    for fname in glob.glob('ETH-USD_prices_*'):
        m = re.match(patt, fname)
        from_ts = int(m.group(1))
        to_ts = int(m.group(2))
        files[fname] = (from_ts, to_ts)
    # we don't want duplicate data;
    # i.e. we don't want the timestamp
    # intervals to overlap
    ts_ints = merge_intervals(list(files.values()))
    # for each file, only load relevant subset
    # that aligns with merged intervals
    start, end = zip(*ts_ints)
    for fname, (from_ts, to_ts) in files.items():
        i = bisect.bisect(start, from_ts) - 1
        target_from, target_to = ts_ints[i]
        df = pd.read_csv(fname)
        df = df[(target_from <= df['time']) & (df['time'] <= min(target_to, to_ts))]
        df_final = pd.concat([df_final, df])
        if 'Unnamed: 0' in df_final.columns:
            df_final.drop(columns=['Unnamed: 0'], inplace=True)
            df_final.sort_values(by=['time'], inplace=True)
            df_final.reset_index(inplace=True, drop=True)
    print('Loaded data for the following time ranges:')
    print(ts_ints)
    return ts_ints, df_final

def clean_up_price_files():
    # clean up duplicate data
    old_files = glob.glob('ETH-USD_prices_*')
    if len(old_files) <= 1:
        print('No files removed.')
        return
    ts_ints, df = load_saved_price_data()
    for from_ts, to_ts in ts_ints:
        df_ = df[(from_ts <= df.time) & (df.time <= to_ts)]
        filename = 'ETH-USD_prices_{}_{}.csv'.format(int(from_ts), int(to_ts))
        df_.to_csv(filename)
        print('Created new file: ', filename)
    #delete old files
    for fname in old_files:
        os.remove(fname)
        print('Removed:', fname)

def merge_intervals(arr):
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

def get_prices(from_ts, to_ts, fsym='ETH', tsym='USD', limit=2000, save=True):

    def _get_prices(from_ts, to_ts, fsym, tsym, limit):
        print('Querying dates {} to {}.'.format(
            convert_unixtime(from_ts),
            convert_unixtime(to_ts)
        ))
        # NOTE: they don't allow queries for prices older than a week
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
            fsym=fsym, tsym=tsym, to_ts=to_ts, limit=limit), headers=CRYPTO_COMPARE_API_HEADER)
        
        d = json.loads(r.content.decode('utf-8'))
        
        if d.get('Response') == 'Error':
            print(d['Message'])
            return pd.DataFrame(res)

        for x in d['Data']['Data']:
            if x['time'] < from_ts:
                break
            for k in res.keys():
                res[k].append(x[k])
        else:
            # need to keep querying to get all data in range
            df_ = _get_prices(from_ts, min(res['time']) - 1, fsym, tsym, limit)
            df = pd.concat([pd.DataFrame(res), df_])
            df.reset_index(inplace=True, drop=True)
            return df
        return pd.DataFrame(res)
       
    df = _get_prices(from_ts, to_ts, fsym, tsym, limit)
    if df.empty:
        print('No new prices gathered.')
    if save:
        df.to_csv('ETH-USD_prices_{}_{}.csv'.format(int(from_ts), int(to_ts)))

    return df


if __name__ == "__main__":
    # testing
    '''
    web3, contract = connect()
    res = approx_earliest_eth_block(1)
    #print(res['number'])
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
    #print(res)
    #res = load_all_price_response_data()
    #print(res)
    
    ts = int(time.time())
    from_ts = ts - 60*60*24*8
    to_ts = ts

    df = get_prices(from_ts, to_ts)
    to_ts = int(time.time())
    # seconds in minute, minutes in hour, hours in day, num days
    from_ts = to_ts - 60*60*24*14
    df = load_all_price_data(from_ts, to_ts)
    print(df)
    '''
    clean_up_price_files()
