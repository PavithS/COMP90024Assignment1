# coding=utf-8

from mpi4py import MPI
import time

from collections import defaultdict, Counter
import numpy as np
from mpi4py import MPI
import os
import io
import time
import sys
import re
import json
import heapq
import argparse

# NOTE : The code works only in Linux not Windows

sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding='utf8')


def outputGcityTable(tweets_per_gcity):
    sorted_gcity = sorted(tweets_per_gcity.items(),
                          key=lambda x: x[1], reverse=True)
    great_city_dict = {
     '2gmel':'Greater Melbourne',               
    '1gsyd':'Greater Sydney',                     
    '3gbri':'Greater Brisbane',                       
'5gper':'Greater Perth',                           
'4gade':'Greater Adelaide',                        
'8acte':'Greater Canberra',                        
'6ghob':'Greater Hobart',                           
'7gdar':'Greater Darwin',                           
'9oter':'Other Territory',        
    }
    
    print('%-25s | %-22s' % ("Greater Capital City", "Number of Tweets Made"))
    for gcity_data in sorted_gcity:
        if gcity_data[0] in great_city_dict:
            left =f'{gcity_data[0]}({great_city_dict[gcity_data[0]]})' 
            print("%-25s | %-22s" % (left, gcity_data[1]))
    return


def outputMostTweetsTable(user_tweets, max_rank=10):

    print('%-5s | %-20s | %-22s' %
          ("Rank", "Author Id", "Number of Tweets Made"))
    USER_ID_KEY, TOTAL_TWEETS_KEY = 0, 2
    for i, user_data in enumerate(user_tweets[:max_rank]):
        print('%-5s | %-20s | %-22s' %
              (i+1, user_data[USER_ID_KEY], user_data[TOTAL_TWEETS_KEY]))
    return


def outputMostUniqueTable(user_tweets, most_unique_users, max_rank=10):

    print('%-5s | %-20s | %-44s' %
          ("Rank", "Author Id", " Number of Unique City Locations and #Tweets"))
    for i, user_data in enumerate(most_unique_users[:max_rank]):
        user_id, total_unique, _ ,total_tweets = user_data
        user_gcity_str = ', '.join([str(user_tweets[user_id]['gcities'][gcity]) + gcity[1:]
                                   for gcity in user_tweets[user_id]['gcities'].keys()])
        print("%-5s | %-20s " % (i + 1, user_id), end='')
        print(
            f'| {total_unique} (#{total_tweets} tweets - {user_gcity_str})')
    return


def getGreatLocations(sal_file):

    great_locations = {'gcc_list': []}
    with open(sal_file, encoding='utf8') as location_file:
        location_data = json.load(location_file)
        for place_name, place_data in location_data.items():
            if place_data['gcc'][1] == 'g' or place_data['gcc'] == '8acte'or place_data['gcc'] == '9oter':
                great_locations[place_name] = place_data
                if place_data['gcc'] not in great_locations['gcc_list']:
                    great_locations['gcc_list'].append(place_data['gcc'])

    return great_locations


def handle_buffer(buff, last_line):
    str_data = buff
    lines = str_data.split("\n")
    lines = list(map(lambda x: x.strip(), lines))
    if last_line:
        lines[0] = last_line + lines[0]
    if lines[-1]=='':
        last_line=None
    else:
        last_line = lines[-1]
        lines = lines[:-1]
    return lines, last_line


def read_big_file(filename, start, end, user_location):
    with open(filename, "r", encoding='utf-8', errors="ignore") as f:

        f.seek(start)
        buff_size = 1024*1024
        first_buff = ""
        if comm.Get_rank() != 0:
            while True:
                tmp = f.read(1)
                first_buff += tmp
                if tmp == '\n':
                    break
            # comm.send(first_buff.strip(), dest=rank-1)
        lines, last_line =[], None
        user_id, tweet_gcity_str = None, None
        while f.tell() < end:
            str_buff = f.read(min(buff_size, end - f.tell()))
            lines, last_line = handle_buffer(str_buff, last_line)
      
            for line in lines:
                if '"full_name"' not in line and '"author_id"' not in line:
                    continue
                line = re.findall(r'"(.*?)"', line)
                LOCATION_KEY, USER_ID_KEY = 'full_name', 'author_id'
                KEY, VALUE = 0, 1
                if line[KEY] == LOCATION_KEY and user_id is not None:
                    tweet_gcity_str = line[VALUE]
                    user_location.update([(user_id, tweet_gcity_str)])
                elif line[KEY] == USER_ID_KEY:
                    user_id = line[VALUE]
            if rank != size-1:
                pass
                # t = comm.recv(source=rank+1)
                # last_line_2 = last_line + t
        return user_location


def parse_location(tweet_location_str: str, great_locations):
    state_dict = {
        'new south wales': 'nsw',
        'queensland': 'qld',
        'south australia': 'sa',
        'tasmania': 'tas.',
        'victoria': 'vic.',
        'western australia': 'wa',
        'northern territory': 'nt',
        'australian capital territory': 'act',
    }
    great_city_list=['melbourne', 'sydney', 'brisbane', 'perth', 'adelaide', 'hobart', 'canberra', 'darwin']
    tweet_gcity = None
    tweet_location_str=tweet_location_str.lower()
    # 
    if (',' not in tweet_location_str):
        if tweet_location_str  in great_locations:
            tweet_gcity = great_locations[tweet_location_str]['gcc']
        return tweet_gcity
    state_or_au = tweet_location_str.split(',')[1].strip()
    
    if state_or_au in great_city_list:
        if state_or_au in great_city_list:
            tweet_gcity = great_locations[state_or_au ]['gcc']
            return tweet_gcity
        
    # location should lower case
    suburb = tweet_location_str.split(',')[0].strip() 
    state = state_or_au
    if state_or_au in state_dict:
        suburb_with_state = f'{suburb} ({state_dict[state]})'
        if suburb_with_state in great_locations:
            tweet_gcity = great_locations[suburb_with_state]['gcc']
            return tweet_gcity
    if suburb in great_locations:
        tweet_gcity = great_locations[suburb]['gcc']
        return tweet_gcity

    return tweet_gcity


def mege_counter(l1, l2):
    l = Counter(l1)+Counter(l2)
    return l

def update_dict(tweets_per_gcity, user_tweets, user_id, tweet_gcity, count=1):
    if tweet_gcity is not None:
        tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
            tweet_gcity, 0) + count
    update_user_tweets(user_tweets, user_id, tweet_gcity, count)


def update_user_tweets(user_tweets, user_id, tweet_gcity, count=1):
    if user_id not in user_tweets:
        user_tweets[user_id] = {}
        user_tweets[user_id]['gcities'] = {}
        user_tweets[user_id]['unique'] =  0
        user_tweets[user_id]['gcity_total'] =0
        user_tweets[user_id]['total'] =0
    user_tweets[user_id]['total'] = user_tweets[user_id].get('total', 0) + count
    if tweet_gcity is None:
        return user_tweets
    if tweet_gcity not in user_tweets[user_id]['gcities']:
        user_tweets[user_id]['unique'] = user_tweets[user_id].get(
            'unique', 0) + 1
    user_tweets[user_id]['gcities'][tweet_gcity] = user_tweets[user_id]['gcities'].get(
        tweet_gcity, 0) + count
    user_tweets[user_id]['gcity_total'] = user_tweets[user_id].get(
        'gcity_total', 0) + count
    return user_tweets


def analyseTweetLocation(local_user_location, great_locations):
    tweets_per_gcity, user_tweets = {}, {}
    for user_id, tweet_gcity_str, count in local_user_location:
        tweet_gcity = parse_location(tweet_gcity_str, great_locations)
        # what if the first line is about location
        if tweet_gcity is None:
            pass
            # continue
        count = int(count)
        update_dict(tweets_per_gcity, user_tweets, user_id, tweet_gcity, count)
    return tweets_per_gcity, user_tweets


def setup_args():
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='')
    # Required sal file
    parser.add_argument('-s', type=str, help='Require sal.json')
    # Required geo data path
    parser.add_argument('-d', type=str, help='Require twitter data file')
    args = parser.parse_args()
    sal_file = args.s
    twitter_data_file = args.d
    return sal_file, twitter_data_file


if __name__ == "__main__":
    test_location={}
    N = 10
    buff={}
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    comm.Barrier()
    t1 = time.time()

    sal_file, twitter_data_file = setup_args()

    great_locations = getGreatLocations(sal_file)

    filename = twitter_data_file

    filesize = os.path.getsize(filename)

    # get chunck size
    blocksize = filesize // size
    start = rank * blocksize
    end = start + blocksize
    if rank == size - 1:
        end = filesize
    user_location = Counter()
    user_location = read_big_file(filename, start, end, user_location)
    comm.Barrier()
    all_user_location = comm.gather(user_location, root=0)
    if rank == 0:
        user_location = Counter()
        for i in all_user_location:
            user_location.update(i)
        user_location = list(
            map(lambda x: (x[0][0], x[0][1], int(x[1])), user_location.items()))
        tweets_per_gcity, user_tweets = analyseTweetLocation(
            user_location, great_locations)
        local_tweets_array = list(map(lambda x: (
            x[0], int(x[1]['unique']), int(x[1]['total']), int(x[1]['gcity_total'])), list(user_tweets.items())))
        reduced_most_tweets_users = heapq.nlargest(
            N, local_tweets_array, lambda x: (x[2]))
        reduced_most_unique_users = heapq.nlargest(
            N, local_tweets_array, lambda x: (x[1], x[3]))
        print("Task 1 output:")
        outputMostTweetsTable(reduced_most_tweets_users)
        print()
        print("Task 2 output:")

        outputGcityTable(tweets_per_gcity)
        print()
        print("Task 3 output:")
        outputMostUniqueTable(user_tweets, reduced_most_unique_users)
        t2 = time.time()
        print("time:", t2-t1,'s')
    MPI.Finalize() 
# mpiexec -np 8 python tweetAnalyser.py -s sal.json -d smallTwitter.json

# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d /mnt/d/bigTwitter.json
