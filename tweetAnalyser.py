from mpi4py import MPI
import time
import json
import re
import argparse
import heapq
from collections import defaultdict
import numpy as np
# Picton


def getGreatLocations(sal_file):

    great_locations = {'gcc_list': []}
    with open(sal_file, encoding='utf8') as location_file:
        location_data = json.load(location_file)
        for place_name, place_data in location_data.items():
            if place_data['gcc'][1] == 'g' or place_data['gcc'] == '8acte':
                great_locations[place_name] = place_data
                if place_data['gcc'] not in great_locations['gcc_list']:
                    great_locations['gcc_list'].append(place_data['gcc'])

    return great_locations


def parse_location(tweet_location_str: str, great_locations):
    state_dict = {
        'New South Wales': 'nsw',
        'Queensland': 'qld',
        'South Australia': 'sa',
        'Tasmania': 'tas.',
        'Victoria': 'vic.',
        'Western Australia': 'wa',
        'Northern Territory': 'nt',
        'Australian Capital Territory': 'act',
    }
    tweet_gcity = None

    if (',' not in tweet_location_str):
        return None

    state_or_au = tweet_location_str.split(',')[1].strip()
    # location should lower case
    suburb = tweet_location_str.split(',')[0].strip().lower()
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


def merge_gcities(dict1, dict2):
    result = {}
    for key, value in dict1.items():
        result[key] = result.setdefault(key, 0) + value
    for key, value in dict2.items():
        result[key] = result.setdefault(key, 0) + value
    return result


def merge_user_tweets(dict1, dict2):
    merged_dict = defaultdict(lambda: defaultdict(int))

    for d in (dict1, dict2):
        for key, value in d.items():
            for inner_key, inner_value in value.items():
                if key == 'gcities':
                    for inner_inner_key, inner_inner_value in inner_value.items():
                        merged_dict[key][inner_key][inner_inner_key] += inner_inner_value
                if inner_key == 'total':
                    merged_dict[key][inner_key] += inner_value
    for key, value in merged_dict.items():
        for inner_key, inner_value in value.items():
            if inner_key == 'gcities':
                merged_dict[key]['unique'] = len(inner_value.keys())
    return dict(merged_dict)


def merge_list(list1, list2):
    merged_list = []
    for i in list1:
        merged_list.append(i)
    for i in list2:
        merged_list.append(i)
    merged_list = heapq.nlargest(10, merged_list, key=lambda x: (x[1], x[2]))

    return merged_list


def update_user_tweets(user_tweets, user_id, tweet_gcity):
    if user_id not in user_tweets:
        user_tweets[user_id] = {}
        user_tweets[user_id]['gcities'] = {}
    if tweet_gcity not in user_tweets[user_id]['gcities']:
        user_tweets[user_id]['unique'] = user_tweets[user_id].get(
            'unique', 0) + 1
    user_tweets[user_id]['gcities'][tweet_gcity] = user_tweets[user_id]['gcities'].get(
        tweet_gcity, 0) + 1
    user_tweets[user_id]['total'] = user_tweets[user_id].get(
        'total', 0) + 1
    return user_tweets

# @profile


def analyseTweetLocation(twitter_data_file, great_locations, line_start, line_end):
    tweets_per_gcity, user_tweets = {}, {}
    with open(twitter_data_file, 'r', encoding='utf8') as file:
        user_id, tweet_gcity = None, None
        LAST_USER, LAST_LOCATION = 0, 1
        last_flag = LAST_LOCATION
        for line_num, line in enumerate(file):
            if line_start > line_num:
                continue
            if line_num == line_end:
                break
            KEY, VALUE = 0, 1
            if '"full_name"' not in line and '"author_id"' not in line:
                continue
            line = re.findall(r'"(.*?)"', line)
            if len(line) < 2:
                continue
            LOCATION_KEY, USER_ID_KEY = 'full_name', 'author_id'
            tweet_gcity_str = None
            if line[KEY] == LOCATION_KEY:
                tweet_gcity_str = line[VALUE]
                last_flag = LAST_LOCATION

            elif line[KEY] == USER_ID_KEY:
                user_id = line[VALUE]
                last_flag = LAST_USER
                continue
            else:
                continue

            tweet_gcity = parse_location(tweet_gcity_str, great_locations)
            # what if the first line is about location
            if user_id is None:
                comm.send(tweet_gcity, dest=comm_rank-1)
                continue
            if tweet_gcity is None:
                user_id = None
                continue

            tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
                tweet_gcity, 0) + 1
            update_user_tweets(user_tweets, user_id, tweet_gcity)

        if last_flag == LAST_USER:
            tweet_gcity = comm.recv(source=comm_rank+1)
            if tweet_gcity is not None:
                tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
                    tweet_gcity, 0) + 1
                update_user_tweets(user_tweets, user_id, tweet_gcity)
    # print('Process', comm_rank, 'finished',tweets_per_gcity)
    # print(user_tweets)
    return tweets_per_gcity, user_tweets


def outputGcityTable(tweets_per_gcity):
    sorted_gcity = sorted(tweets_per_gcity.items(),
                          key=lambda x: x[1], reverse=True)
    print('%-20s | %-22s' % ("Greater Capital City", "Number of Tweets Made"))
    for gcity_data in sorted_gcity:
        print("%-20s | %-22s" % (gcity_data[0], gcity_data[1]))
    return


def outputMostTweetsTable(user_tweets, max_rank=10):
    sorted_users = sorted([(user_id, sum(val for val in tweet_counts.values()))
                           for user_id, tweet_counts in user_tweets.items()],
                          key=lambda x: x[1], reverse=True)
    print('%-5s | %-20s | %-22s' %
          ("Rank", "Author Id", "Number of Tweets Made"))

    for i, user_data in enumerate(sorted_users[:max_rank]):
        print('%-5s | %-20s | %-22s' % (i+1, user_data[0], user_data[1]))
    return


def outputMostUniqueTable(user_tweets, max_rank=10):
    user_totals = {user_id: tweet_counts['total']
                   for user_id, tweet_counts in user_tweets.items()}
    sorted_users = sorted([(user_id, tweet_counts['unique'])
                           for user_id, tweet_counts in user_tweets.items()],
                          key=lambda x: (x[1], user_totals[x[0]]), reverse=True)
    print(sorted_users[:20])

    print('%-5s | %-20s | %-44s' %
          ("Rank", "Author Id", " Number of Unique City Locations and #Tweets"))
    for i, user_data in enumerate(sorted_users[:max_rank]):
        user_id, total_unique = user_data
        user_gcity_str = ', '.join(
            gcity for gcity in user_tweets[user_id]['gcities'].keys())
        print("%-5s | %-20s " % (i + 1, user_id), end='')
        print(
            f'| {total_unique} (#{user_totals[user_id]} tweets - {user_gcity_str})')
    return


'''
comm = MPI.COMM_WORLD
p = comm.Get_size()
rank = comm.Get_rank()
...
MPI.Finalize()
'''

if __name__ == '__main__':
    N = 10
    s = time.time()
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='')
    # Required sal file
    parser.add_argument('-s', type=str, help='Require sal.json')
    # Required geo data path
    parser.add_argument('-d', type=str, help='Require twitter data file')
    args = parser.parse_args()

    sal_file = args.s
    twitter_data_file = args.d

    great_locations = getGreatLocations(sal_file)
    lines_sum = comm.bcast(
        sum(1 for _ in open(twitter_data_file, encoding='utf-8')), root=0)
    e1 = time.time()
    if comm_rank == 0:
        print('Read data: ', e1-s, 's')
    # lines_sum = 718514355

    lines_per_core = lines_sum // comm_size
    lines_to_end = lines_sum
    line_to_start = lines_per_core * comm_rank
    line_to_end = line_to_start + lines_per_core
    if comm_rank == comm_size - 1:
        line_to_end = lines_to_end
    # print('Process', comm_rank, 'will process lines', line_to_start, line_to_end)
    tweets_per_gcity, user_tweets = analyseTweetLocation(
        twitter_data_file, great_locations, line_to_start, lines_to_end)
    e11=time.time()
    if comm_rank == 0:
        print('process', comm_rank, 'finished', e11-e1, 's')
    e2 = time.time()

    comm.reduce(tweets_per_gcity, root=0, op=merge_gcities)

    comm.reduce(user_tweets, root=0, op=merge_user_tweets)
    if comm_rank == 0:
        split_user_tweets_array = np.array_split(
            list(user_tweets.items()), comm_size)
        # print('Split user tweets array')
        # print(split_user_tweets_array)

    else:
        split_user_tweets_array = None

    # 3) scatter merged to each processor
    local_tweets_array = list(map(lambda x: (x[0], int(x[1]['unique']), int(x[1]['total'])), comm.scatter(
        split_user_tweets_array, root=0)))
    e3 = time.time()
    if comm_rank == 0:
        print('merge time: ', e3-e2, 's')

    # print('local_hash_tag')
    # print(local_hash_tag)

    # 4) merge each processor's top n calculation result
    reduced_user_tweets_array_count = comm.reduce(heapq.nlargest(
        N, local_tweets_array, lambda x: (x[1], x[2])), root=0, op=merge_list)

    print('Process', comm_rank, 'finished', reduced_user_tweets_array_count)
    e2 = time.time()

    if comm_rank == 0:
        e3 = time.time()
        # outputMostUniqueTable(user_tweets)
        # print('Process', comm_rank, 'finished', user_tweets)
        # print('Process', comm_rank, 'finished users:', len(user_tweets))
        print()
        e4 = time.time()
        print('Reduce time: ', e3-e4, 's')
        print('Total time: ', e3-s, 's')
        pass
    MPI.Finalize()
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d smallTwitter.json
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d twitter-data-small.json
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d D://bigTwitter.json

# 4 cores