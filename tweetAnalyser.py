from mpi4py import MPI
import time
import json
import re
import argparse
import heapq
from collections import defaultdict, Counter
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
        # dict1, dict2
    return result


def merge_user_tweets(dict1, dict2):
    merged_dict = {}
    for d in (dict1, dict2):
        for key, value in d.items():
            if key not in merged_dict:
                merged_dict[key] = {}
                merged_dict[key]['gcities'] = {}
                merged_dict[key]['total'] = 0
                merged_dict[key]['unique'] = 0
            for inner_key, inner_value in value.items():
                if inner_key == 'gcities':
                    for inner_inner_key, inner_inner_value in inner_value.items():
                        if inner_inner_key not in merged_dict[key][inner_key]:
                            merged_dict[key][inner_key][inner_inner_key] = 0
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
    merged_list.extend(list1)
    merged_list.extend(list2)
    return merged_list


def merge_most_tweets_list(list1, list2):
    TOTAL_INDEX = 2
    merged_list = merge_list(list1, list2)
    merged_list = heapq.nlargest(
        N, merged_list, key=lambda x: (x[TOTAL_INDEX]))
    return merged_list


def merge_most_unique_list(list1, list2):
    UNIQUE_INDEX = 1
    merged_list = merge_list(list1, list2)
    merged_list = heapq.nlargest(
        N, merged_list, key=lambda x: (x[UNIQUE_INDEX], x[2]))
    return merged_list


def update_user_tweets(user_tweets, user_id, tweet_gcity, count=1):
    if user_id not in user_tweets:
        user_tweets[user_id] = {}
        user_tweets[user_id]['gcities'] = {}
    if tweet_gcity not in user_tweets[user_id]['gcities']:
        user_tweets[user_id]['unique'] = user_tweets[user_id].get(
            'unique', 0) + 1
    user_tweets[user_id]['gcities'][tweet_gcity] = user_tweets[user_id]['gcities'].get(
        tweet_gcity, 0) + count
    user_tweets[user_id]['total'] = user_tweets[user_id].get(
        'total', 0) + count
    return user_tweets

# @profile


def update_dict(tweets_per_gcity, user_tweets, user_id, tweet_gcity, count=1):
    tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
        tweet_gcity, 0) + count
    update_user_tweets(user_tweets, user_id, tweet_gcity, count)


def analyseTweetLocation(twitter_data_file, great_locations, line_start, line_end):
    tweets_per_gcity, user_tweets = Counter(), Counter()
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
            update_dict(tweets_per_gcity, user_tweets, user_id, tweet_gcity)
        if last_flag == LAST_USER:
            tweet_gcity = comm.recv(source=comm_rank+1)
            if tweet_gcity is not None:
                update_dict(tweets_per_gcity, user_tweets,
                            user_id, tweet_gcity)
    return tweets_per_gcity, user_tweets


def read_file(twitter_data_file):
    time_start = time.time()
    user_location = Counter()
    with open(twitter_data_file, 'r', encoding='utf8') as file:
        for line in file:
            KEY, VALUE = 0, 1
            if '"full_name"' not in line and '"author_id"' not in line:
                continue
            line = re.findall(r'"(.*?)"', line)

            LOCATION_KEY, USER_ID_KEY = 'full_name', 'author_id'
            tweet_gcity_str = None
            if line[KEY] == LOCATION_KEY:
                tweet_gcity_str = line[VALUE]
                user_location.update([(user_id, tweet_gcity_str)])
            elif line[KEY] == USER_ID_KEY:
                user_id = line[VALUE]
    time_end = time.time()
    if comm_rank == 0:
        print('read file time: ', time_end - time_start)
    return user_location


def analyseTweetLocation2(local_user_location, great_locations):
    tweets_per_gcity, user_tweets = {}, {}
    for user_id, tweet_gcity_str, count in local_user_location:
        tweet_gcity = parse_location(tweet_gcity_str, great_locations)
        # what if the first line is about location
        if tweet_gcity is None:
            continue

        count = int(count)
        update_dict(tweets_per_gcity, user_tweets, user_id, tweet_gcity, count)
    return tweets_per_gcity, user_tweets


def outputGcityTable(tweets_per_gcity):
    sorted_gcity = sorted(tweets_per_gcity.items(),
                          key=lambda x: x[1], reverse=True)
    print('%-20s | %-22s' % ("Greater Capital City", "Number of Tweets Made"))
    for gcity_data in sorted_gcity:
        print("%-20s | %-22s" % (gcity_data[0], gcity_data[1]))
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
        user_id, total_unique, total_tweets = user_data
        user_gcity_str = ', '.join([str(user_tweets[user_id]['gcities'][gcity]) + gcity[1:]
                                   for gcity in user_tweets[user_id]['gcities'].keys()])
        print("%-5s | %-20s " % (i + 1, user_id), end='')
        print(
            f'| {total_unique} (#{total_tweets} tweets - {user_gcity_str})')
    return


def setup_mpi():
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    return comm, comm_rank, comm_size


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


if __name__ == '__main__':
    N = 10
    t1 = time.time()
    comm, comm_rank, comm_size = setup_mpi()

    sal_file, twitter_data_file = setup_args()
    # setup sal dictionary
    great_locations = getGreatLocations(sal_file)
    # lines_sum = comm.bcast(
    #     sum(1 for _ in open(twitter_data_file, encoding='utf-8')), root=0)
    if comm_rank == 0:
        print(f'====== {comm_size} cores are running ====== ')
    # lines_sum = 718514355

    # lines_per_core = lines_sum // comm_size
    # lines_to_end = lines_sum
    # line_to_start = lines_per_core * comm_rank
    # line_to_end = line_to_start + lines_per_core
    # if comm_rank == comm_size - 1:
    #     line_to_end = lines_to_end
    # # print('Process', comm_rank, 'will process lines', line_to_start, line_to_end)
    # tweets_per_gcity, user_tweets = analyseTweetLocation(
    #     twitter_data_file, great_locations, line_to_start, lines_to_end)

    user_location = read_file(twitter_data_file)
    user_location = list(
        map(lambda x: (x[0][0], x[0][1], int(x[1])), user_location.items()))
    # 2) split merged to each processor

    if comm_rank == 0:

        split_user_location_np_array = np.array_split(
            user_location, comm_size)

    else:
        split_user_location_np_array = None
    # 3) scatter to each processor
    # local_user_location = list(map(lambda x: (x[0], x[1]), comm.scatter(
    #     split_user_location_np_array, root=0)))
    local_user_location = comm.scatter(split_user_location_np_array, root=0)
    # print('process', comm_rank, 'finished', len(local_user_location))
    tweets_per_gcity, user_tweets = analyseTweetLocation2(
        local_user_location, great_locations)
    # print('process', comm_rank, 'finished', tweets_per_gcity)

    e11 = time.time()
    e2 = time.time()
    # merge each processor's result
    tweets_per_gcity = comm.reduce(tweets_per_gcity, root=0, op=merge_gcities)

    user_tweets = comm.reduce(user_tweets, root=0, op=merge_user_tweets)
    if comm_rank == 0:
        split_user_tweets_array = np.array_split(
            list(user_tweets.items()), comm_size)
    else:
        split_user_tweets_array = None

    # 3) scatter merged to each processor
    local_tweets_array = list(map(lambda x: (x[0], int(x[1]['unique']), int(x[1]['total'])), comm.scatter(
        split_user_tweets_array, root=0)))
    e3 = time.time()
    if comm_rank == 0:
        print('merge time: ', e3-e2, 's')

    # merge each processor's top n calculation result

    reduced_most_tweets_users = comm.reduce(heapq.nlargest(
        N, local_tweets_array, lambda x: (x[2])), root=0, op=merge_most_tweets_list)

    reduced_most_unique_users = comm.reduce(heapq.nlargest(
        N, local_tweets_array, lambda x: (x[1], x[2])), root=0, op=merge_most_unique_list)
    # print('Process', comm_rank, reduced_most_unique_users)

    if comm_rank == 0:
        e2 = time.time()
        
        outputGcityTable(tweets_per_gcity)
        outputMostTweetsTable(reduced_most_tweets_users)
        print()
        outputMostUniqueTable(user_tweets, reduced_most_unique_users)
        print('Total time: ', e2-t1, 's')
    MPI.Finalize()
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d smallTwitter.json
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d twitter-data-small.json
# mpiexec -np 4 python tweetAnalyser.py -s sal.json -d D://bigTwitter.json

# 4 cores
