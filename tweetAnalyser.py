from mpi4py import MPI
import sys
import time
import json
import re

from memory_profiler import profile

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


def merge_dicts(dict1, dict2):
    result = {}
    for key, value in dict1.items():
        result[key] = result.setdefault(key, 0) + value
    for key, value in dict2.items():
        result[key] = result.setdefault(key, 0) + value
    return result


def increase_user_tweets(user_tweets, user_id, tweet_gcity):
    if user_id not in user_tweets:
        user_tweets[user_id] = {}
    user_tweets[user_id][tweet_gcity] = user_tweets[user_id].get(
        tweet_gcity, 0) + 1
    return user_tweets

# @profile


def analyseTweetLocation(twitter_data_file, great_locations, line_start, line_end):
    tweets_per_gcity = {}
    user_tweets = {}
    with open(twitter_data_file, 'r', encoding='utf8') as file:
        user_id = None
        tweet_gcity = None
        LAST_USER = 0
        LAST_LOCATION = 1
        last_flag = LAST_LOCATION
        for line_num, line in enumerate(file):
            if line_num == line_end:
                break
            if line_start > line_num:
                continue
            KEY, VALUE = 0, 1
            line = re.findall(r'"(.*?)"', line)
            if len(line) < 2:
                continue
            if line[KEY] == 'full_name' and user_id is None:
                comm.send(parse_location(
                    line[VALUE], great_locations), dest=comm_rank-1)
                continue

            if line[KEY] == 'author_id':
                user_id = line[VALUE]
                last_flag = LAST_USER
            elif line[KEY] == 'full_name':
                last_flag = LAST_LOCATION
                tweet_gcity = parse_location(line[VALUE], great_locations)
                if tweet_gcity is not None:
                    tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
                        tweet_gcity, 0) + 1
                if user_id is not None:
                    increase_user_tweets(user_tweets, user_id, tweet_gcity)
                user_id = None
                tweet_gcity = None
        if last_flag == LAST_USER:
            tweet_gcity = comm.recv(source=comm_rank+1)
            if tweet_gcity is not None:
                tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
                    tweet_gcity, 0) + 1
                increase_user_tweets(user_tweets, user_id, tweet_gcity)
    # print('Process', comm_rank, 'finished',tweets_per_gcity)
    return tweets_per_gcity, user_tweets


def outputGcityTable(tweets_per_gcity):
    sorted_gcity = sorted(tweets_per_gcity.items(),
                          key=lambda x: x[1], reverse=True)
    print('Greater Capital City | Number of Tweets Made')
    for gcity_data in sorted_gcity:
        print(gcity_data[0], gcity_data[1])
    return


def outputMostTweetsTable(user_tweets, max_rank=10):
    sorted_users = sorted([(user_id, sum(val for val in tweet_counts.values()))
                           for user_id, tweet_counts in user_tweets.items()],
                          key=lambda x: x[1], reverse=True)
    print('Rank | Author Id | Number of Tweets Made')
    for i, user_data in enumerate(sorted_users[:max_rank]):
        print(i+1, user_data[0], user_data[1])
    return


def outputMostUniqueTable(user_tweets, max_rank=10):
    sorted_users = sorted([(user_id, len(tweet_counts.keys()))
                           for user_id, tweet_counts in user_tweets.items()],
                          key=lambda x: x[1], reverse=True)
    user_totals = {user_id: sum(val for val in tweet_counts.values())
                   for user_id, tweet_counts in user_tweets.items()}
    print('Rank | Author Id | Number of Unique City Locations and #Tweets')
    for i, user_data in enumerate(sorted_users[:max_rank]):
        user_id, total_unique = user_data
        user_gcity_str = ', '.join(
            gcity for gcity in user_tweets[user_id].keys())
        print(
            f'{i + 1} {user_id} {total_unique} (#{user_totals[user_id]} tweets - {user_gcity_str})')
    return


# twitter_file = sys.argv[1]
# sal_file = sys.argv[2]

'''
comm = MPI.COMM_WORLD
p = comm.Get_size()
rank = comm.Get_rank()
...
MPI.Finalize()
'''

if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    sal_file = 'sal.json'
    # twitter_data_file = 'twitter-data-small.json'
    # twitter_data_file = '../smallTwitter.json'
    twitter_data_file = 'D:\\bigTwitter.json'
    great_locations = getGreatLocations(sal_file)
    # calculating number of lines of data to be processed, line to start, line to end
    lines_sum = comm.bcast(
        sum(1 for _ in open(twitter_data_file, encoding='utf-8')), root=0)
    # lines_sum = 718514355

    lines_per_core = lines_sum // comm_size
    # the total number of line to be read by the processor
    lines_to_end = lines_sum   # ignore first line
    # the index of the first line to be processed by the processor
    line_to_start = lines_per_core * comm_rank  # ignore first line
    # the index of the last line to be processed by the processor
    line_to_end = line_to_start + lines_per_core
    if comm_rank == comm_size - 1:  # last core to finish all remaining lines
        line_to_end = lines_to_end
    print('Process', comm_rank, 'will process lines', line_to_start, line_to_end)
    s = time.time()
    tweets_per_gcity, user_tweets = analyseTweetLocation(
        twitter_data_file, great_locations, line_to_start, lines_to_end)
    reduced_hash_tag_count = comm.reduce(
        tweets_per_gcity, root=0, op=merge_dicts)
    e = time.time()
    print('Process', comm_rank, 'finished in', e-s)

    if comm_rank == 0:

        print('Greater Capital City | Number of Tweets Made')
        print(tweets_per_gcity)

    MPI.Finalize()
# mpiexec -np 8 python tweetAnalyser.py
