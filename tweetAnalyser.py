from mpi4py import MPI
import sys
import time
import json
import re

from memory_profiler import profile


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


@profile
def analyseTweetLocation(twitter_data_file, great_locations, num_cores, rank):
    tweets_per_gcity = {}
    user_tweets = {}
    with open(twitter_data_file, 'r', encoding='utf8') as file:
        line = file.readline()
        incomplete_data = {'user_id': None, 'tweet_location': None}
        user_id = None
        while line:
            try:
                line = re.findall(r'"(.*?)"', line)
                if line[0] == 'author_id':
                    user_id = line[1]
                elif line[0] == 'full_name':
                    if user_id is not None:
                        tweet_location = line[1].split(',')[0]
                        if tweet_location in great_locations:
                            tweet_gcity = great_locations[tweet_location]['gcc']
                        tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(tweet_gcity, 0) + 1
                        if user_id not in user_tweets:
                            user_tweets[user_id] = {}
                        user_tweets[user_id][tweet_gcity] = user_tweets[user_id].get(tweet_gcity, 0) + 1
                        user_id = None
                    else:
                        incomplete_data['tweet_location'] = tweet_location
            except:
                pass
            line = file.readline()
        if user_id is not None:
            incomplete_data['user_id'] = user_id

    return tweets_per_gcity, user_tweets, incomplete_data


def outputGcityTable(tweets_per_gcity):
    sorted_gcity = sorted(tweets_per_gcity.items, key=lambda x: x[1], reverse=True)
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
        user_gcity_str = ', '.join(gcity for gcity in user_tweets[user_id].keys())
        print(f'{i + 1} {user_id} {total_unique} (#{user_totals[user_id]} tweets - {user_gcity_str})')
    return


twitter_file = sys.argv[1]
sal_file = sys.argv[2]

'''
comm = MPI.COMM_WORLD
p = comm.Get_size()
rank = comm.Get_rank()
...
MPI.Finalize()
'''


if __name__ == '__main__':
    sal_file = 'sal.json'
    twitter_data_file = 'twitter-data-small.json'
    great_locations = getGreatLocations(sal_file)
    analyseTweetLocation(twitter_data_file, great_locations, 1, 0)
