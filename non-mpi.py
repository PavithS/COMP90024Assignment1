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
    suburb = tweet_location_str.split(',')[0].strip().lower()
    if (state_or_au == 'Australia' and suburb in great_locations):
        tweet_gcity = great_locations[suburb]['gcc']
        return tweet_gcity
    # location should lower case

    suburb = tweet_location_str.split(',')[0].strip().lower()
    state = state_or_au

    suburb_with_state = f'{suburb} ({state_dict[state]})'

    if suburb_with_state in great_locations:
        tweet_gcity = great_locations[suburb_with_state]['gcc']
        return tweet_gcity
    if suburb in great_locations:
        tweet_gcity = great_locations[suburb]['gcc']
        return tweet_gcity

    return tweet_gcity


# @profile
def analyseTweetLocation(twitter_data_file, great_locations, num_cores, rank):
    tweets_per_gcity = {}
    user_tweets = {}
    with open(twitter_data_file, 'r', encoding='utf8') as file:
        line = file.readline()
        incomplete_data = {'user_id': None, 'tweet_location': None}
        user_id = None
        while line:
            try:
                KEY,VALUE = 0,1
                line = re.findall(r'"(.*?)"', line)
                if line[KEY] == 'author_id':
                    user_id = line[VALUE]
                elif line[KEY] == 'full_name':
                    tweet_gcity = parse_location(
                            line[VALUE], great_locations)
                    if user_id is not None:
                        tweet_location = ''
                        if tweet_gcity is not None:
                            tweets_per_gcity[tweet_gcity] = tweets_per_gcity.get(
                                tweet_gcity, 0) + 1
                            if user_id not in user_tweets:
                                user_tweets[user_id] = {}
                            user_tweets[user_id][tweet_gcity] = user_tweets[user_id].get(
                                tweet_gcity, 0) + 1
                        user_id = None
                    else:
                        incomplete_data['tweet_location'] = tweet_location
            except:
                pass
            line = file.readline()
        if user_id is not None:
            incomplete_data['user_id'] = user_id
    print(tweets_per_gcity)

    return tweets_per_gcity, user_tweets, incomplete_data


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
    s = time.time()
    sal_file = 'sal.json'
    # twitter_data_file = '../smallTwitter.json'
    # twitter_data_file = '../smallTwitter.json'
    twitter_data_file = 'D:\\bigTwitter.json'
    
    great_locations = getGreatLocations(sal_file)
    tweets_per_gcity, user_tweets, incomplete_data = analyseTweetLocation(
        twitter_data_file, great_locations, 1, 0)
    outputGcityTable(tweets_per_gcity)
    outputMostTweetsTable(user_tweets)
    outputMostUniqueTable(user_tweets)
    e = time.time()
    print(e-s)
