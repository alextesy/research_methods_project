import gzip
import re
import pickle
import time
import simplejson
import zipfile
from io import BytesIO
from collections import OrderedDict, defaultdict


def lookup(json, k):
    # return json[k]
    if '.' in k:
        # jpath path
        ks = k.split('.')
        v = json
        for k in ks: v = v.get(k, {})
        return v or ""
    return json.get(k, "")


def get_echo_users_tweets(t1):
    file_count = 0
    users = {}
    users_links_until_t1 = defaultdict(lambda: defaultdict(int))
    all_users_links = defaultdict(lambda: defaultdict(int))
    hashtags = defaultdict(list)
    with zipfile.ZipFile("/data/home/kremians/hashtags/recent_hisroty.zip",
                         "r") as zfile:
        for name in zfile.namelist():
            # We have a zip within a zip
            if re.search('\.gz$', name) != None:
                zfiledata = BytesIO(zfile.read(name))
                with gzip.open(zfiledata) as zf:
                    for line in zf:
                        pass
                        json = simplejson.loads(line)
                        user = lookup(json, 'user.screen_name')
                        created_at = lookup(json, 'created_at')
                        mentions = lookup(json, 'entities.user_mentions')
                        current_hashtags_list = lookup(json, 'entities.hashtags')

                        '''if user not in users_links_until_t1.keys():
              users_links_until_t1[user] = {}
            if user not in all_users_links.keys():
              all_users_links[user] = {}'''
                        for mention in mentions:
                            mentioned_user = mention['screen_name']
                            if not mentioned_user == user:
                                # add a link only if it was created until t=t1
                                if created_at <= t1:
                                    users_links_until_t1[user][mentioned_user] += 1
                                all_users_links[user][mentioned_user] += 1


                        for current_hashtag in current_hashtags_list:
                            hashtag = current_hashtag['text']
                            if hashtag not in hashtags.keys():
                                hashtags[hashtag] = []
                            tuple_to_add = (user, created_at)
                            hashtags[hashtag].append(tuple_to_add)

                            # if tuple_to_add not in hashtags[hashtag]:
                            #  hashtags[hashtag].append(tuple_to_add)

                        # users_dict[lookup(json, 'user.id')] += tweet_text
                zf.close()
                print('User: ' +user + ' is finished')
                file_count += 1
                if (file_count%500 == 0):
                    print('Saving temporary copy for count: ' + str(file_count))
                    temporary_save(hashtags,users_links_until_t1,all_users_links)

    zfile.close()
    for key, value in hashtags.items():
        hashtags[key] = sorted(value, key=lambda tup: time.mktime(time.strptime(tup[1], '%a %b %d %H:%M:%S +0000 %Y')))

    ordered_hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: len(x[1]), reverse=True))
    save_dicts(ordered_hashtags, users_links_until_t1, all_users_links)
    return users_links_until_t1, all_users_links, ordered_hashtags

def temporary_save(hashtags,users_links_until_t1,all_users_links):
    for key, value in hashtags.items():
        hashtags[key] = sorted(value, key=lambda tup: time.mktime(time.strptime(tup[1], '%a %b %d %H:%M:%S +0000 %Y')))
    ordered_hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: len(x[1]), reverse=True))
    save_dicts(ordered_hashtags, users_links_until_t1, all_users_links)


def save_dicts(ordered, users_t1, all_users):
    d1 = default_to_regular(ordered)
    d2 = default_to_regular(users_t1)
    d3 = default_to_regular(all_users)

    with open('ordered.pkl', 'wb') as f:
        pickle.dump(d1, f)
    with open('users_t1.pkl', 'wb') as f:
        pickle.dump(d2, f)
    with open('all_users.pkl', 'wb') as f:
        pickle.dump(d3, f)


def default_to_regular(d):
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d



def before_t1(t1,hashtime):
    hashtime = time.strptime(hashtime, '%a %b %d %H:%M:%S +0000 %Y')
    t1 = time.strptime(t1, '%a %b %d %H:%M:%S +0000 %Y')
    return  hashtime<=t1

def ordinal_time_hashtag_probability(all_users_links, hashtags, hashtag, t1, k):
    general_k_exposed_users = 0
    users_used_hashtag = 0
    #t1 = time.strptime(t1, '%a %b %d %H:%M:%S +0000 %Y')
    relevant_users = [user[0] for user in hashtags[hashtag]]
    all_users_links = dict((rel_user, all_users_links[rel_user]) for rel_user in relevant_users if rel_user in all_users_links)
    for user, mentions in all_users_links.items():
        # make sure that user did not use the hashtag before t1
        user_use_of_h_before_t1 = []
        for hashtag_use in hashtags[hashtag]:
            if(before_t1(t1,hashtag_use[1]) and hashtag_use[0] == user):
                user_use_of_h_before_t1.append(hashtag_use)

        if len(user_use_of_h_before_t1)> 0:
            continue
        else:
            current_exposure_count = 0
            extra_exposures = []
            for user_mentioned in mentions:
                relevant_hashtag_uses = [hashtag_use for hashtag_use in hashtags[hashtag] if before_t1(t1,hashtag_use[1])
                                         and hashtag_use[
                                             0] == user_mentioned]  # consider hashtags that happened until t=t1
                if len(relevant_hashtag_uses) > 0:  # user is exposed to another (mentioned) user that used hashtag h.
                    current_exposure_count += 1
                # check for the next exposures (after t=t1)
                extra_exposures += [hashtag_use for hashtag_use in hashtags[hashtag] if not before_t1(t1,hashtag_use[1])
                                    and hashtag_use[0] == user_mentioned]
            if current_exposure_count == k:
                general_k_exposed_users += 1
                # check if the user used h after t=t1
                user_use_of_h_after_t1 = [hashtag_use for hashtag_use in hashtags[hashtag] if not before_t1(t1,hashtag_use[1])
                                          and hashtag_use[0] == user]
                # sort the exposures after t=t1 and current user's use of h by time used.
                extra_exposures = sorted(extra_exposures, key=lambda tup: time.mktime(time.strptime(tup[1], '%a %b %d %H:%M:%S +0000 %Y')))
                user_use_of_h_after_t1 = sorted(user_use_of_h_after_t1, key=lambda tup: time.mktime(time.strptime(tup[1], '%a %b %d %H:%M:%S +0000 %Y')))
                # compare the minimum time to check if current user used h before the k+1 exposure.
                if len(user_use_of_h_after_t1) > 0:
                    if (len(
                            extra_exposures) == 0):  # no extra exposures, meaning the current user did use before k+1 exposures
                        users_used_hashtag += 1
                    elif before_t1(extra_exposures[0][1] , user_use_of_h_after_t1[0][1]):
                        users_used_hashtag += 1
    if general_k_exposed_users > 0:
        hashtag_probability = users_used_hashtag / general_k_exposed_users
        return hashtag_probability
    else:
        return -1





time_pairs = [('Tue Nov 5 12:00:00 +0000 2016', 'Mon Nov 16 12:00:00 +0000 2016'),
              ('Thu Dec 1 12:00:00 +0000 2016', 'Sat Dec 3 12:00:00 +0000 2016')]

def read_dicts():
    with open('ordered.pkl', 'rb') as f:
        ordered = pickle.load(f)
    with open('users_t1.pkl', 'rb') as f:
        users_t1 = pickle.load(f)
    with open('all_users.pkl', 'rb') as f:
        all_users = pickle.load(f)
    return ordered,users_t1,all_users

def main():
    for times in time_pairs:
        t1 = times[0]
        t2 = times[1]
        # t1 = 'Sat Dec 3 19:45:21 +0000 2016'
        # t2 = 'Sat Dec 31 19:45:21 +0000 2016'

        mention_threshold = 3
        #users_links, all_users_links, hashtags = get_echo_users_tweets(t1)
        hashtags, users_links, all_users_links = read_dicts()

        #top_500_hashtags = dict(list(hashtags.items())[:3])
        print('t1: ' + t1)
        print('t2: ' + t2)
        all_k = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        snapshot_probabilities = {}
        ordinal_time_probabilities = {}
        with open('Antisemite_hashtags.txt','r',encoding='utf-8-sig') as f:
            antiesmite_hashtags = f.readlines()

        antiesmite_hashtags =[tag.strip().replace('#','') for tag in antiesmite_hashtags]
        for hashtag in antiesmite_hashtags:
            print('Proccessing:' +hashtag)
            if hashtag not in hashtags:
                print('Not found:' + hashtag)
                continue
            for k in all_k:
                probability = ordinal_time_hashtag_probability(all_users_links, hashtags, hashtag, t1, k)
                if hashtag not in ordinal_time_probabilities.keys():
                    ordinal_time_probabilities[hashtag] = []
                print(str(k)+' ' + str(probability))
                ordinal_time_probabilities[hashtag].append((k, probability))
        print('**** SNAP-SHOT ******')
        for key in snapshot_probabilities.keys():
            print(key)
            values = snapshot_probabilities[key]
            for value in values:
                print(str(value[0]) + ',' + str(value[1]))
        print('**** ORDINAL TIME ******')
        for key in ordinal_time_probabilities.keys():
            print(key)
            values = ordinal_time_probabilities[key]
            for value in values:
                print(str(value[0]) + ',' + str(value[1]))
        with open('prob_dict.pkl','wb') as f:
            pickle.dump(ordinal_time_probabilities,f)
        print("done.")


main()

print('start')
