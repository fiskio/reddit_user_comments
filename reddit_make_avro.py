import json
import os
import operator
import time
import bz2

import itertools as it

base_path = os.getcwd()
split_files_path = os.path.join(base_path, '1e6')
avro_path = os.path.join(base_path, 'avro')

input_files = sorted([os.path.join(split_files_path, f) for f in next(os.walk(split_files_path))[2]])
print('# input files:', len([x for x in next(os.walk(split_files_path))[2]]))

# These users have loads of messages, but with very little content...skip 'em!
# Also any username with the string 'bot' in it is excluded, sorry SpamBot...
oulier_users = ['MTGCardFetcher', 'AutoModerator', '[deleted]', 'TweetPoster', 'imgurtranscriber', 'TotesMessenger', 'autotldr']


# What does a raw reddit message record contain?
def show_keys(input_file):
    with bz2.open(input_file, 'rt') as in_file:
        for line in it.islice(in_file, 1):
            line = json.loads(line)
            for k,v in line.items():
                print('%s -> %s' % (k, v))

# show_keys(input_files[0])

"""Given an input file, read at most `n_records` and return a list of (uid, msg_lst),
   with users having at least `min_msg` messages and if their username wasn't blacklisted,
   sorted in descending order of messages.

   Only a few record fields are kept:
       `created_utc`
       `author`
       `subreddit`
       `body`
"""
def group_by_author(input_file, n_records=None, min_msg=100, blacklist=[]):
    with bz2.open(input_file, 'rt') as in_file:
        records = [json.loads(line) for line in it.islice(in_file, n_records)]
        print('Total messages: %s\n' % len(records))

        by_user = {}
        for rec in records:
            uid = rec['author']
            # some are fake users and very high count, skip 'em
            if uid in blacklist or                'bot' in uid.lower(): # no bots allowed
                continue

            if uid not in by_user:
                by_user[uid] = []
            by_user[uid] += [dict(created_utc=int(rec['created_utc']),
                                  author=rec['author'],
                                  subreddit=rec['subreddit'],
                                  body=rec['body'])]
        print('Total users: %s\n' % len(by_user))

        skimmed_by_user_lst = [(uid, msg_lst)
                               for uid, msg_lst in by_user.items()
                               if len(msg_lst) >= min_msg]

        sorted_by_user_lst = sorted(skimmed_by_user_lst,
                                    key=lambda x: len(x[1]),
                                    reverse=True)
        return sorted_by_user_lst

all_lst = {}
# process 1e6 messages at the time
for input_file in input_files:
    t0 = time.time()
    sorted_by_user_lst = group_by_author(input_file, blacklist=oulier_users)
    print('%s took %.2f secs\n' % (os.path.basename(input_file), time.time()-t0))

    print('OK users: %d\n' % len(sorted_by_user_lst))

    print('Top Users [%s]:\n' % input_file)
    for uid, msg_lst in sorted_by_user_lst[:10]:
        print(uid, len(msg_lst))

    # merge
    for uid, msg_lst in sorted_by_user_lst:
        if uid not in all_lst:
            all_lst[uid] = []
        all_lst[uid] += msg_lst

    print('\ntotal %d\n' % len(all_lst))
    print('#########\n')

# Re-sort msg lists by time
for uid, msg_lst in all_lst.items():
    all_lst[uid] = sorted(msg_lst, key=lambda x: x['created_utc'])

# Ovearall Top
all_sorted_by_user_lst = sorted(all_lst.items(),
                            key=lambda x: len(x[1]),
                            reverse=True)

print('Top Users [ALL]:\n')
for uid, msg_lst in it.islice(all_sorted_by_user_lst, 10):
    print(uid, len(msg_lst))


# In[20]:

def save_user_avro(sorted_msgs, output_dir):
    from fastavro import writer
    schema = {
        'doc': 'A collection of messages for Reddit user',
        'name': 'reddit_user_log',
        'namespace': 'reddit',
        'type': 'record',
        'fields': [
            {'name': 'created_utc', 'type': 'long'},
            {'name': 'author', 'type': 'string'},
            {'name': 'subreddit', 'type': 'string'},
            {'name': 'body', 'type': 'string'},
        ],
    }
    if not os.path.exists(output_dir):
        print('creating dir: %s' % output_dir)
        os.mkdir(output_dir)
    for uid, records in sorted_msgs:
        out_path = os.path.join(output_dir, '%s.%s.avro' % (uid, len(records)))
        with open(out_path, 'wb') as out:
            writer(out, schema, records)
            print('created avro file: %s' % out_path)

save_user_avro(all_sorted_by_user_lst, avro_path)

print('Top Users [ALL=%d]:\n' % len(all_sorted_by_user_lst))
for uid, msg_lst in it.islice(all_sorted_by_user_lst, 100):
    print(uid, len(msg_lst))


# import fastavro as avro

# class AvroStreamer:
    # def __init__(self, input_dir, max_users=None, min_msg=None, max_msg=None):
        # self.input_dir = input_dir
        # self.max_users = max_users
        # self.min_msg = min_msg
        # self.max_msg = max_msg
        # self.n_msgs = 0

        # # open files
        # self.open_files()

    # def open_files(self):
        # all_avros = [os.path.join(self.input_dir, f)
                     # for f in next(os.walk(self.input_dir))[2]
                     # if f.endswith('.avro')]

        # all_avros = sorted(all_avros, key=lambda x: int(x.split('.')[2]), reverse=True)

        # filtered_avros = []
        # for favro in all_avros:
            # n_msgs = int(favro.split('.')[2])
            # if (self.min_msg is None or n_msgs >= self.min_msg) and                (self.max_msg is None or n_msgs <= self.max_msg):
                # filtered_avros += [favro]
                # self.n_msgs += n_msgs
            # if self.max_users is not None and len(filtered_avros) >= self.max_users:
                # break
        # self.avros = filtered_avros
        # self.avros = [avro.reader(open(fo, 'rb')) for fo in filtered_avros]

    # def __iter__(self):
        # for stream in self.avros:
            # yield next(stream)

    # @property
    # def n_users(self):
        # return len(self.avros)

# streamer = AvroStreamer(avro_path, max_users=1000, min_msg=100, max_msg=500)
# print(streamer.n_msgs, streamer.n_users)
# print(list(it.islice(streamer, 5)))


# # In[ ]:




# # In[ ]:



