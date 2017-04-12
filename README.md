# Reddit User Comments Dataset

A dataset of XXX selected Reddit users collected between 2007 and 2016.
Each user has at least 100 comments.
Total number of selected comments: XXX

## Download

The preparation of this dataset is extremely space and time consuming, so it's reccomended to dowload the resulting avro files using this torrent:
```
 XXX TORRENT
```

## Build from Scratch

The raw data can be downloaded using this torrent file:
```
http://code.dewarim.com/reddit-2016-08.torrent
```
Dowload size: 237 GByte
Total comments: XXX

### Prepare

Use the script `prepare.sh`, for instance assuming the downloaded files are located in the `reddit_data` directory:
```
./prepare.sh reddit_data
```
This decompresses the files on the fly and splits them into json lines files of 1 million comments each, in a temporary local directory called `1e6`.

Then python script `reddit_make_avro.py` is then used to filter the raw comments.

The `avro` directory then contains one avro per user, where each user has at least 100 messages.

The filename format is <user_name>.<num_msg>.avro

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

