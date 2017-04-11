#!/bin/bash

BASE=$1
FALL=reddit_v2

# split comments into 1e6 lines files
for YEAR in 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016
do
    echo $BASE/$YEAR/
    lbzip2 -d -c $BASE/$YEAR/* | split -l 1000000 -d -a 4 --additional-suffix .split_$YEAR --filter='lbzip2 > $FILE.bz2'
done

# move everything into a folder for convenience
mkdir -p 1e6
mv *split* 1e6

# filter, group and save into avro
python3 reddit_make_avro.py

# clean up!
#rm -r 1e6
