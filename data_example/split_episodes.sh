#!/usr/bin/env bash

set -e

header=$(head -n 1 episodes.csv)

tail -n +2 episodes.csv | 
    split - episodes_ --lines=10 --suffix-length=2 --numeric-suffixes=1 --additional-suffix=.split

for file in episodes_*.split; do
    (echo "$header"; cat "$file") > "${file%.*}.csv"
    rm "$file"
done