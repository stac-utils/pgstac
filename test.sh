#!/bin/bash
q="SELECT count(*) from search('{\"limit\":1}');"
psql -c "$q" &
psql -c "$q" &
psql -c "$q" &
psql -c "$q" &
psql -c "$q" &
psql -c "$q" &
