#!/bin/bash

#create temp streaming file
sudo cp adults_2550_male_rural.csv streaming_file_temp.csv

#drop header in file
sudo sed -i '1d' streaming_file_temp.csv

#create streaming file
sudo echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > streaming_file.csv

#sort transactions according to time stamp
sudo sort -t'|' -k20 -g --parallel=4 streaming_file_temp.csv >> streaming_file.csv

#cleanup
sudo rm streaming_file_temp.csv

sudo mkdir streaming_data
sudo mv streaming_file.csv streaming_data
cd streaming_data

sudo split --suffix-length=4 --line-bytes=200MB --numeric-suffixes streaming_file.csv stream_data
sudo sed -i '1d' stream_data0000

