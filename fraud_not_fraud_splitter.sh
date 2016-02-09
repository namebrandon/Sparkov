#!/bin/bash


#Split on Fraud and Legit

sed -n '/|1|/p' adults_2550_female_rural.csv  > f_adults_2550_female_rural.csv
sed -n '/|1|/p' adults_2550_female_urban.csv  > f_adults_2550_female_urban.csv
sed -n '/|1|/p' adults_2550_male_rural.csv  > f_adults_2550_male_rural.csv
sed -n '/|1|/p' adults_2550_male_urban.csv  > f_adults_2550_male_urban.csv
sed -n '/|1|/p' young_adults_female_rural.csv  > f_young_adults_female_rural.csv
sed -n '/|1|/p' young_adults_female_urban.csv  > f_young_adults_female_urban.csv
sed -n '/|1|/p' young_adults_male_rural.csv  > f_young_adults_male_rural.csv
sed -n '/|1|/p' young_adults_male_urban.csv  > f_young_adults_male_urban.csv
sed -n '/|1|/p' adults_50up_female_rural.csv  > f_adults_50up_female_rural.csv
sed -n '/|1|/p' adults_50up_female_urban.csv  > f_adults_50up_female_urban.csv
sed -n '/|1|/p' adults_50up_male_rural.csv  > f_adults_50up_male_rural.csv
sed -n '/|1|/p' adults_50up_male_urban.csv  > f_adults_50up_male_urban.csv


sed -n '/|0|/p' adults_2550_female_rural.csv  > l_adults_2550_female_rural.csv
sed -n '/|0|/p' adults_2550_female_urban.csv  > l_adults_2550_female_urban.csv
sed -n '/|0|/p' adults_2550_male_rural.csv  > l_adults_2550_male_rural.csv
sed -n '/|0|/p' adults_2550_male_urban.csv  > l_adults_2550_male_urban.csv
sed -n '/|0|/p' young_adults_female_rural.csv  > l_young_adults_female_rural.csv
sed -n '/|0|/p' young_adults_female_urban.csv  > l_young_adults_female_urban.csv
sed -n '/|0|/p' young_adults_male_rural.csv  > l_young_adults_male_rural.csv
sed -n '/|0|/p' young_adults_male_urban.csv  > l_young_adults_male_urban.csv
sed -n '/|0|/p' adults_50up_female_rural.csv  > l_adults_50up_female_rural.csv
sed -n '/|0|/p' adults_50up_female_urban.csv  > l_adults_50up_female_urban.csv
sed -n '/|0|/p' adults_50up_male_rural.csv  > l_adults_50up_male_rural.csv
sed -n '/|0|/p' adults_50up_male_urban.csv  > l_adults_50up_male_urban.csv


#Drop headers

sed -i '1d' f_adults_2550_female_rural.csv
sed -i '1d' f_adults_2550_female_urban.csv
sed -i '1d' f_adults_2550_male_rural.csv
sed -i '1d' f_adults_2550_male_urban.csv
sed -i '1d' f_young_adults_female_rural.csv
sed -i '1d' f_young_adults_female_urban.csv
sed -i '1d' f_young_adults_male_rural.csv
sed -i '1d' f_young_adults_male_urban.csv
sed -i '1d' f_adults_50up_female_rural.csv
sed -i '1d' f_adults_50up_female_urban.csv
sed -i '1d' f_adults_50up_male_rural.csv
sed -i '1d' f_adults_50up_male_urban.csv



sed -i '1d' l_adults_2550_female_rural.csv
sed -i '1d' l_adults_2550_female_urban.csv
sed -i '1d' l_adults_2550_male_rural.csv
sed -i '1d' l_adults_2550_male_urban.csv
sed -i '1d' l_young_adults_female_rural.csv
sed -i '1d' l_young_adults_female_urban.csv
sed -i '1d' l_young_adults_male_rural.csv
sed -i '1d' l_young_adults_male_urban.csv
sed -i '1d' l_adults_50up_female_rural.csv
sed -i '1d' l_adults_50up_female_urban.csv
sed -i '1d' l_adults_50up_male_rural.csv
sed -i '1d' l_adults_50up_male_urban.csv



#create empty files with headers
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_2550_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_2550_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_2550_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_2550_male_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_young_adults_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_young_adults_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_young_adults_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_young_adults_male_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_50up_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_50up_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_50up_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_f_adults_50up_male_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_2550_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_2550_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_2550_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_2550_male_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_young_adults_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_young_adults_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_young_adults_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_young_adults_male_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_50up_female_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_50up_female_urban.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_50up_male_rural.csv
echo "ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long"  > s_l_adults_50up_male_urban.csv


#Sort by timestamp

sort -t'|' -k20 -g --parallel=4 f_adults_2550_female_rural.csv > s_f_adults_2550_female_rural.csv
sort -t'|' -k20 -g --parallel=4 f_adults_2550_female_urban.csv > s_f_adults_2550_female_urban.csv
sort -t'|' -k20 -g --parallel=4 f_adults_2550_male_rural.csv > s_f_adults_2550_male_rural.csv
sort -t'|' -k20 -g --parallel=4 f_adults_2550_male_urban.csv > s_f_adults_2550_male_urban.csv
sort -t'|' -k20 -g --parallel=4 f_young_adults_female_rural.csv > s_f_young_adults_female_rural.csv
sort -t'|' -k20 -g --parallel=4 f_young_adults_female_urban.csv > s_f_young_adults_female_urban.csv
sort -t'|' -k20 -g --parallel=4 f_young_adults_male_rural.csv > s_f_young_adults_male_rural.csv
sort -t'|' -k20 -g --parallel=4 f_young_adults_male_urban.csv > s_f_young_adults_male_urban.csv
sort -t'|' -k20 -g --parallel=4 f_adults_50up_female_rural.csv > s_f_adults_50up_female_rural.csv
sort -t'|' -k20 -g --parallel=4 f_adults_50up_female_urban.csv > s_f_adults_50up_female_urban.csv
sort -t'|' -k20 -g --parallel=4 f_adults_50up_male_rural.csv > s_f_adults_50up_male_rural.csv
sort -t'|' -k20 -g --parallel=4 f_adults_50up_male_urban.csv > s_f_adults_50up_male_urban.csv


sort -t'|' -k20 -g --parallel=4 l_adults_2550_female_rural.csv > s_l_adults_2550_female_rural.csv
sort -t'|' -k20 -g --parallel=4 l_adults_2550_female_urban.csv > s_l_adults_2550_female_urban.csv
sort -t'|' -k20 -g --parallel=4 l_adults_2550_male_rural.csv > s_l_adults_2550_male_rural.csv
sort -t'|' -k20 -g --parallel=4 l_adults_2550_male_urban.csv > s_l_adults_2550_male_urban.csv
sort -t'|' -k20 -g --parallel=4 l_young_adults_female_rural.csv > s_l_young_adults_female_rural.csv
sort -t'|' -k20 -g --parallel=4 l_young_adults_female_urban.csv > s_l_young_adults_female_urban.csv
sort -t'|' -k20 -g --parallel=4 l_young_adults_male_rural.csv > s_l_young_adults_male_rural.csv
sort -t'|' -k20 -g --parallel=4 l_young_adults_male_urban.csv > s_l_young_adults_male_urban.csv
sort -t'|' -k20 -g --parallel=4 l_adults_50up_female_rural.csv > s_l_adults_50up_female_rural.csv
sort -t'|' -k20 -g --parallel=4 l_adults_50up_female_urban.csv > s_l_adults_50up_female_urban.csv
sort -t'|' -k20 -g --parallel=4 l_adults_50up_male_rural.csv > s_l_adults_50up_male_rural.csv
sort -t'|' -k20 -g --parallel=4 l_adults_50up_male_urban.csv > s_l_adults_50up_male_urban.csv


#Clean up

rm f_adults_2550_female_rural.csv
rm f_adults_2550_female_urban.csv
rm f_adults_2550_male_rural.csv
rm f_adults_2550_male_urban.csv
rm f_young_adults_female_rural.csv
rm f_young_adults_female_urban.csv
rm f_young_adults_male_rural.csv
rm f_young_adults_male_urban.csv
rm f_adults_50up_female_rural.csv
rm f_adults_50up_female_urban.csv
rm f_adults_50up_male_rural.csv
rm f_adults_50up_male_urban.csv

rm l_adults_2550_female_rural.csv
rm l_adults_2550_female_urban.csv
rm l_adults_2550_male_rural.csv
rm l_adults_2550_male_urban.csv
rm l_young_adults_female_rural.csv
rm l_young_adults_female_urban.csv
rm l_young_adults_male_rural.csv
rm l_young_adults_male_urban.csv
rm l_adults_50up_female_rural.csv
rm l_adults_50up_female_urban.csv
rm l_adults_50up_male_rural.csv
rm l_adults_50up_male_urban.csv



mv s_f_adults_2550_female_rural.csv f_adults_2550_female_rural.csv
mv s_f_adults_2550_female_urban.csv f_adults_2550_female_urban.csv
mv s_f_adults_2550_male_rural.csv f_adults_2550_male_rural.csv
mv s_f_adults_2550_male_urban.csv f_adults_2550_male_urban.csv
mv s_f_young_adults_female_rural.csv f_young_adults_female_rural.csv
mv s_f_young_adults_female_urban.csv f_young_adults_female_urban.csv
mv s_f_young_adults_male_rural.csv f_young_adults_male_rural.csv
mv s_f_young_adults_male_urban.csv f_young_adults_male_urban.csv
mv s_f_adults_50up_female_rural.csv f_adults_50up_female_rural.csv
mv s_f_adults_50up_female_urban.csv f_adults_50up_female_urban.csv
mv s_f_adults_50up_male_rural.csv f_adults_50up_male_rural.csv
mv s_f_adults_50up_male_urban.csv f_adults_50up_male_urban.csv


mv s_l_adults_2550_female_rural.csv l_adults_2550_female_rural.csv
mv s_l_adults_2550_female_urban.csv l_adults_2550_female_urban.csv
mv s_l_adults_2550_male_rural.csv l_adults_2550_male_rural.csv
mv s_l_adults_2550_male_urban.csv l_adults_2550_male_urban.csv
mv s_l_young_adults_female_rural.csv l_young_adults_female_rural.csv
mv s_l_young_adults_female_urban.csv l_young_adults_female_urban.csv
mv s_l_young_adults_male_rural.csv l_young_adults_male_rural.csv
mv s_l_young_adults_male_urban.csv l_young_adults_male_urban.csv
mv s_l_adults_50up_female_rural.csv l_adults_50up_female_rural.csv
mv s_l_adults_50up_female_urban.csv l_adults_50up_female_urban.csv
mv s_l_adults_50up_male_rural.csv l_adults_50up_male_rural.csv
mv s_l_adults_50up_male_urban.csv l_adults_50up_male_urban.csv


