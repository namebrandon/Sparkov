from __future__ import division
import redis
import cPickle
from pyspark import SparkContext,SparkConf
from kafka import KafkaConsumer
from itertools import product, islice
from toolz import concat
import time
from math import log
import numpy as np
import pandas as pd #pip 27 install pandas
import folium   # pip 27 install folium
import datetime
import os


__author_ = 'Brandon Harris - harrirb@uchicago.edu'


# AMAZON AWS - CHANGE KAFKA IP

def gen_map(fraud_limit):
    for index,row in fraud_limit.iterrows():
        trans_num = row ['tx_id']
        trans_date = row['tx_date']
        trans_time = row['tx_time']
        amt = row['tx_amt']
        merchant = row['merchant']
        merchant = merchant.strip('fraud_')
        first = row['fname']
        last = row['lname']
        merchant_lat = row['merch_lat']
        merchant_long = row['merch_long']

        skip_marker = False
        #try:
        #    location = geolocator.reverse(str(merchant_lat) + ", " + str(merchant_long))
        #except GeocoderTimedOut as e:
        #    skip_marker = True

        if not skip_marker:
            pop_data = 'Trans Num: ' + trans_num + "<br/>"
            pop_data = pop_data + "Trans Time: " + trans_date + " - " + trans_time + "<br/>"
            pop_data = pop_data + "Amount " + str(amt) + "<br/>"
            pop_data = pop_data + "Customer: " + first + " " + last + "<br/>"
            pop_data = pop_data + "Merchant: " + merchant +"<br/>"
            #pop_data = pop_data + str(location.address)
            map.simple_marker([float(merchant_lat), float(merchant_long)], popup=pop_data, clustered_marker=True, marker_color="red")
        #time.sleep(0.5)
    map.create_map(path='map/fraud_map.html')


consumer = KafkaConsumer('cc_fraud_detected',bootstrap_servers=['172.31.52.33:9092'])
map = folium.Map(location=[39.809, -98.555], tiles='OpenStreetMap',zoom_start=4)

my_columns = ['tx_id','fname','lname','merchant','tx_amt','tx_date','tx_time','merch_lat','merch_long']

df = pd.DataFrame(columns = my_columns)
f = open('fraud_detected.csv','w')

f.write("trans_num|amt|first|last|merchant|unix_time|category|three_letter|cc_num|merch_lat|merch_long|ssn|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|trans_date|trans_time|is_fraud|profile")
f.write("\n")

counter = 0

for message in consumer:

        m = message.value
        tx_data = []

        data = m.split('|')

        tx_id = data[0]
        tx_amt = data[1]
        fname = data[2]
        lname = data[3]
        merchant = data[4]
	merchant = merchant.replace('fraud_','')
        tx_unix_time = data[5]
        tx_category = data[6]
	tx_three_letter = data[7]
        ccnum = data[8]
        merch_lat = data[9]
        merch_long = data[10]
        tx_ssn = data[11]
        tx_gender = data[12]
        tx_street = data[13]
        tx_city = data[14]
	tx_state = data[15]
        tx_zip = data[16]
	tx_user_lat = data[17]
	tx_user_long = data[18]
        tx_city_pop = data[19]
        tx_job = data[20]
        tx_dob = data[21]
        tx_acct_num = data[22]
        tx_trans_date = data[23]
        tx_trans_time = data[24]
        is_fraud = data[25]
	tx_profile= data[26]

	tx_profile = tx_profile.strip('fraud_')


#        tx_date =datetime.datetime.fromtimestamp(int(tx_unix_time)).strftime('%m-%d-%Y')


#        tx_time=datetime.datetime.fromtimestamp(int(tx_unix_time)).strftime('%H:%M:%S')
	
	
	tx_date = tx_trans_date
	tx_time = tx_trans_time

        tx_data.append(tx_id)
        tx_data.append(fname)
        tx_data.append(lname)
        tx_data.append(merchant)
        tx_data.append(tx_amt)
        tx_data.append(tx_date)
        tx_data.append(tx_time)
        tx_data.append(merch_lat)
        tx_data.append(merch_long)

        #insert row into dataframe
        df.loc[len(data)] = tx_data

	m = m.replace('fraud_','')

	f.write(m)
	f.write("\n")

	counter = counter+1

	# flush buffer to file every 30 lines
	if counter >29:
		f.flush()
		os.fsync(f.fileno())
		counter =0

#	output = str(tx_id) +"|"+ str(tx_amt) +"|"+  str(fname) +"|"+  str(lname) +"|"+  str(merchant)\
#         +"|"+ str(tx_time) +"|"+  str(tx_category)  +"|"+ str(tx_three_letter) +"|"+  str(ccnum)\
#         +"|"+  str(merch_lat)  +"|"+ str(merch_long) +"|"+ str(tx_ssn) +"|"+  str(tx_gender) +"|"+  str(tx_street) +"|"+  str(tx_city)\
#         +"|"+ str(tx_state) +"|"+  str(tx_zip) +"|"+ str(tx_user_lat) +"|"+ str(tx_user_long) +"|"+ str(tx_city_pop)  +"|"+ str(tx_job) +"|"+  str(tx_dob)\
#         +"|"+  str(tx_acct_num)  +"|"+ str(tx_trans_date) +"|"+ str(tx_trans_time) +"|"+  str(is_fraud) +"|"+ str(tx_profile)

        gen_map(df)
        print str(m) +"\n"
	print "\n"



f.close()




