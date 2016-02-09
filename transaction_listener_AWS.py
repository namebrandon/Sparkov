from __future__ import division
import redis
import cPickle
from pyspark import SparkContext,SparkConf
from kafka import KafkaConsumer, SimpleProducer, KafkaClient
from itertools import product, islice
from toolz import concat
import time
from math import log
import numpy as np

__author_ = 'Brandon Harris - harrirb@uchicago.edu'

def window(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def missProb(string_of_seq,window_size,tsm):

    # for our list of sequences create sliding window set of lists
    windows = window(string_of_seq,window_size)
    window_prob={}

    for value in windows:
        prob_sum=0.000
        for index in range(len(value)-1):
            # LETTER SEQ = value[index] and value[index+1]
            # the position in the tsm for LETTER SEQ
            # is coding[value[index]] and coding[value[index+1]]
            # uncomment below for debug helping
            # print value[index] + " : " + value[index+1]
            # print coding
            # print coding[value[index]]
            # print tsm[coding[value[index]]]
            # print('---')
            # print coding[value[index+1]]
            # print tsm[coding[value[index]]][coding[value[index+1]]]
            # print tsm[coding[value[index]]]

            prob_sum = prob_sum + sum(tsm[coding[value[index]]]) - tsm[coding[value[index]]][coding[value[index+1]]]
        window_prob[value] = prob_sum / (window_size - 1)

    return window_prob


def redis_push(r,cust_id,tx):
    # push to redis stack, maintaining only last 4 tx
    # in memory
    last4 = r.get(cust_id)
    if len(last4) >= 11:
        last4 = ','.join(last4.split(',')[-4:])
        push = last4 + "," + tx
        r.set(cust_id,push)
    else:
        print("ERROR: NO HISTORY FOUND FOR CUST_ID: " + cust_id)
    return

#perf_update
def redis_push_list(r,key,value):

    user_last = r.get(key)
    if user_last is None:
        temp_list = []
        temp_list.append(value)

        r.set(key,cPickle.dumps(temp_list))
    else:
        user_last = cPickle.loads(user_last)
        if len(user_last) >4:
            temp_list = user_last[1:5]
            temp_list.append(value)
            r.set(key,cPickle.dumps(temp_list))

        else:
            user_last.append(value)
            r.set(key,cPickle.dumps(user_last))

def eval_transaction(tx):

    # ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long
    tx_data = tx.split("|")

    tx_ssn = tx_data[0]
    tx_gender = tx_data[4]
    tx_street = tx_data[5]
    tx_city = tx_data[6]
    tx_state = tx_data[7]
    tx_zip = tx_data[8]
    tx_user_lat = tx_data[9]
    tx_user_long = tx_data[10]
    tx_city_pop = tx_data[11]
    tx_job = tx_data[12]
    tx_dob = tx_data[13]
    tx_acct_num = tx_data[14]
    tx_trans_date = tx_data[17]
    tx_trans_time = tx_data[18]
    cc_num = tx_data[1]
    tx_profile = tx_data[15]
    tx_unix_time = tx_data[19]
    tx_category = tx_data[20]
    tx_amt = float(tx_data[21])
    tx_id = tx_data[16]
    fname = tx_data[2]
    lname = tx_data[3]
    merchant = tx_data[23]
    is_fraud = tx_data[22]
    trans_time = tx_data[18]
    user_last4 = r.get(cc_num)
    profile_last4 = r.get(tx_profile)
    user_cat_avg = r_agg.get(tx_category+'_'+cc_num)
    merch_lat = tx_data[24]
    merch_long = tx_data[25]


    if user_cat_avg is None:
        no_expense_data = True
    else:
        user_cat_avg = cPickle.loads(user_cat_avg)
        no_expense_data = False

    #perf_update
    # If a user has never bought in a category prior to this transaction
    # set their average to zero.


    #perf_update
    user_prior_unix_ts = r_agg.get('timestamp_'+cc_num)
    if user_prior_unix_ts is None:
        user_prior_unix_ts=0

    #perf_update
    # Get and update the last 5 transaction ID's for a given user and
    # append with latest transaction ID, dropping first tx_id if
    # our list of tx_id's is larger than 4 (i.e. keep last 5 only).


    redis_push_list(r_perf,cc_num,tx_id)
    user_last_4_tx_id = cPickle.loads(r_perf.get(cc_num))

    #perf_update
    # If we are a new user, assuming 4 normal transactions for prior
    #if user_last4 is None:
    #    user_last4 = "NPN,NPN,NPN,NPN"     #NORMAL PRESENT NORMAL

    amount_var = ''
    card_present = ''
    time_var = ''


    # if '_net' in tx_category:
    #     card_present='N'
    # else:
    #     card_present='P'

    time_val = int(trans_time[:2])
    if time_val <5 or time_val >21:
        norm_abnorm='A'
    else:
        norm_abnorm='N'


    if no_expense_data == False:
        user_amount_low_bound = user_cat_avg[0]
        user_amount_high_bound = user_cat_avg[1]

        if float(tx_amt) > (float(user_amount_high_bound)):
            amount_var='H'
        elif float(tx_amt) < (float(user_amount_low_bound)):
            amount_var='L'
        else:
            amount_var='N'
    else:
        amount_var='N'

    tx_lag = int(tx_unix_time) - int(user_prior_unix_ts)

    user_time_bounds = cPickle.loads(r_agg.get('velocity_'+cc_num))
    user_time_low_bound = user_time_bounds[0]
    user_time_high_bound = user_time_bounds[1]

    if tx_lag < float(user_time_low_bound):
        time_var = 'H'
    elif tx_lag > float(user_time_high_bound):
        time_var = 'L'
    else:
        time_var='N'

    tx = amount_var+norm_abnorm+time_var

    return tx_id, tx_amt, fname, lname, merchant, tx_unix_time, tx_category, user_last4, profile_last4, tx_profile,\
           tx, cc_num, is_fraud, user_last_4_tx_id, merch_lat,merch_long,\
	   tx_ssn, tx_gender, tx_street, tx_city, tx_state, tx_zip,\
	   tx_user_lat, tx_user_long, tx_city_pop, tx_job, tx_dob,\
	   tx_acct_num, tx_trans_date, tx_trans_time, tx_profile


# AWS - Change Redis IP
redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
redis_port = 6379
r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
r_full = redis.StrictRedis(host=redis_ip, port=redis_port, db=1)
r_tsm = redis.StrictRedis(host=redis_ip, port=redis_port, db=2)
r_agg = redis.StrictRedis(host=redis_ip, port=redis_port, db=3)
# perf_update
r_perf = redis.StrictRedis(host=redis_ip, port=redis_port,db=4)
r_perf.flushdb()


# perf_update

#perf_update
# Init our scoring matrix to record performance
performance_matrix = np.zeros(shape=(0,2))
score_matrix = np.array([[1,2]])
tx_count = 0
fraud_tx_count =0

# Amazon AWS - Change Kafka IP
consumer = KafkaConsumer('cc_stream_tx2',bootstrap_servers=['172.31.52.33:9092'])
window_size=5

# Amazon AWS - Change Kafka IP

kafka_fraud_writer = KafkaClient('172.31.52.33:9092')
producer = SimpleProducer(kafka_fraud_writer)


# combos = map(lambda x: "".join(concat(x)),list(product(product("HNL", "PN", "HNL"))))
# ENCODE THEM 0-17 (for 18x18 matrix)
# coding = {j: i for i, j in enumerate(combos)}

#coding = {'NPL': 8, 'NPN': 7, 'NPH': 6, 'NNH': 9, 'HNH': 3, 'LPH': 12, 'HNL': 5, 'LNN': 16, 'HNN': 4, 'NNL': 11, 'LPL': 14, 'HPH': 0, 'HPN': 1, 'LPN': 13, 'HPL': 2, 'NNN': 10, 'LNL': 17, 'LNH': 15}
coding = {'HAH': 0, 'HAL': 2, 'HAN': 1, 'HNH': 3, 'HNL': 5, 'HNN': 4, 'LAH': 12, 'LAL': 14, 'LAN': 13, 'LNH': 15, 'LNL': 17, 'LNN': 16, 'NAH': 6, 'NAL': 8, 'NAN': 7, 'NNH': 9, 'NNL': 11, 'NNN': 10}

for message in consumer:
    t0 = time.time()
    # reduce tx to cust_id,list
    # print(message.value.split(','))
    m = message.value


    tx_id, tx_amt, fname, lname, merchant, tx_time, tx_category,\
        user_last4, profile_last4, profile, tx, ccnum, is_fraud,\
        user_last_tx_ids,merch_lat,merch_long, tx_ssn,\
	tx_gender, tx_street, tx_city, tx_state, tx_zip,\
	tx_user_lat, tx_user_long, tx_city_pop, tx_job, \
	tx_dob, tx_acct_num, tx_trans_date,\
	tx_trans_time, tx_profile = eval_transaction(m)
		

    user_sequence =  user_last4 + ',' + tx
    profile_sequence = profile_last4 + ',' + tx

    tx_count = tx_count + 1

    #perf_update
    user_fraud_prob = 0.00
    profile_fraud_prob =0.00

    user_tsm = cPickle.loads(r_tsm.get(ccnum))
    profile_tsm = cPickle.loads(r_tsm.get(profile))

    user_seq_list = user_sequence.split(",")

    if len(user_seq_list)>5:
       user_seq_list = user_sequence.split(",")[1:]

    #profile_seq_list = profile_sequence.split(",")
    #if len(profile_seq_list)>5:
    #    profile_seq_list = profile_sequence.split(",")[1:]

    results = missProb(user_seq_list,window_size,user_tsm)



    debug_tx = True
    debug_output = True
    pause_output_when_fraud_found = False
    pause_output_when_true_fraud = False
    user_weight = 0.7
    profile_weight = 0.3
    #perf_update
    if not debug_tx:
        #perf_udpate
        fraud_threshold_user = 0.945
        fraud_threshold_profile = 0.945
        performance_matrix_user_fraud_threshold=0.945
        performance_matrix_profile_fraud_threshold=0.945
    else:
        fraud_threshold_user = -1
        fraud_threshold_profile = -1
        performance_matrix_user_fraud_threshold=0.945
        performance_matrix_profile_fraud_threshold=0.945

    if debug_output:

        print("----"*40)
        print("Name:            " + fname + " " + lname)
        print("Merchant:        " + merchant)
        print("Category:        " + tx_category)
        print("Amount:          " + str(tx_amt))
        print("Date / Time:     " + time.ctime(int(tx_time)))
        print("isFraud:         " + str(is_fraud))
    # perf_update
        print("Last 5 Tx Id     " + str(user_last_tx_ids))

    # results = localEntropyReduc(user_seq_list,window_size,user_tsm)
    for k,v in results.iteritems():
        if v > fraud_threshold_user:

            if not debug_output:
                print("----"*40)
                print("Name:            " + fname + " " + lname)
                print("Merchant:        " + merchant)
                print("Category:        " + tx_category)
                print("Amount:          " + str(tx_amt))
                print("Date / Time:     " + time.ctime(int(tx_time)))
                print("isFraud:         " + str(is_fraud))
            print "Card Num:        " + ccnum + "                        Last 4 Seq: " + str(user_seq_list[:4])\
                  + " New Tx : " + tx + "   Fraud Prob : " + str(v)
        user_fraud_prob = v
    # results = localEntropyReduc(profile_seq_list,window_size,profile_tsm)

    results = missProb(user_seq_list, window_size, profile_tsm)
    for k,v in results.iteritems():
        if v > fraud_threshold_profile:
            print "Profile:         " + profile + "         Last 4 Seq: " + str(user_seq_list[:4])\
                  + " New Tx : " + tx + "   Fraud Prob : " + str(v)
        profile_fraud_prob = v

    if debug_output:
        print("----"*40)
        t1 = time.time()
        print('Transaction Time Elapsed: ' + str(t1 - t0))
        print("----"*20)

    #perf_update
    # updated our performance matrix with new binary row.
    # Col1 = Did this window contain fraud? 1 / 0
    # Col2 = Did we detect this window as containing fraud?
    #
    update_matrix = []

    #Does the current window contain a fraudulent transaction?
    if is_fraud == "1":
        # push to our list of fraud tx id's
        redis_push_list(r_perf,ccnum+"_if_tx",tx_id)

        update_matrix.append(1)
        r_perf.set(str(ccnum)+"_counter",window_size-1)
        fraud_tx_count = fraud_tx_count + 1
    else:
        window_counter = r_perf.get(str(ccnum)+"_counter")
        if window_counter is None:
            window_counter = 0
        window_counter = int(window_counter)
        if window_counter >0:
            window_counter = window_counter-1
            r_perf.set(str(ccnum)+"_counter",window_counter)
            update_matrix.append(1)
        else:
            update_matrix.append(0)

    # Did we detect fraudulent activity within current window?
    # Eventually need to add profile probability into this
    #if user_fraud_prob >= performance_matrix_user_fraud_threshold or profile_fraud_prob > performance_matrix_profile_fraud_threshold :
    if pause_output_when_true_fraud==True and is_fraud=="1":
            raw_input("--Pausing -- Press Enter--")

    if ((user_weight*user_fraud_prob) + (profile_weight*profile_fraud_prob)) >= performance_matrix_user_fraud_threshold:
        # Any time we detect fraud, store our current window of transaction ID's
        r_perf.set(ccnum+"_l5_fraud",cPickle.dumps(user_last_tx_ids))
        update_matrix.append(1)
        if pause_output_when_fraud_found:
            raw_input("--Pausing -- Press Enter--")

        # SEND Kafka Fraud Listener TX data

        fraud_tx_string = ''

        fraud_tx_string = str(tx_id) +"|"+ str(tx_amt) +"|"+  str(fname) +"|"+  str(lname) +"|"+  str(merchant)\
         +"|"+ str(tx_time) +"|"+  str(tx_category)  +"|"+ str(tx) +"|"+  str(ccnum)\
         +"|"+  str(merch_lat)  +"|"+ str(merch_long) +"|"+ str(tx_ssn) +"|"+  str(tx_gender) +"|"+  str(tx_street) +"|"+  str(tx_city)\
         +"|"+ str(tx_state) +"|"+  str(tx_zip) +"|"+ str(tx_user_lat) +"|"+ str(tx_user_long) +"|"+ str(tx_city_pop)  +"|"+ str(tx_job) +"|"+  str(tx_dob)\
         +"|"+  str(tx_acct_num)  +"|"+ str(tx_trans_date) +"|"+ str(tx_trans_time) +"|"+  str(is_fraud) +"|"+  str(lname) +"|"+  str(merchant)\
         +"|"+ str(tx_time) +"|"+  str(tx_category)  +"|"+ str(tx) +"|"+  str(ccnum)\
         +"|"+  str(merch_lat)  +"|"+ str(merch_long) +"|"+ str(tx_profile)

        producer.send_messages('cc_fraud_detected', fraud_tx_string)


    else:
        # We can't just mark not detecting the fraud as false, because if we had detected it previously
        # and the TX is still in the window, a real business would be looking at these TX's already.
        # So if we fail to detect fraud, we need to cross reference our known list of last 5 (real) fraud TX's
        # with the snapshot of the window at the time we last detected fraud. If there are any matches
        # then a transaction we previously detected as fraud (correctly), is still in the current window.
        # If a transaction we previously detected as fraud is still in our current window, we automatically mark the
        # current window as detecting fraud. Otherwise, we really did fail to detect it, and we make false (failed).

        # Get our last window when we detected fraud (and handle the possibility we never found fraud previously).
        temp_l5 = r_perf.get(ccnum+"_l5_fraud")
        if temp_l5 is None:
            last5_window_of_fraud=[]
        else:
            last5_window_of_fraud = cPickle.loads(temp_l5)

        #print("Last 5 We Detected: " + str(last5_window_of_fraud))

        # Get our list of last 5 known / labeled fraud transaction id's (and handle the no data scenario)
        temp_l5k = r_perf.get(ccnum+"_if_tx")
        if temp_l5k is None:
            last5_real_fraud =[]
        else:
            last5_real_fraud = cPickle.loads(temp_l5k)

        #print("Last 5 is_fraud=1: " + str(last5_real_fraud))

        # If any real fraud exist in our last 5 fraud detected window, return them
        matching_list = [e for e in last5_real_fraud if e in last5_window_of_fraud]
        length_ml = len(matching_list)

        #print("Matches Last 5 Real / Detected: " + str(matching_list))
        # Now we check to see if there were matches, are there any of those matches in our current window
        further_matches = [g for g in matching_list if g in user_last_tx_ids]
        length_fm = len(further_matches)

        #print("Matches with Current Window: " + str(further_matches))
        #print("Current Window: " + str(user_last_tx_ids))

        # If there were matches in both of the above checks, then we had already detected fraud for a TX
        # within the current window we're analyzing
        if length_ml > 0 and length_fm > 0:
            update_matrix.append(1)

        # Otherwise we failed to detect fraud in the current transaction.
        else:
            update_matrix.append(0)

    performance_matrix = np.vstack([performance_matrix, update_matrix])
    update_matrix = []

    #calculate performance metrics
    score = performance_matrix * score_matrix
    totals = score.sum(axis=1)

    #Assign performnace metrics (handling division by zero)

    if (np.shape(totals[np.where( totals == 1 )])[0] + np.shape(totals[np.where( totals == 3 )])[0]) ==0:
        TruePos = 0
    else:
        TruePos = np.shape(totals[np.where( totals == 3 )])[0] / \
                  (np.shape(totals[np.where( totals == 1 )])[0] + np.shape(totals[np.where( totals == 3 )])[0])

    if (np.shape(totals[np.where( totals == 0 )])[0] + np.shape(totals[np.where( totals == 2 )])[0]) ==0:
        TrueNeg = 0
    else:
        TrueNeg = np.shape(totals[np.where( totals == 0 )])[0] / \
                  (np.shape(totals[np.where( totals == 0 )])[0] + np.shape(totals[np.where( totals == 2 )])[0])

    if (np.shape(totals[np.where( totals == 0 )])[0] + np.shape(totals[np.where( totals == 2 )])[0]) ==0:
        FalsePos = 0
    else:
        FalsePos = np.shape(totals[np.where( totals == 2 )])[0] / \
               (np.shape(totals[np.where( totals == 0 )])[0] + np.shape(totals[np.where( totals == 2 )])[0])

    if (np.shape(totals[np.where( totals == 1 )])[0] + np.shape(totals[np.where( totals == 3 )])[0]) ==0:
        FalseNeg = 0
    else:
        FalseNeg = np.shape(totals[np.where( totals == 1 )])[0] / \
               (np.shape(totals[np.where( totals == 1 )])[0] + np.shape(totals[np.where( totals == 3 )])[0])

    print("TruePos: " + str(TruePos) + "   TrueNeg: " + str(TrueNeg) + \
          "   FalsePos: " + str(FalsePos) + "   FalseNeg: " + str(FalseNeg) + \
          "   Total Tx Evaluated: " + str(tx_count) + "   Total Fraud Tx Seen: " +str(fraud_tx_count))
    print("----"*40+"\n")

    #print performance_matrix
    #print totals



    # update transactions for profile/user with latest
    # as well as update latest tx time.
    redis_push(r,ccnum,tx)
    redis_push(r,profile,tx)
    r_agg.set('timestamp_'+ccnum,tx_time)

