from __future__ import division
from collections import Counter
import redis
import cPickle
from pyspark import SparkContext, SparkConf, HiveContext
import datetime
from pyspark.sql.types import *
import time

__author__ = 'Brandon Harris - harrirb@uchicago.edu'


def process(line):
    # Input: "USER_ID, ABC, BCA, ACB, ...."
    # Output: USER_ID, nested list (matrix)
    #         of transition probabilities.

    # split line into user_id and transactions.
    sequences = line.split(',')
    transactions = [sequences[1:]]
    id = sequences[0]

    # # GET ALL COMBINATIONS OF 3-LETTER SEQUENCES
    # combos = map(lambda x: "".join(concat(x)),list(product(product("HNL", "AN", "HNL"))))
    # ENCODE THEM 0-17 (for 18x18 matrix)
    # coding = {j: i for i, j in enumerate(combos)}

    # CREATE LIST OF TRANSACTIONS USING CODINGS
    # INSTEAD OF LETTERS
    coded = []
    for i in range(len(transactions[0])):
        coded.append(coding[transactions[0][i]])

    # CREATE ZEROED OUT 18X18 MATRIX
    # A bit of a hack here, using 1/18 as an initial
    # value, because this is fake data we're working with
    # and not every state may be seen, resulting in zeroes
    # for outlier behavior. Initialize to 1/18 so that
    # if a state is never seen beforehand, all have
    # equal probabilities of occurring
    # perf_update
    b = [[(1/18) for _ in xrange(18)] for _ in xrange(18)]

    # zip(coded, coded[1:]) gets all the pairs of consecutive numbers.
    # Counter counts how many times each pair appears
    # for loop converts dict to matrix
    for (x, y), c in Counter(zip(coded, coded[1:])).iteritems():
        b[x][y] = c

    # CREATE ZEROED OUT 18x18 MATRIX TO HOLD TSM
    tsm = [[0 for _ in xrange(18)] for _ in xrange(18)]

    # CALCULATE TRANSITION PROBS
    # Sum(s,s')=1
    # Divide count of transition over row of that transition
    for m in range(18):
        for n in range(18):
            if sum(b[m]) == 0:
                tsm[m][n] = 0
            else:
                tsm[m][n] = b[m][n] / sum(b[m])

    return id, tsm


def store_partition(iter):
    # connect to redis and store tsm
    # connections are not serializable via spark!

    redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
    redis_port = 6379
    r_tsm = redis.StrictRedis(host=redis_ip, port=redis_port, db=2)

    for (k, v) in iter:
        tsm = v
        serialized_tsm = cPickle.dumps(tsm)
        r_tsm.set(k, serialized_tsm)


def load_data():
    # load data from files
    # and return query results / aggregates.

    hiveContext = HiveContext(sc)
    # 1027
    # path = '/home/brandon/PycharmProjects/markov_chain/data/raw_tx/'
    # path = '/home/brandon/PycharmProjects/markov_chain/data/raw_tx_fraud/train/'

    # AMAZON AWS EMR
    path = 'hdfs:///tmp/files/'    #HDFS


    # new segement files

    tx_files = [path + 'l_adults_2550_female_rural.csv', path + 'l_adults_2550_female_urban.csv', path + 'l_adults_2550_male_rural.csv', \
    path + 'l_adults_2550_male_urban.csv', path + 'l_young_adults_female_rural.csv', path + 'l_young_adults_female_urban.csv',\
    path + 'l_young_adults_male_rural.csv', path + 'l_young_adults_male_urban.csv', path + 'l_adults_50up_female_rural.csv', \
    path + 'l_adults_50up_female_urban.csv', path + 'l_adults_50up_male_rural.csv', path + 'l_adults_50up_male_urban.csv' ]

    # small file for debugging
    # 1027

    # tx_files = [path + 's_l_male_30_40_smaller_cities.csv']
    # tx_files = [path + 'sorted_fraud_male_30_40_smaller_cities.csv']

    # tx_files = [path+'40_60_bigger_cities.csv',path+'40_60_smaller_cities.csv',path+'all_60_up.csv'\
    #         ,path+'female_30_40_bigger_cities.csv',path+'female_30_40_smaller_cities.csv'\
    #         ,path+'male_30_40_bigger_cities.csv',path+'male_30_40_smaller_cities.csv'\
    #         ,path+'millenials.csv',path+'young_adults.csv']

    # 1027
    # tx_files = [path+'l_40_60_bigger_cities.csv',path+'l_40_60_smaller_cities.csv',path+'l_all_60_up.csv'\
    #         ,path+'l_female_30_40_bigger_cities.csv',path+'l_female_30_40_smaller_cities.csv'\
    #         ,path+'l_male_30_40_bigger_cities.csv',path+'l_male_30_40_smaller_cities.csv'\
    #         ,path+'l_millenials.csv',path+'l_young_adults.csv']



    all_tx = sc.textFile(','.join(tx_files),600)

    # 1027
    # txSchemaString = 'ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|merchant|merch_lat|merch_long'
    txSchemaString = 'ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|is_fraud|merchant|merch_lat|merch_long'
    txFields = [StructField(field_name, StringType(), True) for field_name in txSchemaString.split('|')]
    txFields[17] = StructField('trans_date', DateType(), True)

    txSchema = StructType(txFields)
    # ssn|cc_num|first|last|gender|street|city|state|zip|lat|long|city_pop|job|dob|acct_num|profile|trans_num|trans_date|trans_time|unix_time|category|amt|merchant|merch_lat|merch_long
    txHeader = all_tx.filter(lambda l: "ssn|" in l)
    txNoHeader = all_tx.subtract(txHeader)

    temp_tx = txNoHeader.map(lambda k: k.split("|")).map(lambda p: (
    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16],
    datetime.datetime.strptime(p[17], '%Y-%m-%d').date(), p[18], p[19], p[20], p[21], p[22], p[23], p[24], p[25]))

    h_tx_df = hiveContext.createDataFrame(temp_tx, txSchema)
    h_tx_df.registerTempTable("htx")

    hiveContext.cacheTable("htx")

    # HBASE CODE HERE
    # create dataframe with all records
    # map using hbase_process to extract record into individual componenets
    # and create a dictionary to store in hbase
    #h_data = hiveContext.sql("SELECT * FROM htx")
    #h_data.map(hbase_process).foreachPartition(store_full_data)

    # get cust mean time between transactions
    time_lag_eval = hiveContext.sql(
    "SELECT cc_num, unix_time, LAG(htx.unix_time) OVER (PARTITION BY htx.cc_num ORDER BY  htx.unix_time) as lag_time from htx order by cc_num, unix_time asc")
    time_lag_eval.registerTempTable("ts_lag")

    user_avg_time = hiveContext.sql("SELECT cc_num, AVG(unix_time - lag_time) as time_diff, percentile_approx((unix_time - lag_time),0.1) as low_bound, percentile_approx((unix_time - lag_time),0.90) as high_bound from ts_lag where lag_time is not null group by cc_num")
    user_avg_time.registerTempTable("avg_time")


    # get cust mean per category
    mean_per_cat = hiveContext.sql("SELECT cc_num, category, avg(amt) as mean_exp, (avg(amt)-2*(stddev_pop(amt))) as low_bound, (avg(amt)+2*(stddev_pop(amt))) as high_bound from htx group by cc_num, category")
    mean_per_cat.registerTempTable("mean_per_cat")

    # evaluate amount for HML and time of purchase for normal/abnormal
    test = hiveContext.sql(
    # #    "SELECT htx.cc_num, profile, htx.category, htx.trans_date,htx.trans_time, htx.unix_time,IF(htx.amt>(2 * m.mean_exp),'H',(IF(htx.amt<(0.5 * m.mean_exp),'L','N'))) as EXP, IF(htx.category like '%_net%','N','P') as CNP, htx.amt, LAG(htx.unix_time) OVER (PARTITION BY htx.cc_num ORDER BY  htx.unix_time) as lag_time from htx join mean_per_cat m on htx.cc_num=m.cc_num and m.category =htx.category")
    "SELECT htx.cc_num, profile, htx.category, htx.trans_date,htx.trans_time, htx.unix_time,IF(htx.amt>m.high_bound,'H',(IF(htx.amt < m.low_bound,'L','N'))) as EXP, IF(cast(SUBSTR(htx.trans_time,0,2) as int)<05,'A',IF(cast(SUBSTR(htx.trans_time,0,2) as int)>21,'A','N')) as NAT, htx.amt, LAG(htx.unix_time) OVER (PARTITION BY htx.cc_num ORDER BY  htx.unix_time) as lag_time from htx join mean_per_cat m on htx.cc_num=m.cc_num and m.category =htx.category")
    test.registerTempTable("full_table")

    # evaluate for transaction time (HML)
    full_data = hiveContext.sql(
         "SELECT full_table.cc_num, profile, category, trans_date, trans_time, unix_time,lag_time,IF(lag_time is null,100000,unix_time-lag_time) as time_since,amt, EXP,NAT,IF((unix_time-lag_time)<avg_time.low_bound,'H',IF((unix_time-lag_time)>avg_time.high_bound,'L','N')) as VEL from full_table left join avg_time on avg_time.cc_num = full_table.cc_num")
    full_data.registerTempTable("full_data")


    # return full tx data for user with reduced HML/AN/HML variables
    per_cust_transactions = hiveContext.sql(
        "SELECT cc_num as cust_id,concat(EXP,NAT, VEL) as trans_list from full_data order by cc_num, unix_time asc")

    # return full tx data for profile with reduced HML/NP/HML variables in sorted order
    #pre_sort_
    per_profile_transactions = hiveContext.sql(
        "SELECT profile as cust_id,concat(EXP,NAT,VEL) as trans_list from full_data order by profile, unix_time asc")
    #pre_sort_per_profile_transactions.registerTempTable("pre_sort")



    # we only need cust_id (really profile name here) and trans_list, but we had to include cc_num above in our sort
    #per_profile_transactions = hiveContext.sql("SELECT cust_id,trans_list from pre_sort")

    # gets pre-computed reference values for each customer and stores in redis
    #   avg spent per category
    #   n transactions
    #   last unix time stamp
    agg_info = hiveContext.sql(
        "SELECT CONCAT(category, '_', cc_num) as cust_id, category, concat(low_bound,',',high_bound) as low_high from mean_per_cat")
    avg_cat_data = agg_info.rdd.map(lambda x: [str(x.cust_id), str(x.low_high)])


    agg_n_tx = hiveContext.sql(
        "SELECT CONCAT('count_', cc_num) as cust_id, count(cc_num) as tx_count from full_data group by cc_num")
    n_tx = agg_n_tx.rdd.map(lambda x: [str(x.cust_id), str(x.tx_count)])

    agg_unix_ts = hiveContext.sql(
        "SELECT CONCAT('timestamp_', cc_num) as cust_id, max(unix_time) as last_unix_time from full_data group by cc_num")
    n_ts = agg_unix_ts.rdd.map(lambda x: [str(x.cust_id), str(x.last_unix_time)])

    agg_vel_info = hiveContext.sql(
        "SELECT CONCAT('velocity_', cc_num) as cust_id, concat(low_bound,',',high_bound) as low_high from avg_time")
    avg_vel_data = agg_vel_info.rdd.map(lambda x: [str(x.cust_id), str(x.low_high)])




    # compile our final string per customer for all tx's
    per_cust_transactions_r = per_cust_transactions.map(lambda p: (str(p.cust_id), str(p.trans_list))) \
        .reduceByKey(lambda y, z: y + ',' + z).map(lambda x: ''.join(x[0]) + ',' + x[1])



    # compile our final string per profile for all tx's
    per_profile_transactions_r = per_profile_transactions.map(lambda p: (str(p.cust_id), str(p.trans_list))) \
        .reduceByKey(lambda y, z: y + ',' + z).map(lambda x: ''.join(x[0]) + ',' + x[1])


    # return tx data and aggregates
    return_dict = {}
    return_dict['profile'] = per_profile_transactions_r
    return_dict['customer'] = per_cust_transactions_r

    return avg_cat_data, n_tx, n_ts, return_dict, avg_vel_data


def sparkconfig():
    # spark configuration options

    # conf = SparkConf()
    # conf.setMaster("spark://3.168.100.58:7077") # uncomment for standalone cluster
    # conf.setMaster("local")   # uncomment for local execution
    # conf.setAppName("demo_chain")
    # conf.set("spark.executor.memory", "2g")
    # conf.set("spark.default.parallelism", 56)  # 48)
    # conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
    # conf.set("sql.inMemoryColumnarStorage.batchSize",2000)

    # AMAZON AWS EMR
    conf = SparkConf()
    conf.setMaster("yarn-client")	#client gets output to terminals
    #conf.setMaster("yarn-cluster")	# this seems to runf aster but can't confirm
    conf.set("spark.default.parallelism",648)
    conf.setAppName("spark_markov_chain")
    conf.set("spark.executor.memory", "22g")
    conf.set("spark.executor.instances",9)
    conf.set("spark.executor.cores",9)
    conf.set("spark.yarn.executor.memoryOverhead",800)
    conf.set("spark.rdd.compress","True")
    conf.set("spark.shuffle.consolidateFiles","True")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    return conf

def init_redis():
    # initialize redis, wipe any existing data

	# Amazon AWS - Change Redis IP
    redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
    redis_port = 6379
    r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
    r_full = redis.StrictRedis(host=redis_ip, port=redis_port, db=1)
    r_tsm = redis.StrictRedis(host=redis_ip, port=redis_port, db=2)
    r_agg = redis.StrictRedis(host=redis_ip, port=redis_port, db=3)
    r.flushdb()
    r_full.flushdb()
    r_tsm.flushdb()
    r_agg.flushdb()
    return r, r_full, r_tsm, r_agg


def last4_pop(iter):
    # populate redis with our window of
    # last 4 transactions per customer
	
	# Amazon AWS - Change Redis IP
    redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
    redis_port = 6379
    r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
    for x in iter:
        key = x.split(",")[0]
        last_4_tx = ','.join(x.split(",")[-4:])
        r.set(key, last_4_tx)

    return


def populate_redis(r, rdddata):
    # populate redis with full record data

    for x in rdddata.collect():
        r.set(x[0], x[1])


def agg_pop(iter):
    # populate redis with aggregate data
	# Amazon AWS - Change Redis IP
    redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
    redis_port = 6379
    r_agg = redis.StrictRedis(host=redis_ip, port=redis_port, db=3)

    for x in iter:
        r_agg.set(x[0], x[1])

def agg_pop_list(iter):
    # populate redis with aggregate data returned in a list
    # matching [X,'Y,Z']
    # used for our high/lowbound data, where X is key
    # Y is lower bound and Z is higher bound
    # we populate redis with the key, then a list [Y,Z]
	# Amazon AWS - Change Redis IP
    redis_ip = 'test.h3ol1v.0001.use1.cache.amazonaws.com'
    redis_port = 6379
    r_agg = redis.StrictRedis(host=redis_ip, port=redis_port, db=3)

    for x in iter:
        key = x[0]
        value = x[1].split(',') #results in a list of [low,high]
        r_agg.set(key, cPickle.dumps(value))



# initialize timer
t0 = time.time()

# send over .py dependencies to spark workers
# sc = SparkContext(conf=sparkconfig(),
#                   pyFiles=['/home/brandon/anaconda/pkgs/toolz-0.7.2-py27_0/lib/python2.7/site-packages/toolz.zip',
#                            '/home/brandon/anaconda/pkgs/redis-py-2.10.3-py27_0/lib/python2.7/site-packages/redis.zip',
#                            '/home/brandon/anaconda/pkgs/thrift-0.9.2-py27_0/lib/python2.7/site-packages/thrift-0.9.2-py2.7-linux-x86_64.egg',
#                            '/home/brandon/anaconda/pkgs/happybase-0.9-py27_0/lib/python2.7/site-packages/happybase-0.9-py2.7.egg'])

# sc = SparkContext(conf=sparkconfig(),
#                  pyFiles=['/home/brandon/anaconda/pkgs/redis-py-2.10.3-py27_0/lib/python2.7/site-packages/redis.zip'])

# AMAZON AWS EMR
sc = SparkContext(conf=sparkconfig(),
                 pyFiles=['/home/hadoop/sparkov/redis.zip'])


# Generate all possible transactions letters
# combos = map(lambda x: "".join(concat(x)),list(product(product("HNL", "PN", "HNL"))))
# ENCODE THEM 0-17 (for 18x18 matrix)
# coding = {j: i for i, j in enumerate(combos)}
# Hardcode Coding
#coding = {'NPL': 8, 'NPN': 7, 'NPH': 6, 'NNH': 9, 'HNH': 3, 'LPH': 12, 'HNL': 5, 'LNN': 16, 'HNN': 4, 'NNL': 11,
#          'LPL': 14, 'HPH': 0, 'HPN': 1, 'LPN': 13, 'HPL': 2, 'NNN': 10, 'LNL': 17, 'LNH': 15}
coding = {'HAH': 0, 'HAL': 2, 'HAN': 1, 'HNH': 3, 'HNL': 5, 'HNN': 4, 'LAH': 12, 'LAL': 14, 'LAN': 13, 'LNH': 15,
          'LNL': 17, 'LNN': 16, 'NAH': 6, 'NAL': 8, 'NAN': 7, 'NNH': 9, 'NNL': 11, 'NNN': 10}

# broadcast sequence coding to workers
sc.broadcast(coding)

r, r_full, r_tsm, r_agg = init_redis()

agg_exp_info, agg_n_tx, agg_unix_ts, a, vel_info = load_data()
cust_tx = a['customer']
profile_tx = a['profile']

cust_tx.foreachPartition(last4_pop)
profile_tx.foreachPartition(last4_pop)


# send aggregates to redis
agg_unix_ts.foreachPartition(agg_pop)
agg_n_tx.foreachPartition(agg_pop)
agg_exp_info.foreachPartition(agg_pop_list)
vel_info.foreachPartition(agg_pop_list)

cust_tx.map(process).foreachPartition(store_partition)
profile_tx.map(process).foreachPartition(store_partition)


# DEBUG CODE
user_ccnum = '3088076288546889'
print('**'*10+'USER: ')
print cPickle.loads(r_tsm.get(user_ccnum))
print('**'*10+'PROFILE: ')
print cPickle.loads(r_tsm.get('young_adults_female_urban.json'))
print('**'*10+'AVERAGE PER CAT: ')
print cPickle.loads(r_agg.get('shopping_pos_'+user_ccnum))
print('**'*10+'NUM OF TX: ')
print r_agg.get('count_'+user_ccnum)
print('**'*10+'UNIX TS: ')
print r_agg.get('timestamp_'+user_ccnum)
print('**'*10+'LAST 4 TX USER: ')
print r.get(user_ccnum)
print('**'*10+'LAST 4 TX PROFILE: ')
print r.get('young_adults_male_rural.json')

t1 = time.time()
print('Time Elapsed: ' + str(t1 - t0))

