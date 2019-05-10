from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import binascii
import datetime
import os
import sys

def updateFunc (new_value, last_state):
    # print(new_value,'~~~~~~~~~~~~~')
    # last_state is a dict
    int_city = int(binascii.hexlify(new_value[1].encode('utf8')), 16)
    hash_result = hashFunc(int_city)
    # print(last_state)

    temp_bloom = last_state['bloom_array']
    # print(temp_bloom)
    temp_true_city = last_state['true_city']
    if (temp_bloom[hash_result[0]] == 1) and (temp_bloom[hash_result[1]] ==1) and (int_city not in temp_true_city):
        last_state['FP_count'] = last_state['FP_count']+1
    else:
        last_state['TN_count'] = last_state['TN_count']+1
    last_state['true_city'].add(int_city)
    last_state['bloom_array'][hash_result[0]] =1
    last_state['bloom_array'][hash_result[1]] =1

    FPR = last_state['FP_count']/(last_state['FP_count']+last_state['TN_count'])
    with open(output_name, 'a') as f:
        f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+ "," + str(FPR)+"\n")
    return last_state

def hashFunc (int_city):
    h1 = (337 * int_city + 541) % 200
    h2 = (571*int_city+613)%200
    return (int(h1), int(h2))


# port_num = 9999
port_num = int(sys.argv[1])
# output_name = "task1.csv"
output_name = sys.argv[2]
output = open(output_name, 'w')
output.write("Time, FPR\n")
output.flush()
os.fsync(output.fileno())

sc = SparkContext("local[2]", "hw6")
ssc = StreamingContext(sc, 10)
ssc.checkpoint('checkpoint')

initialStateRDD = sc.parallelize([('temp_key', {"bloom_array": [0]*200, 'true_city':set(), "TN_count":0, "FP_count":0})])
lines = ssc.socketTextStream("localhost", port_num)
key_city = lines.map(lambda x: ('temp_key', json.loads(x)['city'])).updateStateByKey(updateFunc ,initialRDD=initialStateRDD)
key_city.pprint()
ssc.start()
ssc.awaitTermination()



