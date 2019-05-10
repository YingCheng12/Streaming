from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import binascii
import statistics
import os
import datetime
import sys

def updateFunc(new_value, last_state):
    # print(last_state)
    # print(new_value)

    last_state['city'].append(new_value)



    # print(type(new_value))
    if len(last_state['city'])==8:
        last_state['city'].pop(0)
        last_state['city'].pop(0)

    estimate_value_list = []
    cities = []
    if len(last_state['city'])%2==0:
        for i in last_state['city']:
            for j in i:
                cities.append(j)
                R = find_0(j)
                estimate_value_list.append(R)

        temp_output = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+","+str(len(set(cities))) +',' +str(max(estimate_value_list))
        with open(output_name, 'a') as f:
            f.write(temp_output+'\n')

    return last_state


def find_0(int_city):
    a_list=[271,277,281,283,293,307,311,313,317,331,337,347,349,353,359,367,541,547,557,563,569,571,577,587,593,599,601,607,613,617,619,631,641,643,647,653,659,661,673,677,929,937,941,947,953,967]
    hash_result = []
    for i in range(0, 30):
        a = a_list[i]
        b = a_list[i+1]
        # m = 200
        temp = (a * int_city + b) % 130
        temp_binary = bin(int(temp))[2:]
        # print(temp_binary)
        temp_r = len(temp_binary) - len(temp_binary.rstrip('0'))
        temp_r_start = 2**temp_r
        hash_result.append(temp_r_start)
    step = 2
    small_group = [hash_result[i:i+step] for i in range(0,len(hash_result),step)]
    ave_list = []
    for i in small_group:
        temp_ave = sum(i)/len(i)
        ave_list.append(temp_ave)
    # print(len(ave_list))
    # print(sorted(ave_list))
    return sorted(ave_list)[-2]



# port_num = 9999
port_num = int(sys.argv[1])
# output_name = "task2.csv"
output_name = sys.argv[2]
output = open(output_name, 'w')
output.write("Time, Ground Truth, Estimation\n")
output.flush()
os.fsync(output.fileno())

sc = SparkContext("local[2]", "hw6")
ssc = StreamingContext(sc, 5)
ssc.checkpoint('checkpoint')

initialStateRDD = sc.parallelize([('temp_key', {'city':[]})])
lines = ssc.socketTextStream("localhost", port_num)
key_city = lines.map(lambda x: ('temp_key', int(binascii.hexlify(json.loads(x)['city'].encode('utf8')),16))).updateStateByKey(updateFunc,initialRDD=initialStateRDD)
key_city.pprint()
ssc.start()
ssc.awaitTermination()

# int(binascii.hexlify(new_value[1].encode('utf8')), 16)