import json
import pandas as pd
from kafka import KafkaConsumer, cluster
from pymongo import MongoClient
import math
import Levenshtein

def main():

    print(" Starting Clustering Consumer....")

    topic_name = "pokec_user_data_stream"  
    k_consumer_a = KafkaConsumer(topic_name,
                                 bootstrap_servers=["localhost:9092"],
                                 auto_offset_reset="earliest",
                                 value_deserializer=lambda x: json.loads(x.decode("utf-8")))

    m_client = MongoClient()
    ids = []
    streamed_data = []
    clusters = {"a" : [0, 5] ,"b" : [5, 10],"c" : [10, 15],"d" : [15, 20] , "e" : [20, math.inf]}
   
    for streaming_data in k_consumer_a:
        
        u_id =  streaming_data.value["user_id"]
        gender = streaming_data.value["GenderMF"]
        smoke_status = streaming_data.value["SmokingStatus"]

        
        if u_id not in ids:
            ids.append(u_id)

            min_edit_distance = math.inf
            for s_d in streamed_data:
                
                dis_a = Levenshtein.distance(s_d["GenderMF"], gender)
                dis_b = Levenshtein.distance(s_d["SmokingStatus"], smoke_status)

                if dis_a + dis_b < min_edit_distance:
                    min_edit_distance = dis_a + dis_b


            print(f"Min Edit Distance = {min_edit_distance}")
            group = None
            for c in clusters:
                if min_edit_distance >= clusters[c][0] and min_edit_distance < clusters[c][1]:
                    group = c 
                    break
            
            print(group)
            streamed_data.append({"GenderMF": gender, "SmokingStatus": smoke_status})
            
     

if __name__ == "__main__":
    main()