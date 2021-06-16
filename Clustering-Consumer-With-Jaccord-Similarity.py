import json
import pandas as pd
from kafka import KafkaConsumer, cluster
from pymongo import MongoClient
import math
import Levenshtein

def jaccard_similarity(list1, list2):
    s1 = set(list1)
    s2 = set(list2)
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))

def main():

    print(" Starting Clustering Consumer With Jaccord Similarity....")

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
        col_a = streaming_data.value["I_like_movies"]
        col_b = streaming_data.value["I_like_watching_movie"]

        sentence = col_a + " " + col_b
        
        if u_id not in ids:
            ids.append(u_id)


            min_edit_distance = math.inf
            edit_distance_computed = False

            for s_d in streamed_data:
                
                # Jaccord Similarity
                j_sim = jaccard_similarity(sentence.split(), s_d.split())
                if j_sim < 0.75:
                    continue
                

                edit_distance_computed = True
                # Edit Distance
                dist =  Levenshtein.distance(sentence, s_d)
                if dist < min_edit_distance:
                    min_edit_distance = dist

            group = "f"    
            if edit_distance_computed:
                
                print(f"Min-Edit-Distance for current sentence with other sentence = {min_edit_distance}")
                for c in clusters:
                    if min_edit_distance >= clusters[c][0] and min_edit_distance < clusters[c][1]:
                        group = c 
                        break
            else:
                print(f"Edit Distance Not Computed, Since Jaccord-Was High")
            
            m_client.BDA.edit_distance_sim_with_jaccord.insert_one({"group" : group})
            streamed_data.append(sentence)
            
     

if __name__ == "__main__":
    main()