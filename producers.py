import json
import pandas as pd 
from kafka import KafkaProducer
import threading
import time

def producer(topic_name, file_a_uri):

    # Loading data 
    df_pokec_a = pd.read_csv(file_a_uri)


    k_producer_a = KafkaProducer(bootstrap_servers=["localhost:9092"],
                               value_serializer=lambda x: json.dumps(x).encode("utf-8")                              )

     # Start Streaming data to apache-kafka
    num_records_to_send  = 50
    i = 1
    for data_tuple in df_pokec_a.itertuples():

        obj_to_stream = {}
        for col_i in range(df_pokec_a.columns.shape[0]):
            obj_to_stream[df_pokec_a.columns[col_i]] = data_tuple[col_i+1]
        
        k_producer_a.send(topic=topic_name,value=obj_to_stream)

        if i == num_records_to_send:
            break
        
        time.sleep(0.5)

def main():

    topic_name = "pokec_user_data_stream"     

    # For Main machine
    system_root = "/home/djzaamir/Desktop/BDA-Data/Cleaned-Data"
    files = [system_root + "/pokec_chunk_a.csv",
             system_root + "/pokec_chunk_b.csv",
             system_root + "/pokec_chunk_c.csv"]
    
    _threads = []
    for file in files:
        print(f"Streaming = {file}")
        t = threading.Thread(target=producer, args=(topic_name, file))
        t.start()
        _threads.append(t)

    for t in _threads:
        t.join()

if __name__ == "__main__":
    main()