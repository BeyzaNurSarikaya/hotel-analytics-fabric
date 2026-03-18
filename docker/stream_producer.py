import pandas as pd
import json
import time
import os
from azure.eventhub import EventHubProducerClient, EventData

# We are retrieving the information from environment variables.
CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
FILE_PATH = "hotel_raw_stream.csv"

def send_events():
    if not CONNECTION_STR or not EVENTHUB_NAME:
        print("❌ Error: Connection details are missing! Please check your environment variables.")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, 
        eventhub_name=EVENTHUB_NAME
    )
    
    df = pd.read_csv(FILE_PATH)
    # Cleaning: There may be corrupted rows in the stream data; let's make NaN values empty strings.
    df = df.fillna("") 

    print(f"🚀 Stream started: Data is being sent to the {EVENTHUB_NAME} channel...")

    with producer:
        for index, row in df.iterrows():
            data = row.to_dict()
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(data)))
            
            producer.send_batch(event_data_batch)
            
            if index % 50 == 0:
                print(f"📤 Total sent: {index} lines")
            
            # 100 records per second
            time.sleep(0.01)

if __name__ == "__main__":
    send_events()