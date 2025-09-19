from confluent_kafka import Consumer
import json


class Jsonconsumer:
    def __init__(self,bootstrap_servers, group_id, topic):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer({"bootstrap.servers": self.bootstrap_servers,"group.id": self.group_id} )
        self.consumer.subscribe([self.topic])


    def read_message(self):
        while True:
            msg=self.consumer.poll(1.0) #timeout
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            data=json.loads(msg.value().decode('utf-8')) #json to dict
            print(f"Received message: {data}")
    
cosumer=Jsonconsumer("localhost:19092","my_group","yesdeepa")
cosumer.read_message()
