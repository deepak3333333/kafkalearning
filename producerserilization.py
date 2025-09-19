from confluent_kafka import Producer
import json
class JsonProducer:
    def __init__(self,bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_object = Producer({"bootstrap.servers": self.bootstrap_servers})


    def send_message(self, message):
        # convert dict to json
        json_message = json.dumps(message)
        self.producer_object.produce(self.topic, json_message)
        return True
producer=JsonProducer("localhost:19092","yesdeepa")
data={"name":"kumar","age":24,"city":"bangalore"}


success=producer.send_message(data)

if success:
    print("message sent successfully")
else:
    print("message not sent")
producer.producer_object.flush()


