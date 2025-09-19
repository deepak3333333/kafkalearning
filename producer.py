from confluent_kafka import Producer
from admin import Admin
class ProducerClass:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_object = Producer({"bootstrap.servers": self.bootstrap_servers})

    def send_message(self, message):
        try:
            self.producer_object.produce(self.topic, message)
        except Exception as e:
            print(f"Error sending message: {e}")

    def commit(self):
        self.producer_object.flush()


bootstrap_servers = "localhost:19092"
topic = "kumar"
a = Admin(bootstrap_servers)
a.create_topic(topic)
send_data = ProducerClass(bootstrap_servers, topic)


try:
    while True:
        message = input("Enter message to send: ")
        send_data.send_message(message)

except KeyboardInterrupt:
    send_data.commit()
    print("Message sent successfully")



# import json
# serilize_data = json.dumps({"name": "kumar", "age": 24}).encode('utf-8')