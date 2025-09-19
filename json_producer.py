import json
from producer import ProducerClass
from admin import Admin as admin
class JSONProducer(ProducerClass):
    def __init__(self,bootstrap_servers, topic):
        super().__init__(bootstrap_servers, topic)
    def send_message(self, message):
        try:
            self.producer_object.produce(self.topic, message)
        except Exception as e:
            print(f"Error sending message: {e}")
        return True

bootstrap_servers = "localhost:19092"
topic = "invertry"
a=admin(bootstrap_servers)
a.create_topic(topic)
p=JSONProducer(bootstrap_servers, topic)
while True:
    first_name = input("Enter first name: ")
    last_name = input("Enter last name: ")
    age = int(input("Enter age: "))
    city = input("Enter city: ")
    data = {
        "first_name": first_name,
        "last_name": last_name,
        "age": age,
        "city": city
    }
    json_data = json.dumps(data).encode('utf-8')
    p.send_message(json_data)
    
    
    
    p.commit()
    print("message sent successfully")