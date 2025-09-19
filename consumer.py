from confluent_kafka import Consumer
class ConsumerClass:
    def __init__(self, topic_address, group_id, topic):
        self.topic_address = topic_address
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer({"bootstrap.servers": self.topic_address,"group.id": self.group_id} )

    def consume_message(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(10.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                print(f"Received message: {msg.value()}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


bootstrap_servers = "localhost:19092"
topic = "kumar"
group_id = "my_group"
Consumer=ConsumerClass(topic_address=bootstrap_servers, group_id=group_id, topic=topic)
Consumer.consume_message()

