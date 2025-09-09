from confluent_kafka.admin import AdminClient,NewTopic
class Admin:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.admin=AdminClient({"bootstrap.servers": self.bootstrap_servers})
    def topic_exist(self,topic):
        all_topics=self.admin.list_topics().topics
        print(all_topics)
        if topic in all_topics:
            return True
        else:
            return False

    def create_topic(self,topic):
        if self.topic_exist(topic):
            print("Topic already exists")
        else:
            new_topic=NewTopic(topic,num_partitions=7)
            print(new_topic)
            fs=self.admin.create_topics([new_topic])
            print(f"Creating topic {new_topic}")


        
        