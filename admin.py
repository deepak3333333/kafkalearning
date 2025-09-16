from confluent_kafka.admin import AdminClient, NewTopic

class Admin:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def topic_exist(self, topic):
        all_topics = self.admin.list_topics().topics
        return topic in all_topics
       # or
        # def topic_exist(self,topic):
        #      all_topics=self.admin.list_topics().topics 
        #      print(all_topics) 
        #      if topic in all_topics:
        #          return True 
        #      else:
        #          return False

    def create_topic(self, topic):
        if self.topic_exist(topic):
            print(f"Topic '{topic}' already exists")
            return

        new_topic = NewTopic(topic, num_partitions=7, replication_factor=1)
        fs = self.admin.create_topics([new_topic])
        print(fs)
        print(fs.items())

        for t, f in fs.items():
            try:
                f.result()  # raises exception if creation failed
                print(f"Topic '{t}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{t}': {e}")
