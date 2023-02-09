
import time

class KafkaServer:

    def __init__(self):
        self.topics = {}        # topic_name --> messages
        self.consumers = {}     # consumer_name --> topic, pointer

    def create_topic(self, topic, partitions):

        # ToDo: create seperate queues for partitions
        self.topics[topic] = []

    def get_message(self, consumer_name):
        topic, ptr = self.consumers.get(consumer_name, (None, 0))
        while ptr >= len(self.topics.get(topic, [])):
            time.sleep(1)

        msg = self.topics[self.topic][ptr]
        return msg

    def commit_message(self, consumer_name):
        topic, ptr = self.consumers.get(consumer_name, (None, 0))
        if topic is None:
            return

        ptr += 1
        self.consumers[consumer_name] = self.topic, ptr

    def send_message(self, topic, msg):
        self.topics[topic].append(msg)


g_kafka_server = KafkaServer()

class KafkaConsumer:

    TOTAL_CLIENTS = 0
    def __init__(self, server_url, topic):
        self.name = f"consumer{self.TOTAL_CLIENTS}"
        self.TOTAL_CLIENTS += 1

        self.topic = topic
        self.kafka_server = g_kafka_server

    def receive(self):
        msg = self.kafka_server.get_message(self.name)
        return msg

    def commit(self):
        self.kafka_server.commit_message(self.name)


class KafkaProducer:
    def __init__(self, server_url, topic):
        self.topic = topic
        self.kafka_server = g_kafka_server

    def send(self, msg):
        self.kafka_server.send_message(self.topic, msg)


