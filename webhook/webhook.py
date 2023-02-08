
import time
from datetime import datetime
from typing import Dict


class KafkaServer:
    pass


class KafkaClient:
    pass


class Redis:
    pass


class HTTPClient:
    pass


class WorkersPool():
    def __init__(self, factory: WorkerFactory, num_workers: int, **params):
        self.factory = factory
        self.num_workers = num_workers

        self.workers = [factory.create(self, **params) for _ in range(self.num_workers)]

    def start(self):
        for worker in self.workers:
            # ToDo: assign an new process to each worker
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.stop()


class Worker:
    def __init__(self, pool: WorkerPool, **params):
        self.pool = pool
        self.should_shutdown = False

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class WorkerFactory:
    def __init__(self, **params):
        pass

    def create(self, **params):
        pass


class FromSenderWorker(Worker):

    def __init__(self, pool, **params):
        super().__init__(pool, **params)
        self.kafka_inbound_topic = params["kafka_inbound_topic"]
        self.kafka_outbound_topic = params["kafka_outbound_topic"]
        self.kafka_url = params["kafka_url"]
        self.kafka_consumer = KafkaClient(self.kafka_url, self.kafka_inbound_topic)
        self.kafka_producer = KafkaClient(self.kafka_url, self.kafka_outbound_topic)

        self.redis_url = params["redis_url"]
        self.redis_db = params["redis_db"]
        self.redis_client = Redis(self.redis_url, self.redis_db)

    def start(self):
        while not self.should_shutdown:
            try:
                payload = self.kafka_consumer.recieve()

                source_id = payload["source_id"]
                message_id = payload["message_id"]
                send_to = payload["send_to"]
                # message = payload["message"]

                if self.is_duplicate(source_id, message_id):
                    self.kafka_consumer.commit()
                    continue

                self.register_message(source_id, message_id, payload)
                self.kafka_producer.send({
                    "source_id": source_id,
                    "message_id": message_id,
                    "send_to": send_to
                })
                self.kafka_consumer.commit()
            except:  # Fixme, too broad
                pass

    def stop(self):
        pass

    def register_message(self, source_id, message_id, payload):

        # fixme: encode paylaod
        self.redis_client.add(db="messages", key=(source_id, message_id), value=payload)

    def is_duplicate(self, source_id, message_id):

        # fixme: handle the case when messages are move to seconadary storage
        #        due to space constraints
        return self.redis_client.contains(db="messages", key=(source_id, message_id))


class ToReceiverWorker(Worker):

    MESSAGE_TRACKING_DB = "message_tracking"

    def __init__(self, pool, **params):
        super().__init__(pool, **params)

        self.kafka_inbound_topic = params["kafka_inbound_topic"]
        self.kafka_url = params["kafka_url"]
        self.kafka_consumer = KafkaClient(self.kafka_url, self.kafka_inbound_topic)

        # ToDo: this is the queue that gets message back from reciever. Name this correctly
        self.kafka_producer = KafkaClient(self.kafka_url, self.kafka_outbound_topic)

        # ToDo: this is the queue that checks status and fetches message async
        # self.kafka_producer = KafkaClient(self.kafka_url, self.kafka_outbound_topic)

        self.redis_url = params["redis_url"]
        self.redis_db = params["redis_db"]
        self.redis_client = Redis(self.redis_url, self.redis_db)

    def start(self):
        while not self.should_shutdown:
            try:
                payload = self.kafka_consumer.recieve()

                source_id = payload["source_id"]
                message_id = payload["message_id"]
                send_to = payload["send_to"]

                while not self.can_send_to_receiver(source_id, message_id, send_to):
                    # ToDo: Cannot loop forever; Implement timeout
                    time.sleep(1)

                response = self.send_message(payload)
                if not self.reciever_is_async(send_to):
                    self.kafka_producer.send(response)
                else:
                    # ToDo: if need to check status async then send message to another queue / worker
                    #       to ping the receiver async
                    pass

                # ToDo: check if there is error in response
                self.kafka_consumer.commit()
            except:  # ToDo: too broad
                pass

    def stop(self):
        pass

    def send_message(self, payload):
        source_id = payload["source_id"]
        message_id = payload["message_id"]
        send_to = payload["send_to"]

        # assume http post on port 80 for now
        client = HTTPClient(send_to, port=80)
        msg = self.get_message((source_id, message_id))

        # ToDo:
        # 1. Update message status that it is sent to receiver
        # 2. Update message tracking db

        try:
            num_messages, last_sent = self.redis_client.get(db=MESSAGE_TRACKING_DB, key=receiver)
            now = int(time.time())
            self.redis_client.set(db=MESSAGE_TRACKING_DB, key=receiver, value=(num_messages + 1, now))
            response = client.post(msg)
        except:  # If post failed
            num_messages, last_sent = self.redis_client.get(db=MESSAGE_TRACKING_DB, key=receiver)
            self.redis_client.set(db=MESSAGE_TRACKING_DB, key=receiver, value=(num_messages -1, last_sent))
            raise

        return response

    def should_throttle(self, receiver):
        config = self.redis_client.get(db="receiver_config", key=receiver)
        rate = config["rate"]  # msg / second
        window = config["window"]  # window_length

        now = int(time.time())
        num_messages, last_sent = self.redis_client.get(db=MESSAGE_TRACKING_DB, key=receiver)
        if last_sent < now < last_sent + window and num_messages == rate:  # ToDo: double check
            return False
        return True

    def can_send_to_receiver(self, source_id, message_id, send_to):
        if self.should_throttle(send_to):
            return False

        return True


class FromReceiverAsyncWorker(Worker):
    """
    Recieve message from reciever in async fashion
    Gets response from an earlier post and pushes the response to backward direction
    queue of "from_receiver_queue"
    """

    def __init__(selfpool, **params):
        super().__init__(pool, **params)
        # ToDo

    def start(self):
        pass

    def stop(self):
        pass

class WebHookProxy:

    def __init__(self):
        # self.from_sender_queue = KafkaServer()
        # self.to_receiver_queue = KafkaServer()
        # self.from_receiver_queue = KafkaServer()
        # self.fetch_receiver_async_queue = KafkaServer()

        # self.messages_db = Redis()
        # self.receiver_config = Redis()
        # self.message_tracking = Redis()

        # Workers to:
        # 1. Receive message from sender (github) via "from_sender_queue" kafa topic,
        # 2. process it and
        # 3. send to "to_receiver_queue" kafka topic
        self.from_sender_workers = WorkersPool(WorkerFactory(worker_class="FromSenderWorker"), num_workers=10)

        # Workers to:
        # 1. Receive message from "to_receiver_queue" kafka topic
        # 2. Process it and
        # 3. Post to receiver (example.com)
        # 4. If receiver repsonds synchronously then push the message to "from_receiver_queue" kafka topic
        #    otherwise send message to "fetch_receiver_async_queue" to ask another worker to fetch async
        self.to_receiver_workers = WorkersPool(WorkerFactory(worker_class="ToReceiverWorker"), num_workers=10)

        # Workers to:
        # 1. Receive message from reciever in an async fashion kafka topic
        # 2. Push recieved message to "from_receiver_queue"
        self.from_receiver_async_workers = WorkersPool(WorkerFactory(worker_class="FromReceiverAsyncWorker"), num_workers=10)

        # Workers to:
        # 1. Receive message from "from_receiver_queue"
        # 2. Process it
        # 3. Send message to sender (github)
        self.from_receiver_workers = WorkersPool(WorkerFactory(worker_class="FromReceiverWorker"), num_workers=10)

    def start(self):
        self.from_sender_workers.start()
        self.to_receiver_workers.start()
        self.from_receiver_workers.start()

    def stop(self):
        self.from_sender_workers.stop()
        self.to_receiver_workers.stop()
        self.from_receiver_workers.stop()




