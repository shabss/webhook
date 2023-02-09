
import time

from webhook.common import MESSAGES_DB, MESSAGE_TRACKING_DB, RECEIVER_CONFIG_DB, HTTPClient
from webhook.kafka import KafkaConsumer, KafkaProducer
from webhook.redis import RedisClient
from webhook.worker_base import Worker

class FromSenderWorker(Worker):

    def __init__(self, pool, **params):
        super().__init__(pool, **params)
        self.kafka_inbound_topic = params["kafka_inbound_topic"]
        self.kafka_outbound_topic = params["kafka_outbound_topic"]
        self.kafka_url = params["kafka_url"]
        self.kafka_consumer = KafkaConsumer(server_url=self.kafka_url, topic=self.kafka_inbound_topic)
        self.kafka_producer = KafkaProducer(server_url=self.kafka_url, topic=self.kafka_outbound_topic)

    def start(self):
        while not self.should_shutdown:
            try:
                payload = self.kafka_consumer.receive()

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
            except Exception as ex:  # Fixme, too broad
                print(f"Exception: {self.__class__}: {ex}")

    def stop(self):
        pass

    def register_message(self, source_id, message_id, payload):

        # fixme: encode the paylaod
        # fixme: add message status
        self.redis_client.add(db=MESSAGES_DB, key=(source_id, message_id), value=payload)

    def is_duplicate(self, source_id, message_id):

        # fixme: handle the case when messages are move to secondary storage
        #        due to space constraints
        return self.redis_client.contains(db=MESSAGES_DB, key=(source_id, message_id))


class ToSenderWorker(Worker):
    def __init__(self, pool, **params):
        super().__init__(pool, **params)
        self.kafka_inbound_topic = params["kafka_inbound_topic"]
        self.kafka_url = params["kafka_url"]
        self.kafka_consumer = KafkaConsumer(self.kafka_url, self.kafka_inbound_topic)
        self.original_sender = params["original_sender"]

    def start(self):
        while not self.should_shutdown:
            payload = None
            result = None
            try:
                payload = self.kafka_consumer.receive()
                message = payload["message"]
                success, result = self.send_message(message)
                if not success:
                    raise RuntimeError("sending messages to original sender was unsuccessful")

                # Todo: perform message tracking stuff

                self.kafka_consumer.commit()
            except Exception as ex:
                # ToDo: report error here also
                self.report_error(payload, result, ex)


    def send_message(self, message):
        client = HTTPClient(self.original_sender, 80)
        result = client.post(message)
        if result["code"] == 200:
            return True, {}
        else:
            return False, result

    def report_error(self, payload, result, exception):
        # toDo: 1) send to sentry 2) move this function to base class
        pass


class ToReceiverWorker(Worker):

    def __init__(self, pool, **params):
        super().__init__(pool, **params)

        self.kafka_inbound_topic = params["kafka_inbound_topic"]
        self.kafka_url = params["kafka_url"]
        self.kafka_consumer = KafkaConsumer(self.kafka_url, self.kafka_inbound_topic)

        # ToDo: this is the queue that gets message back from receiver. Name this correctly
        self.kafka_outbound_topic = params["kafka_outbound_topic"]
        self.kafka_producer = KafkaProducer(self.kafka_url, self.kafka_outbound_topic)

        # ToDo: this is the queue that checks status and fetches message async
        # self.kafka_producer = KafkaClient(self.kafka_url, self.kafka_outbound_topic)

    def start(self):
        while not self.should_shutdown:
            try:
                payload = self.kafka_consumer.receive()

                source_id = payload["source_id"]
                message_id = payload["message_id"]
                send_to = payload["send_to"]

                while not self.can_send_to_receiver(source_id, message_id, send_to):
                    # ToDo: Cannot loop forever; Implement timeout
                    time.sleep(1)

                response = self.send_message(payload)
                if not self.is_receiver_async(send_to):
                    self.kafka_producer.send(response)
                else:
                    # ToDo: if need to check status async then send message to another queue / worker
                    #       to ping the receiver async
                    pass

                # ToDo: check if there is error in response
                self.kafka_consumer.commit()
            except Exception as ex:  # ToDo: too broad
                print(f"Exception: {self.__class__}: {ex}")

    def stop(self):
        pass

    def is_receiver_async(self, receiver):

        # ToDo: look at config and decide
        return False

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
        #    a) update last sent timestamp and increment num sent
        #    b) setup a TTL on that message, to trigger retries

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
        config = self.redis_client.get(db=RECEIVER_CONFIG_DB, key=receiver)
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
    Receive message from receiver in async fashion
    Gets response from an earlier post and pushes the response to backward direction
    queue of "from_receiver_queue"
    """

    def __init__(self, pool, **params):
        super().__init__(pool, **params)
        # ToDo

    def start(self):
        pass

    def stop(self):
        pass