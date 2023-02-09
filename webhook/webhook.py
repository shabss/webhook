
import importlib
import time
import sys
from datetime import datetime
from typing import Dict, ForwardRef

from webhook.worker_base import (
    WorkersPool,
    WorkerFactory
)

class WebHookProxy:

    def __init__(self):
        # self.from_sender_queue = KafkaTopic()
        # self.to_receiver_queue = KafkaTopic()
        # self.from_receiver_queue = KafkaTopic()
        # self.fetch_receiver_async_queue = KafkaTopic()

        # self.messages_db = Redis()
        # self.receiver_config = Redis()
        # self.message_tracking = Redis()

        # Workers to:
        # 1. Receive message from sender (github) via "from_sender_queue" kafa topic,
        # 2. process it and
        # 3. send to "to_receiver_queue" kafka topic
        self.from_sender_workers = WorkersPool(WorkerFactory(
            worker_class="FromSenderWorker",
            kafka_inbound_topic="from_sender_queue",
            kafka_outbound_topic="to_receiver_queue",
            kafka_url="kafka_url_not_needed_at_this_time",
            redis_url="redis_url_not_needed_at_this_time",
            redis_db="webhook"
        ), num_workers=10)

        # Workers to:
        # 1. Receive message from "to_receiver_queue" kafka topic
        # 2. Process it and
        # 3. Post to receiver (example.com)
        # 4. If receiver repsonds synchronously then push the message to "from_receiver_queue" kafka topic
        #    otherwise send message to "fetch_receiver_async_queue" to ask another worker to fetch async
        self.to_receiver_workers = WorkersPool(WorkerFactory(
            worker_class="ToReceiverWorker",
            kafka_inbound_topic="to_receiver_queue",
            kafka_outbound_topic="from_receiver_queue",
            kafka_url="kafka_url_not_needed_at_this_time",
            redis_url="redis_url_not_needed_at_this_time",
            redis_db="webhook"
        ), num_workers=10)

        # Workers to:
        # 1. Receive message from receiver in an async fashion kafka topic
        # 2. Push received message to "from_receiver_queue"
        self.from_receiver_async_workers = WorkersPool(
            WorkerFactory(
                worker_class="ReceiverStatusAsyncWorker",
                kafka_outbound_topic="from_receiver_queue",
                redis_url = "redis_url_not_needed_at_this_time",
                redis_db = "webhook"
            ), num_workers=10)

        # Workers to:
        # 1. Receive message from "from_receiver_queue"
        # 2. Process it
        # 3. Send message to sender (github)
        self.from_receiver_workers = WorkersPool(
            WorkerFactory(
                original_sender="original_sender",
                worker_class="ToSenderWorker",
                kafka_inbound_topic="from_receiver_queue",
                kafka_url="kafka_url_not_needed_at_this_time",
                redis_url="redis_url_not_needed_at_this_time",
                redis_db="webhook"
            ), num_workers=10)

    def start(self):
        self.from_sender_workers.start()
        self.to_receiver_workers.start()
        self.from_receiver_async_workers.start()
        self.from_receiver_workers.start()

    def stop(self):
        self.from_sender_workers.stop()
        self.to_receiver_workers.stop()
        self.from_receiver_workers.stop()


