import importlib
from webhook.redis import RedisClient

class WorkerFactory:
    def __init__(self, **params):
        self.params = params
        worker_class_str = params["worker_class"]
        mod = importlib.import_module(f"..webhook_workers", self.__class__.__module__)
        self.worker_class = getattr(mod, worker_class_str)

    def create(self, pool):
        return self.worker_class(pool=pool, **self.params)

class WorkersPool:
    def __init__(self, factory: WorkerFactory, num_workers: int):
        self.factory = factory
        self.num_workers = num_workers

        self.workers = [factory.create(self) for _ in range(self.num_workers)]

    def start(self):
        for worker in self.workers:
            # ToDo: assign an new process to each worker
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.stop()


class Worker:
    def __init__(self, pool: WorkersPool, **params):
        self.pool = pool
        self.should_shutdown = False

        self.redis_url = params["redis_url"]
        self.redis_db = params["redis_db"]
        self.redis_client = RedisClient(self.redis_url, self.redis_db)

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()