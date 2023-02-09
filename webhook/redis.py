
class RedisClient:

    NAMESPACES = {}
    def __init__(self, url, namespace):
        self.namespace = namespace
        self.NAMESPACES[namespace] = {}

    def set(self, db, key, value) -> None:
        namespace = self.NAMESPACES[self.namespace]
        db_dict = namespace.get(db, None)
        if db_dict is None:
            db_dict = namespace[db] = {}

        db_dict[key] = value

    def get(self, db, key):
        namespace = self.NAMESPACES[self.namespace]
        value = namespace[db][key]
        return value

    def contains(self, db, key) -> bool:
        namespace = self.NAMESPACES[self.namespace]
        db_dict = namespace.get(db, None)
        if db_dict is None:
            return False
        return key in db_dict

