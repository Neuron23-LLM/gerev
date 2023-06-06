import threading
from dataclasses import dataclass
from typing import List
from queue import Queue

from data_source.api.basic_document import BasicDocument

@dataclass
class IndexQueueItem:
    queue_item_id: int
    doc: BasicDocument

class IndexQueue:
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def __init__(self):
        if IndexQueue._instance is not None:
            raise RuntimeError("IndexQueue is a singleton, use .get_instance() to get the instance")

        self.condition = threading.Condition()
        self.queue = Queue()

    def put_single(self, doc: BasicDocument):
        self.put([doc])

    def put(self, docs: List[BasicDocument]):
        with self.condition:
            for doc in docs:
                self.queue.put(doc)

            self.condition.notify_all()
    
    def consume_all(self, max_docs=5000, timeout=1) -> List[IndexQueueItem]:
        with self.condition:
            self.condition.wait(timeout=timeout)

            queue_items = []
            count = 0
            while not self.queue.empty() and count < max_docs:
                doc = self.queue.get()
                queue_items.append(IndexQueueItem(queue_item_id=-1, doc=doc))
                count += 1

            return queue_items

    def qsize(self):
        return self.queue.qsize()
