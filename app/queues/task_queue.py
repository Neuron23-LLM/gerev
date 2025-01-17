import threading
from dataclasses import dataclass
from typing import Optional
from queue import Queue, Empty

from paths import SQLITE_TASKS_PATH


@dataclass
class Task:
    data_source_id: int
    function_name: str
    kwargs: dict
    attempts: int = 3


@dataclass
class TaskQueueItem:
    queue_item_id: int
    task: Task


class TaskQueue:
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def __init__(self):
        if TaskQueue._instance is not None:
            raise RuntimeError("TaskQueue is a singleton, use .get() to get the instance")

        self.queue = Queue()
        self.condition = threading.Condition()

    def add_task(self, task: Task):
        self.queue.put(task)

    def get_task(self, timeout=1) -> Optional[TaskQueueItem]:
        try:
            raw_item = self.queue.get(block=True, timeout=timeout)
            return TaskQueueItem(queue_item_id=raw_item['pqid'], task=raw_item['data'])

        except Empty:
            return None
        
    def qsize(self):
        return self.queue.qsize()
