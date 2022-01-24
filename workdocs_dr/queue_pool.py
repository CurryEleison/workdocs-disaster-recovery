import threading
import logging


class QueueWorkPool:
    def __init__(self, task_queue, worker_count: int = 4, worker_action=lambda wi, l: None,
                 lock=None) -> None:
        self.task_queue = task_queue
        self.worker_count = worker_count
        self.worker_action = worker_action
        self.lock = lock

    def worker(self, tq, lock=threading.Lock()):
        while True:
            got_queue_item = False
            try:
                workitem = tq.get(block=True, timeout=60)
                got_queue_item = True
                if workitem is None:
                    break
                self.worker_action(workitem, lock)
            except Exception as err:
                try:
                    logging.warning(err)
                except:
                    logging.warning("Encountered issue logging warning from QueueHelper")
            finally:
                if got_queue_item:
                    tq.task_done()

    def start_tasks(self):
        worker_count = self.worker_count
        args = [self.task_queue, self.lock] if self.lock else [self.task_queue]
        self._threads = [threading.Thread(target=self.worker, args=args, daemon=True)
                         for _ in range(worker_count)]
        for t in self._threads:
            t.start()

    def finish_tasks(self):
        self.task_queue.join()
        for _ in self._threads:  # signal workers to quit
            self.task_queue.put(None)
        for t in self._threads:  # wait until workers exit
            t.join()
