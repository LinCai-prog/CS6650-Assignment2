"""Stop the producer on the first sampled resort-queue breach."""
import datetime
import json
import threading


class QueueDepthGuard:
    def __init__(self, base, threshold=1000):
        self.base = base
        self.threshold = threshold
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.client = None

    def set_client(self, client):
        with self.lock:
            self.client = client
            if self.event.is_set() and client.poll() is None:
                client.terminate()

    def observe(self, sample):
        breached = {
            q['name']: q.get('messages_ready', 0) + q.get('messages_unacknowledged', 0)
            for q in sample.get('queues', [])
            if q['name'].startswith('resort.')
            and q.get('messages_ready', 0) + q.get('messages_unacknowledged', 0) > self.threshold
        }
        if not breached:
            return
        with self.lock:
            if self.event.is_set():
                return
            trigger = {'queueSampleUtc': sample.get('utc'),
                       'detectedUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                       'threshold': self.threshold, 'comparison': '>',
                       'breachedResortQueues': breached,
                       'action': 'terminate new HTTP requests; leave consumers running'}
            (self.base / 'queue-threshold-trigger.json').write_text(json.dumps(trigger, indent=2))
            self.event.set()
            if self.client is not None and self.client.poll() is None:
                self.client.terminate()
            print('QUEUE GUARD: ' + json.dumps(trigger), flush=True)
