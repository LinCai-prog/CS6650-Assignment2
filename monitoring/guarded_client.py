#!/usr/bin/env python3
"""Run a local load client only while fresh resort queue samples are available."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
from queue_guard import QueueDepthGuard

RESORTS = {f'resort.{i}' for i in range(1,11)}


def next_sample(stream, stale_seconds):
    offset = stream.tell(); line = stream.readline()
    if not line.endswith('\n'):
        stream.seek(offset); return None
    try:
        data = json.loads(line)
        timestamp = datetime.fromisoformat(data['utc'].replace('Z','+00:00'))
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        by_name = {q['name']: q for q in data['queues']}
        if not -2 <= age <= stale_seconds or not RESORTS <= by_name.keys():
            return None
        if any(type(by_name[n].get(field)) is not int or by_name[n][field] < 0
               for n in RESORTS for field in ('messages_ready','messages_unacknowledged')):
            return None
        return data, timestamp
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queues', required=True, type=Path, help='Live JSONL file from sample_queues.py')
    parser.add_argument('--output-dir', required=True, type=Path, help='New directory for guard evidence')
    parser.add_argument('--threshold', type=int, default=1000)
    parser.add_argument('--stale-seconds', type=float, default=5)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args(); command = args.command
    if command and command[0] == '--': command = command[1:]
    if not command or args.threshold < 1 or not 1 <= args.stale_seconds <= 60:
        parser.error('Provide a command, positive threshold and stale limit in 1..60 seconds')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    guard = QueueDepthGuard(args.output_dir, args.threshold)
    with args.queues.open() as samples:
        samples.seek(0, 2)
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            item = next_sample(samples, args.stale_seconds)
            if item: break
            time.sleep(.05)
        else: raise SystemExit('No fresh queue sample; client was not started')
        data, stamp = item; guard.observe(data)
        if guard.event.is_set(): return 2
        if any(q['name'] in RESORTS and q['messages_ready'] + q['messages_unacknowledged'] for q in data['queues']):
            raise SystemExit('Existing resort backlog; wait for drain before starting a benchmark')
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL); guard.set_client(child)
        last_sample = time.monotonic()
        try:
            while child.poll() is None and not guard.event.is_set():
                item = next_sample(samples, args.stale_seconds)
                if item and item[1] > stamp:
                    data, stamp = item; last_sample = time.monotonic(); guard.observe(data)
                if time.monotonic()-last_sample > args.stale_seconds:
                    print('Queue samples went stale; stopping the client', flush=True); return 2
                time.sleep(.05)
            return 2 if guard.event.is_set() else child.returncode
        except KeyboardInterrupt:
            return 130
        finally:
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=15)
                except subprocess.TimeoutExpired: child.kill(); child.wait()

if __name__ == '__main__':
    raise SystemExit(main())
