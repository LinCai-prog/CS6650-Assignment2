#!/usr/bin/env python3
"""Capture Linux CPU, memory, network, disk and optional service counters."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=int, default=120)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--service', help='Optional systemd service name')
    args = parser.parse_args()
    if args.samples < 1 or not 1 <= args.interval <= 60:
        parser.error('Samples must be positive and interval must be in 1..60 seconds')
    for i in range(args.samples):
        tick = time.monotonic()
        sample = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'kind': 'sample'}
        for key, path in [('procStat','stat'), ('meminfo','meminfo'), ('netDev','net/dev'), ('diskstats','diskstats')]:
            sample[key] = (Path('/proc') / path).read_text()
        if args.service:
            result = subprocess.run(['systemctl','show','-p','ActiveState','-p','MainPID','-p','CPUUsageNSec','-p','MemoryCurrent','-p','NRestarts','--',args.service], capture_output=True, text=True, timeout=3)
            sample['service'] = dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)
            sample['serviceQueryExitCode'] = result.returncode
        print(json.dumps(sample), flush=True)
        if i + 1 < args.samples:
            time.sleep(max(0, args.interval - (time.monotonic() - tick)))

if __name__ == '__main__':
    main()
