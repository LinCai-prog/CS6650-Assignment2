#!/usr/bin/env python3
"""Broker-local, one-second Ready + Unacked samples for the project queues."""
import argparse
import json
import subprocess
from pathlib import Path

EXPR = '''F = fun Loop(0) -> ok; Loop(N) ->
 case filelib:is_file(STOP_PATH) of true -> ok; false ->
 Tick = erlang:monotonic_time(millisecond),
 Stats = [[{name, element(4, amqqueue:get_name(Q))} |
 rabbit_amqqueue:info(Q, [messages_ready, messages_unacknowledged])]
 || Q <- rabbit_amqqueue:list(<<"/">>),
 (lists:prefix("resort.", binary_to_list(element(4, amqqueue:get_name(Q)))) orelse
 element(4, amqqueue:get_name(Q)) =:= <<"skitrak.deadletter.queue">>)],
 io:format("FAST_QUEUE ~ts~n", [rabbit_json:encode([{utc, list_to_binary(
 calendar:system_time_to_rfc3339(erlang:system_time(millisecond),
 [{unit,millisecond},{offset,"Z"}]))},{queues,Stats}])]),
 timer:sleep(max(0,1000-(erlang:monotonic_time(millisecond)-Tick))),
 Loop(N-1) end end, F(SAMPLES).'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=int, default=600)
    parser.add_argument('--stop-file', type=Path, required=True,
                        help='Use a fresh broker-local path under /tmp; touch it to stop')
    args = parser.parse_args()
    if not 1 <= args.samples <= 14400 or args.stop_file.exists():
        parser.error('Use 1..14400 samples and a new stop-file path')
    expr = EXPR.replace('SAMPLES', str(args.samples)).replace('STOP_PATH', json.dumps(str(args.stop_file.resolve())))
    with subprocess.Popen(['rabbitmqctl', '-t', str(args.samples+30), 'eval', expr], stdout=subprocess.PIPE, text=True) as proc:
        try:
            for line in proc.stdout:
                if 'FAST_QUEUE ' in line:
                    sample = json.loads(line.split('FAST_QUEUE ', 1)[1]); sample['kind'] = 'sample'
                    print(json.dumps(sample), flush=True)
        except KeyboardInterrupt:
            args.stop_file.touch(); return 130
        return proc.wait()

if __name__ == '__main__':
    raise SystemExit(main())
