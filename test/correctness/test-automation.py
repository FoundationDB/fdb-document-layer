#!/usr/bin/python
#
# test-automation.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# MongoDB is a registered trademark of MongoDB, Inc.
#

import argparse
import fcntl
import os
import random
import shlex
import signal
import subprocess
import sys
import time

import util

sys.path.append(os.path.dirname(__file__))

processes = list()
instances = list()
run_path = ""


def get_used_memory_in_mb():
    import psutil
    virt_mem = float(0.0)
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['pid', 'name'])
            for xx in ['bigdoc', 'fdbdoc']:
                if xx in pinfo['name']:
                    virt_mem = virt_mem + float(proc.memory_info_ex().vms / (1024 * 1024))
        except psutil.NoSuchProcess:
            pass
    return float(virt_mem)


def kill_process(proc):
    try:
        if proc is None:
            pass
        else:
            os.kill(proc.pid, signal.SIGTERM)
    except:
        pass


def kill_processes():
    global processes
    for proc in processes:
        kill_process(proc)


def show_statistics(ns):
    kill_processes()

    global run_path
    run_count = len([
        name for name in os.listdir(run_path)
        if os.path.isfile(os.path.join(run_path, name)) and not name.endswith('.timeout')
    ])
    print ("Total tests run: ", str(run_count))

    failed_count = len(
        [f for f in os.listdir(run_path) if f.endswith('.failed') and os.path.isfile(os.path.join(run_path, f))])
    print ("Failing tests  : ", str(failed_count))

    # this assumes that the database name we use for testing is "test" and we are interested in doclayer
    used_mem = float(0.0)
    if "doclayer" in [ns['1'], ns['2']]:
        import pymongo
        client = pymongo.MongoClient(ns['doclayer_host'], ns['doclayer_port'], max_pool_size=1)
        cmd_output = client.test.command("getmemoryusage")
        used_mem = float(cmd_output['process memory usage'])
    print ("Used memory by FDBDOC : ", str(used_mem))


def non_block_readline(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.readline()
    except:
        return ''


def start_process(ns):
    # default random value, in case we do not find anything else to use
    instance_id = int(str(random.random())[2:])

    ns['instance_id'] = instance_id
    processes.append(subprocess.Popen(shlex.split(util.get_cmd_line(ns)), shell=False, stderr=subprocess.PIPE))
    instances.append(instance_id)


def test_auto_forever(ns):
    import atexit
    atexit.register(show_statistics, ns)

    # make sure the folder where failure will be stored is created
    global run_path
    run_path = os.path.dirname(os.path.realpath(__file__)) + '/test_results/'
    if not os.path.exists(run_path):
        os.makedirs(run_path)

    # number of minute to run the test
    num_min = ns["num_min"]

    start_time = time.time()

    # number of parallel runs
    num_parallel = ns["num_parallel"]

    # Initialize it with 0 which means auto-generate it
    ns['instance_id'] = 0

    is_time_to_stop = False
    last_update = [None] * num_parallel

    while True:
        try:
            # Why do we need this here ? - if too many tests start to fail, perhaps we should restore it, but
            # commenting it out for now.
            # sleep(0.1)

            # if we cannot create new tests and there are no other active, lets finish
            if is_time_to_stop == True:  # and len(processes) == 0:
                break

            # create initial number of processes
            if not is_time_to_stop and len(processes) == 0:
                for ii in range(min(len(processes), num_parallel), max(len(processes), num_parallel)):
                    start_process(ns)
                    last_update[ii] = time.time()

            for ii, pp in enumerate(processes):
                # if user provided num of min to run, we should check that here
                curr_time = time.time()
                run_min = (curr_time - start_time) / 60

                if 0 < num_min < run_min and is_time_to_stop == False:
                    print ("Time to stop, time to run was set to", num_min)
                    print ("Start time:", time.asctime(time.localtime(start_time)))
                    print ("Finish time:", time.asctime(time.localtime(curr_time)))
                    is_time_to_stop = True

                # if test run did not updated itself in 2 minutes, perhaps it is stuck ?
                stop_unresponsive = ((curr_time - last_update[ii]) / 60) > 2

                if pp.poll() is not None or stop_unresponsive:
                    if stop_unresponsive:
                        fname = run_path + "journal_" + str(instances[ii]) + ".running"
                        fname = util.rename_file(fname, ".timeout")
                        print ("Process was stopped because of timeout, check out this file for more info : ", fname)
                    kill_process(processes[ii])
                    del processes[ii]
                    del instances[ii]
                    # Start new process only if we are not in time out
                    if not is_time_to_stop:
                        start_process(ns)
                        last_update[ii] = time.time()

                out = non_block_readline(pp.stderr)
                if out != '':
                    last_update[ii] = time.time()
                    sys.stdout.write(out)
                    sys.stdout.flush()

        except Exception as inst:
            print (inst)
            print ("Unexpected error:", sys.exc_info()[0])
            pass
    print ("AUTOMATION FINISHED ITS WORK")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', default=False, action='store_true', help='verbose')
    parser.add_argument('--mongo-host', type=str, default='localhost', help='hostname of MongoDB server')
    parser.add_argument('--mongo-port', type=int, default=27018, help='port of MongoDB server')
    parser.add_argument('--doclayer-host', type=str, default='localhost', help='hostname of document layer server')
    parser.add_argument('--doclayer-port', type=int, default=27019, help='port of document layer server')
    parser.add_argument('--max-pool-size', type=int, default=None, help='maximum number of threads in the thread pool')
    subparsers = parser.add_subparsers(help='type of test to run')

    parser_auto_forever = subparsers.add_parser(
        'auto-forever', help='run comparison test forever with random input arguments')
    parser_auto_forever.add_argument('1', choices=['mongo', 'mm', 'doclayer'], help='first tester')
    parser_auto_forever.add_argument('2', choices=['mongo', 'mm', 'doclayer'], help='second tester')
    parser_auto_forever.add_argument(
        '--num-min',
        type=int,
        default=0,
        help='number of minutes the auto-forever test run, if not specified is really'
        ' forever')
    parser_auto_forever.add_argument(
        '--num-parallel',
        type=int,
        default=2,
        help='number of parallel runs of forever tests, if not specified is only 2')
    parser_auto_forever.set_defaults(func=test_auto_forever)

    ns = vars(parser.parse_args())

    ns['func'](ns)
