import sys
import socket
import os
from argparse import ArgumentParser
import requests
import time
import datetime
import random
import threading
import multiprocessing
import json 
import yaml
from dxf import *
from multiprocessing import Process, Queue
import importlib
import hash_ring
from decouple import config

## get requests
def send_request_get(client, payload):
    ## Read from the queue
    s = requests.session()
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    s.post("http://" + str(client) + "/up", data=json.dumps(payload), headers=headers, timeout=100)

def auth(dxf, response):
    dxf.authenticate(config('REGISTRY_USERNAME'), config('REGISTRY_PASSWORD'), response=response, actions=["*"])

def send_warmup_thread(requests, q, registry, generate_random):
    trace = {}
    digests = []
    dxf = DXF(config('REGISTRY_URL'), config('REGISTRY_REPO'), auth)
    f = open(str(os.getpid()), 'wb')
    f.write('\0')
    f.close()
    for request in requests:
        if request['size'] < 0:
            trace[request['uri']] = 'bad'
        elif not (request['uri'] in trace):
            with open(str(os.getpid()), 'wb') as f:
                if generate_random is True:
                    f.seek(request['size'] - 9)
                    f.write(str(random.getrandbits(64)))
                    f.write('\0')
                else:
                    f.seek(request['size'] - 1)
                    f.write('\0')

            try:
                dgst = dxf.push_blob(str(os.getpid()))
                digests.append(dgst)
            except:
                dgst = 'bad'
            print request['uri'], dgst
            trace[request['uri']] = dgst
    if config('REGISTRY_USERNAME') == 'AWS':
        dxf.set_alias(str(os.getpid()), *digests)
    os.remove(str(os.getpid()))
    q.put(trace)

def warmup(data, out_trace, registry, threads, generate_random):
    trace = {}
    processes = []
    q = Queue()
    process_data = []
    for i in range(threads):
        process_data.append([])
    i = 0
    for request in data:
        if request['method'] == 'GET':
            process_data[i % threads].append(request)
            i += 1
    for i in range(threads):
        p = Process(target=send_warmup_thread, args=(process_data[i], q, registry, generate_random))
        processes.append(p)

    for p in processes:
        p.start()

    for i in range(threads):
        d = q.get()
        for thing in d:
            if thing in trace:
                if trace[thing] == 'bad' and d[thing] != 'bad':
                    trace[thing] = d[thing]
            else:
                trace[thing] = d[thing]

    for p in processes:
        p.join()

    with open(out_trace, 'w') as f:
        json.dump(trace, f)
 
def stats(responses):
    responses.sort(key = lambda x: x['time'])

    endtime = 0
    data = 0
    latency = 0
    total = len(responses)
    onTimes = 0
    failed = 0
    startTime = responses[0]['time']
    for r in responses:
        if r['onTime'] == 'failed':
            total -= 1
            failed += 1
            continue
        if r['time'] + r['duration'] > endtime:
            endtime = r['time'] + r['duration']
        latency += r['duration']
        data += r['size']
        if r['onTime'] == 'yes':
            onTimes += 1
    duration = endtime - startTime
    print 'Statistics'
    print 'Successful Requests: ' + str(total)
    print 'Failed Requests: ' + str(failed)
    print 'Duration: ' + str(duration)
    print 'Data Transfered: ' + str(data) + ' bytes'
    print 'Average Latency: ' + str(latency / total)
    print '% requests on time: ' + str(1.*onTimes / total)
    print 'Throughput: ' + str(1.*total / duration) + ' requests/second'

           
def serve(port, ids, q, out_file):
    server_address = ("0.0.0.0", port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(server_address)
        sock.listen(len(ids))
    except:
        print "Port already in use: " + str(port)
        q.put('fail')
        quit()
    q.put('success')
 
    i = 0
    response = []
    print "server waiting"
    while i < len(ids):
        connection, client_address = sock.accept()
        resp = ''
        while True:
            r = connection.recv(1024)
            if not r:
                break
            resp += r
        connection.close()
        try:
            info = json.loads(resp)
            if info[0]['id'] in ids:
                info = info[1:]
                response.extend(info)
                i += 1
        except:
            print 'exception occured in server'
            pass
    
    if not os.path.exists(config('RESULT_DIRECTORY')):
        os.makedirs(config('RESULT_DIRECTORY'))
    with open(os.path.join(config('RESULT_DIRECTORY'), config('REGISTRY_URL') + "-" + out_file), 'w') as f:
        json.dump(response, f)
    print 'results written to ' + config('RESULT_DIRECTORY'), config('REGISTRY_URL') + "-" + out_file
    stats(response)
    

  
## Get blobs
def get_blobs(data, clients_list, port, out_file):
    processess = []

    ids = []
    for d in data:
        ids.append(d[0]['id'])

    serveq = Queue()
    server = Process(target=serve, args=(port, ids, serveq, out_file))
    server.start()
    status = serveq.get()
    if status == 'fail':
        quit()
    ## Lets start processes
    i = 0
    for client in clients_list:
        p1 = Process(target = send_request_get, args=(client, data[i], ))
        processess.append(p1)
        i += 1
        print "starting client ..."
    for p in processess:
        p.start()
    for p in processess:
        p.join()

    server.join()

def get_requests(files, t, limit):
    ret = []
    for filename in files:
        with open(filename, 'r') as f:
            requests = json.load(f)
    
        for request in requests:
            method = request['http.request.method']
            uri = request['http.request.uri']
            if (('GET' == method) or ('PUT' == method)) and (('manifest' in uri) or ('blobs' in uri)):
                size = request['http.response.written']
                if size > 0:
                    timestamp = datetime.datetime.strptime(request['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    duration = request['http.request.duration']
                    client = request['http.request.remoteaddr']
                    document_type = 'manifest' if ('manifest' in uri) else 'layer'
                    r = {
                        'delay': timestamp, 
                        'uri': uri, 
                        'size': size, 
                        'method': method, 
                        'duration': duration,
                        'client': client,
                        'document_type': document_type
                    }
                    ret.append(r)
    ret.sort(key= lambda x: x['delay'])
    begin = ret[0]['delay']

    for r in ret:
        r['delay'] = (r['delay'] - begin).total_seconds()
   
    if t == 'seconds':
        begin = ret[0]['delay']
        i = 0
        for r in ret:
            if r['delay'] > limit:
                break
            i += 1
        print i 
        return ret[:i]
    elif t == 'requests':
        return ret[:limit]
    else:
        return ret

def organize(requests, out_trace, numclients, client_threads, port, wait, registries, round_robin, push_rand):
    organized = []

    if round_robin is False:
        ring = hash_ring.HashRing(range(numclients))
    with open(out_trace, 'r') as f:
        blob = json.load(f)

    for i in range(numclients):
        organized.append([{'port': port, 'id': random.getrandbits(32), 'threads': client_threads, 'wait': wait, 'registry': registries, 'random': push_rand, 'registry_repo': config('REGISTRY_REPO'), 'registry_username': config('REGISTRY_USERNAME'), 'registry_url': config('REGISTRY_URL'), 'registry_password': config('REGISTRY_PASSWORD')}])
        print organized[-1][0]['id']
    i = 0

    for r in requests:
        request = {
            'delay': r['delay'],
            'duration': r['duration'],
            'document_type': r['document_type'],
            'method': r['method']
        }
        if r['uri'] in blob and r['method'] != 'PUT':
            b = blob[r['uri']]
            if b != 'bad':
                request['blob'] = b
                if round_robin is True:
                    organized[i % numclients].append(request)
                    i += 1
                else:
                    organized[ring.get_node(r['client'])].append(request)
        else:
            request['size'] = r['size']
            if round_robin is True:
                organized[i % numclients].append(request)
                i += 1
            else:
                organized[ring.get_node(r['client'])].append[(request)]

    return organized


def main():

    parser = ArgumentParser(description='Trace Player, allows for anonymized traces to be replayed to a registry, or for caching and prefecting simulations.')
    parser.add_argument('-i', '--input', dest='input', type=str, required=True, help = 'Input YAML configuration file, should contain all the inputs requried for processing')
    parser.add_argument('-c', '--command', dest='command', type=str, required=True, help= 'Trace player command. Possible commands: warmup, run, simulate, warmup is used to populate the registry with the layers of the trace, run replays the trace, and simulate is used to test different caching and prefecting policies.')

    args = parser.parse_args()
    
    configs = file(args.input, 'r')

    try:
        inputs = yaml.load(configs)
    except Exception as inst:
        print 'error reading config file'
        print inst
        exit(-1)

    verbose = False

    if 'verbose' in inputs:
        if inputs['verbose'] is True:
            verbose = True
            print 'Verbose Mode'

    trace_files = []

    if config('TRACE_DIRECTORY') is not "":
        location = config('TRACE_DIRECTORY')
        if '/' != location[-1]:
            location += '/'
        for fname in config('TRACE_FILES').split(','):
            trace_files.append(location + fname)
    else:
        trace_files.extend(config('TRACE_FILES').split(','))

    if verbose:
        print 'Input traces'
        for f in trace_files:
            print f

    limit_type = None
    limit = 0

    if config('LIMIT_TYPE') is not "":
        limit_type = config('LIMIT_TYPE')
        if limit_type in ['seconds', 'requests']:
            limit = config('LIMIT_AMOUNT', cast=int)
        else:
            print 'Invalid trace limit_type: limit_type must be either seconds or requests'
            print exit(1)
    elif verbose:
        print 'limit_type not specified, entirety of trace files will be used will be used.'

    if 'output' in inputs['trace']:
        out_file = inputs['trace']['output']
    else:
        out_file = 'output.json'
        if verbose:
            print 'Output trace not specified, ./output.json will be used'

    generate_random = False
    if args.command != 'simulate':
        if "warmup" not in inputs or 'output' not in inputs['warmup']:
            print 'warmup not specified in config, warmup output required. Exiting'
            exit(1)
        else:
            interm = inputs['warmup']['output']
            if 'random' in inputs['warmup']:
                if inputs['warmup']['random'] is True:
                    generate_random = True

    registries = []
    if 'registry' in inputs:
        registries.extend(inputs['registry'])

    json_data = get_requests(trace_files, limit_type, limit)

    if args.command == 'warmup':
        if verbose: 
            print 'warmup mode'
        if config('WARMUP_THREADS') is not "":
            threads = config('WARMUP_THREADS', cast=int)
        else:
            threads = 1
        if verbose:
            print 'warmup threads: ' + str(threads)
        warmup(json_data, interm, registries[0], threads, generate_random)

    elif args.command == 'run':
        if verbose:
            print 'run mode'

        if 'client_info' not in inputs or inputs['client_info'] is None:
            print 'client_info required for run mode in config file'
            print 'exiting'
            exit(1)

        port = config('MASTER_PORT', cast=int)
        
        print 'master port: ' + str(port)

        client_threads = config('CLIENT_THREADS', cast=int)
        if verbose:
            print str(client_threads) + ' client threads'

        client_list = config('CLIENTS').split(',')

        wait = config('WAIT', cast=bool)

        round_robin = True

        data = organize(json_data, interm, len(client_list), client_threads, port, wait, registries, round_robin, generate_random)
        ## Perform GET
        get_blobs(data, client_list, port, out_file)


    elif args.command == 'simulate':
        if verbose:
            print 'simulate mode'
        if 'simulate' not in inputs:
            print 'simulate file required in config'
            exit(1)
        pi = inputs['simulate']['name']
        if '.py' in pi:
            pi = pi[:-3]
        try:
            plugin = importlib.import_module(pi)
        except Exception as inst:
            print 'Plugin did not work!'
            print inst
            exit(1)
        try:
            if 'args' in inputs['simulate']:
                plugin.init(json_data, inputs['simulate']['args'])
            else:
                plugin.init(json_data)
        except Exception as inst:
            print 'Error running plugin init!'
            print inst


if __name__ == "__main__":
    main()
