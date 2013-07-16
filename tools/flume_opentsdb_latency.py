import sys
import urllib2
import socket
import time
import json
from urllib2 import urlopen
from collections import defaultdict

metric  = "ms3-000.yandex.ru/base_antiwizard-msk_antiwizard/cluster_power-cluster_power"
puthost = "localhost:5444"
gethost = "localhost:8444"


# ssh -L 8444:w487.hdp.yandex.net:8444 -L 5444:w487.hdp.yandex.net:5444 root@w497.hdp.yandex.net

def put(host, ts, timeout):
    measure = {
        metric: [{"type": "numeric", "timestamp": ts, "value": 0}]
    }
    try:
        response = urlopen("http://%(host)s/write" % locals(), json.dumps(measure), timeout)
        return response.getcode() == 200
    except:
        return False

# returns latency before meet start, avg. latency per request, #requests, coderesponses, #fails
def get(host, metric, start, end, timeout, step):
    def parseTs(measure, metric):
        if not measure.startswith(metric): raise Exception("Bad format")
        if not measure[-1]=="\n": raise Exception("Bad format")
        return int(measure.split(" ")[1])

    metric = "l.numeric." + metric
    query = "http://%(host)s/q?start=%(start)s&end=%(end)s&m=min:%(metric)s&ascii&nocache"
    query = query % locals()
    begin = time.time() 
    n = 0
    responses = defaultdict(lambda: 0)
    fails = 0
    latency = 0.0
    while True:
        try:
            n+=1
            if step>0: time.sleep(step)
            querystart = time.time()
            response = urlopen(query, None, timeout)
            latency += (time.time() - querystart)
            end = time.time()
            if end - begin > timeout: return float(timeout)
            responses[response.getcode()]+=1
            if response.getcode() != 200: continue
            tss = map(lambda x: parseTs(x, metric), response.readlines())
            info  = ""  + str(n)
            info += " " + str(start)
            info += " " + str(latency / n)
            info += " " + json.dumps(responses)
            info += " " + str(fails)
            if len(tss)>0:
                info += " " + str(tss[-1])
            print >> sys.stderr, info
            if start in tss:
                return (end - begin, latency/n, n, responses, fails)
            continue
        except urllib2.URLError, e:
            # For Python 2.6
            if isinstance(e.reason, socket.timeout):
                return (float(timeout), latency/n, n, responses, fails)
            else:
                continue
        except socket.timeout, e:
            # For Python 2.7
            return (float(timeout), latency/n, n, responses, fails)
        except KeyboardInterrupt:
            raise
        except:
            fails+=1
            continue
repeat = 1
if len(sys.argv) > 1:
    repeat = int(sys.argv[1])

while repeat>0:
    repeat-=1
    ts = int(time.time())
    if put(puthost, ts, 5*60):
        latency, requestlatency, requests, codes, fails = get(gethost, metric, ts, ts + 10, 5*60, 0)
        codes = json.dumps(codes)
        print "%(latency)s\t%(requestlatency)s\t%(requests)s\t%(codes)s\t%(fails)s" % locals()
