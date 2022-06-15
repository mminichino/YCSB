<!--
Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Couchbase (SDK 3.x) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Couchbase Server cluster. It uses the official
Couchbase Java SDK (version 3.x) and provides a rich set of configuration options, including support for the N1QL
query language.

## Quickstart

### 1. Start Couchbase Server
You need to start a single node or a cluster to point the client at. Please see [http://couchbase.com](couchbase.com)
for more details and instructions.

### 2. Set up YCSB
You can either download the release zip and run it, or just clone from master.

```
git clone https://github.com/mminichino/YCSB
cd YCSB
```

The bucket and index automation in the helper script requires the ```cbc``` CLI that is part of ```libcouchbase```. You can 
read about it [here](https://docs.couchbase.com/c-sdk/current/hello-world/cbc.html).

To install it on a Linux distribution that uses the ```yum``` package manager, first create a repo configuration
for your Linux distribution per the documentation and then install the packages:

```
yum install -y libcouchbase3 libcouchbase-devel libcouchbase3-tools
```

### 3. Run the Tests (A-F)

```
./run_cb.sh -h cbnode-0000.domain.com -u user -p password 
```

To use SSL to connect to the cluster:

```
./run_cb.sh -h cbnode-0000.domain.com -s -u user -p password
```

To run a specific workload (YCSB-A in this example):

```
./run_cb.sh -h cbnode-0000.domain.com -u user -p password -w a
```

### Manual Mode

To just create the bucket:
```
./run_cb.sh -h cbnode-0000.domain.com -B
```

To just create the index:
```
./run_cb.sh -h cbnode-0000.domain.com -I
```

To run a data load:
```
./run_cb.sh -h cbnode-0000.domain.com -s -M -l -w a
```

To run a scenario:
```
./run_cb.sh -h cbnode-0000.domain.com -s -M -r -w a
```

To manually load data (without any script automation - i.e. if ```cbc``` isn't installed):
```
./run_cb.sh -h cbnode-0000.domain.com -s -Z -M -l -w a
```

To manually run a scenario (without any automation):
```
./run_cb.sh -h cbnode-0000.domain.com -s -Z -M -r -w a
```

## Capella
For Couchbase Capella (Couchbase hosted DBaaS) you will need to create a bucket named "ycsb" before you run the test(s).
This is because Capella database users do not have sufficient permissions to operate on buckets. You will also need to 
use SSL to connect. You can provide the name given on the Capella portal as the host. The helper utility will get the SRV 
records and extract a node name to use for the host parameter.

```
./run_cb.sh -h cb.abcdefg.cloud.couchbase.com -s -u dbuser -p password 
```

## Performance Considerations
As it is with any benchmark, there are lot of knobs to tune in order to get great or (if you are reading
this and trying to write a competitor benchmark ;-)) bad performance.

The first setting you should consider, if you are running on Linux 64bit is setting `-p couchbase.epoll=true`. This will
then turn on the Epoll IO mechanisms in the underlying Netty library which provides better performance since it has less
synchronization to do than the NIO default. This only works on Linux, but you are benchmarking on the OS you are
deploying to, right?

The second option, `boost`, sounds more magic than it actually is. By default this benchmark trades CPU for throughput,
but this can be disabled by setting `-p couchbase.boost=0`. This defaults to 3, and 3 is the number of event loops run
in the IO layer. 3 is a reasonable default but you should set it to the number of **physical** cores you have available
on the machine if you only plan to run one YCSB instance. Make sure (using profiling) to max out your cores, but don't
overdo it.

## Sync vs Async
By default, since YCSB is sync the code will always wait for the operation to complete. In some cases it can be useful
to just "drive load" and disable the waiting. Note that when the "-p couchbase.syncMutationResponse=false" option is
used, the measured results by YCSB can basically be thrown away. Still helpful sometimes during load phases to speed
them up :)

## Debugging Latency
The Couchbase Java SDK has the ability to collect and dump different kinds of metrics which allow you to analyze
performance during benchmarking and production. By default this option is disabled in the benchmark, but by setting
`couchbase.networkMetricsInterval` and/or `couchbase.runtimeMetricsInterval` to something greater than 0 it will
output the information as JSON into the configured logger. The number provides is the interval in seconds. If you are
unsure what interval to pick, start with 10 or 30 seconds, depending on your runtime length.

This is how such logs look like:

```
INFO: {"heap.used":{"init":268435456,"used":36500912,"committed":232259584,"max":3817865216},"gc.ps marksweep.collectionTime":0,"gc.ps scavenge.collectionTime":54,"gc.ps scavenge.collectionCount":17,"thread.count":26,"offHeap.used":{"init":2555904,"used":30865944,"committed":31719424,"max":-1},"gc.ps marksweep.collectionCount":0,"heap.pendingFinalize":0,"thread.peakCount":26,"event":{"name":"RuntimeMetrics","type":"METRIC"},"thread.startedCount":28}
INFO: {"localhost/127.0.0.1:11210":{"BINARY":{"ReplaceRequest":{"SUCCESS":{"metrics":{"percentiles":{"50.0":102,"90.0":136,"95.0":155,"99.0":244,"99.9":428},"min":55,"max":1564,"count":35787,"timeUnit":"MICROSECONDS"}}},"GetRequest":{"SUCCESS":{"metrics":{"percentiles":{"50.0":74,"90.0":98,"95.0":110,"99.0":158,"99.9":358},"min":34,"max":2310,"count":35604,"timeUnit":"MICROSECONDS"}}},"GetBucketConfigRequest":{"SUCCESS":{"metrics":{"percentiles":{"50.0":462,"90.0":462,"95.0":462,"99.0":462,"99.9":462},"min":460,"max":462,"count":1,"timeUnit":"MICROSECONDS"}}}}},"event":{"name":"NetworkLatencyMetrics","type":"METRIC"}}
```

It is recommended to either feed it into a program which can analyze and visualize JSON or just dump it into a JSON
pretty printer and look at it manually. Since the output can be changed (only by changing the code at the moment), you
can even configure to put those messages into another couchbase bucket and then analyze it through N1QL! You can learn
more about this in general [in the official docs](http://developer.couchbase.com/documentation/server/4.0/sdks/java-2.2/event-bus-metrics.html).


## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can
tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and
implement the facilities on your own.

You can set the following properties (with the default settings applied):

 - couchbase.host=127.0.0.1: The hostname from one server.
 - couchbase.bucket=ycsb: The bucket name to use.
 - couchbase.password=: The password of the bucket.
 - couchbase.syncMutationResponse=true: If mutations should wait for the response to complete.
 - couchbase.persistTo=0: Persistence durability requirement
 - couchbase.replicateTo=0: Replication durability requirement
 - couchbase.upsert=false: Use upsert instead of insert or replace.
 - couchbase.adhoc=false: If set to true, prepared statements are not used.
 - couchbase.kv=true: If set to false, mutation operations will also be performed through N1QL.
 - couchbase.maxParallelism=1: The server parallelism for all n1ql queries.
 - couchbase.kvEndpoints=1: The number of KV sockets to open per server.
 - couchbase.queryEndpoints=5: The number of N1QL Query sockets to open per server.
 - couchbase.epoll=false: If Epoll instead of NIO should be used (only available for linux.
 - couchbase.boost=3: If > 0 trades CPU for higher throughput. N is the number of event loops, ideally
   set to the number of physical cores. Setting higher than that will likely degrade performance.
 - couchbase.networkMetricsInterval=0: The interval in seconds when latency metrics will be logged.
 - couchbase.runtimeMetricsInterval=0: The interval in seconds when runtime metrics will be logged.
 - couchbase.documentExpiry=0: Document Expiry is the amount of time(second) until a document expires in Couchbase.
 - couchbase.sslMode=none: Set to ```data``` to use SSL to connect to the cluster.
 - couchbase.usesrv=true: Set to ```false``` to not allow domains with SRV records as the hostname
