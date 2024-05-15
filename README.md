# Couchbase YCSB 2.0.0
This pacakge is a YCSB implementation to test against a Couchbase cluster. It uses YCSB core 0.18.0 and the Couchbase Java SDK v3.5.

## Requirements
- Java 11 or higher
- Linux, macOS, or Windows load generator system (8 cores / 32GB RAM minimum recommended)
- Couchbase Server or Couchbase Capella

## Quickstart

### 1. Start Couchbase Server
You will need a Couchbase cluster for testing. Please see either the documentation on [Couchbase Server](https://docs.couchbase.com/home/server.html) for self-managed deployments or [Couchbase Capella](https://docs.couchbase.com/cloud/index.html)
for public cloud based DBaaS deployments.

### 2. Set up YCSB
Download the distribution to begin testing.
```
curl -OLs https://github.com/mminichino/YCSB/releases/download/2.0.0/ycsb-couchbase.zip
```
```
unzip ycsb-couchbase.zip
```
```
cd ycsb-couchbase
```
On Linux/macOS:
```
vi conf/db.properties
```
On Windows:
```
notepad conf\db.properties
```
On Linux/macOS:
```
bin/run.sh
```
On Windows:
```
bin\run.bat
```

### 3. Run a specific test

```
bin/run.sh -w workloads/workloada
```

## Capella
The bucket automation for Capella uses the v4 public API. To use the automation, create an API key in the Capella UI and save it to a file named ```default-api-key-token.txt``` in a directory named ```.capella``` in either $HOME (Linux/macOS) or %userprofile% (Windows).

Uncomment the project and database properties in the config file and set them to match the project and database names as configured in Capella for the database you want to use for testing.

For the hostname property, use the ```cb.abcxyz.cloud.couchbase.com``` name provided in the UI - note: *omit* the prefix ```couchbases://```.

On Linux/macOS
```
% mkdir .capella
% vi .capella/default-api-key-token.txt
```

On Windows
```
C:\Users\Username> mkdir .capella
C:\Users\Username> cd .capella
C:\Users\Username\.capella> notepad default-api-key-token.txt
```

Edit properties
````
couchbase.project=ycsb-capella
couchbase.database=testdb01
````

## Database Credentials
To use the automation, be sure to use credentials that have permission to create and mange buckets and indexes. While this test suite should not interfere with other buckets in a cluster, it will consume cluster resources, so it is advisable to make sure that the test operations are the only operations being performed against the cluster. It is also a best practice to not use a production cluster that is being used for production operations.

## Run script options
| Option        | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| -w WORKLOAD   | Path to workload definition file                                            |
| -l            | Only load data (don't do run phase)                                         |
| -r            | Only run test (don't do load phase - requires previously loaded data)       |
| -R RECORDS    | Record count (number of records to load)                                    |
| -O OPERATIONS | Operation count (number of transaction to perform in run phase)             |
| -T TIME       | Stop run after this amount of time (in seconds)                             |
| -M            | Manual mode - bucket and index (for workload E) have to be created manually |
| -S            | Enable statistics collection during the test                                |

## Configuration Options
Under normal circumstances you should only need to set the hostname, username, and password properties. If you are using a cluster that was just built for testing with default settings then technically you just need to set the hostname parameter. For Capella, you should at least set the hostname, username, password, project and database properties.

| Property                 | Default       | Description                                                                       |
|--------------------------|---------------|-----------------------------------------------------------------------------------|
| statistics.enable        | false         | Enable statistics collection and reporting                                        |
| couchbase.hostname       | 127.0.0.1     | Connect hostname or IP address                                                    |
| couchbase.bucket         | ycsb          | Test bucket                                                                       |
| couchbase.scope          | _default      | Test scope                                                                        |
| couchbase.collection     | _default      | Test collection                                                                   |
| couchbase.username       | Administrator | Database credential username                                                      |
| couchbase.password       | password      | Database credential password                                                      |
| couchbase.bucketType     | 0             | Create bucket of type Couchstore (0) or Magma (1)                                 |
| couchbase.replicaNum     | 1             | Replica number for bucket                                                         |
| couchbase.project        | test-project  | Capella project name                                                              |
| couchbase.database       | cbdb01        | Capella database name                                                             |
| couchbase.durability     | 0             | Durability None (0) Majority (1) Persist to Active (2) or Persist to Majority (3) |
| couchbase.adhoc          | false         | If true queries will not use a pre-generated plan                                 |
| couchbase.maxParallelism | 0             | Set maximum parallelism for queries                                               |
| couchbase.kvEndpoints    | 4             | Set the number of KV endpoints for data operations                                |
| couchbase.sslMode        | true          | Use SSL (true) or do not use SSL (false)                                          |
| couchbase.kvTimeout      | 5             | Timeout for KV operations                                                         |
| couchbase.connectTimeout | 5             | Timeout for connections                                                           |
| couchbase.queryTimeout   | 75            | Timeout for query operations                                                      |
| couchbase.mode           | default       | Test mode either normal (default) or array append (array)                         |
| couchbase.ttlSeconds     | 0             | Document TTL                                                                      |
| couchbase.eventing       | timestamp.js  | Experimental future feature to create an Eventing function                        |

## Manual mode
Linux or macOS:
```
java -cp "$(pwd)/lib/*" site.ycsb.db.couchbase3.ClusterInit -p conf/db.properties
```
```
bin/run.sh -M -w workloads/workloada
```
```
java -cp "$(pwd)/lib/*" site.ycsb.db.couchbase3.ClusterClean -p conf/db.properties
```
Windows:
```
java -cp "lib\*" site.ycsb.db.couchbase3.ClusterInit -p conf\db.properties
```
```
.\bin\run.bat -M -w .\workloads\workloada
```
```
java -cp "lib\*" site.ycsb.db.couchbase3.ClusterClean -p conf\db.properties
```
