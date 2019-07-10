# Gravitee Elassandra Repository [![Build Status](https://travis-ci.org/strapdata/gravitee-repository-elassandra.svg?branch=master)](https://travis-ci.org/strapdata/gravitee-repository-elassandra)

[Elassandra](https://www.elassandra.io/) repository (Management, Rate Limit and Analytics), with the following benefits:

* Distributed gravitee configuration on many datacenters through the Cassandra replication (in active/active mode).
* Elasticsearch reporting for gravitee analytics.
* Scalability by adding Elassandra nodes (without re-indexing) and datacenters (for geo localisation concerns or workload separation)
* Reduce the global complexity and TCO by using the same NoSQL database for both gravitee configuration, reporting and potentially APIs data storage.

This repository uses Datastax Java driver for communication with Cassandra and Elasticsearch query over CQL.

## Requirement

The minimum requirement is :
 * Maven3
 * Jdk8

In order to use **Gravitee** snapshot, You need to declare the following repository in you Maven settings :

https://oss.sonatype.org/content/repositories/snapshots


## Building

This Elassandra repository use [Elassandra-Unit](https://github.com/strapdata/elassandra-unit) to run gravitee unit tests.

```
$ git clone https://github.com/strapdata/gravitee-repository-elassandra.git
$ cd gravitee-repository-elassandra
$ mvn clean package
```

To build the gravitee docker images including this gravitee-repository-elassandra.zip, 

```bash
build.sh
```

Strapdata docker images are also available on the Docker Hub:

 * [strapdata/graviteeio-management-api](https://hub.docker.com/r/strapdata/graviteeio-management-api)
 * [strapdata/graviteeio-gateway](https://hub.docker.com/r/strapdata/graviteeio-gateway)

## Installing

Unzip the gravitee-repository-elassandra-1.0.0-SNAPSHOT.zip in the gravitee home directory.

Copy the *gravitee-repository-elassandra-1.0.0-SNAPSHOT.zip* in the **Gravitee** */plugins* directory.


## Configuration

Configuration settings, prefixed by *scope*.**elassandra**, where *scope* is **management** or **ratelimit**:


| Parameter                           |   Description   |        default |
| ----------------------------------- | --------------- | -------------: |
| contactPoint                        | Allows to connect to Cassandra cluster nodes. It is not necessary to add all contact points because Cassandra driver will use auto-discovery mechanism. |       localhost |
| endpoint                            | Defines the Elasticsearch endpoint used to create Elasticsearch indices. | http://localhost:9200 |
| port                                | Defines the CQL native transport port |            9042 |
| keyspaceName                        | Name of the keyspace. Note that the final will be prefixed with the corresponding scope. | <scope>gravitee |
| username                            | Permit to connect to Cassandra and Elasticsearch if using access with credentials. |        cassandra |
| password                            | Permit to connect to Cassandra and Elasticsearch if using access with credentials. |        cassandra |
| connectTimeoutMillis                | Defines how long the driver waits to establish a new connection to a Cassandra node before giving up |            5000 |
| readTimeoutMillis                   | Controls how long the driver waits for a response from a given Cassandra node before considering it unresponsive |           12000 |
| consistencyLevel                    | Sets the level of consistency for read & write access, e.g. ONE, QUORUM, ALL (see Datastax documentation for comprehensive list) |             ONE |
| ssl.provider                        | Java SSL/TLS provider | JDK |
| ssl.truststore.path                 | Truststore file name (JKS or P12) |   |
| ssl.truststore.password             | Truststore password |   |
| ssl.keystore.path                   | Keystore file name (JKS or P12) for TLS client authentication |   |
| ssl.keystore.password               | Keystore password |   |

For analytics, settings are the same as Elasticsearch, see the
[gravitee documentation](https://docs.gravitee.io/apim_installguide_management_api_configuration.html#analytics) (Elassandra is seen as a vanilla Elasticsearch).

Configuration sample:

```yaml
management:
  type: elassandra
  elassandra:
    clusterName: elassandra
    port: 39042
    contactPoint: elassandra.default.svc.cluster.local
    endpoint: https://elassandra-elasticsearch.default.svc.cluster.local:9200
    username: cassandra
    password: cassandra
    ssl:
      truststore:
        path: /ca-pub/truststore.p12
        password: changeit
ratelimit:
  type: elassandra
  elassandra:
    clusterName: elassandra
    port: 39042
    contactPoint: elassandra.default.svc.cluster.local
    endpoint: https://elassandra-elasticsearch.default.svc.cluster.local:9200
    username: cassandra
    password: cassandra
    ssl:
      truststore:
        path: /ca-pub/truststore.p12
        password: changeit
analytics:
  type: elasticsearch
  elasticsearch:
    endpoints:
      - https://elassandra-elasticsearch.default.svc.cluster.local:9200
    security:
      username: cassandra
      password: cassandra
      ssl:
        truststore:
          path: /ca-pub/truststore.p12
          password: changeit
    index: analytics
    cluster: elassandra
...
```

## Support

 * Commercial support is available through [Strapdata](http://www.strapdata.com/).
 * Community support available via [elassandra google groups](https://groups.google.com/forum/#!forum/elassandra).
 * Post feature requests and bugs on https://github.com/strapdata/gravitee-repository-elassandra

## License

```
This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2019, Strapdata (contact@strapdata.com).

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```

## Acknowledgments

* Elasticsearch and Kibana are trademarks of Elasticsearch BV, registered in the U.S. and in other countries.
* Apache Cassandra, Apache Lucene, Apache, Lucene and Cassandra are trademarks of the Apache Software Foundation.
* Gravitee is a trademark of Graviteesource.
* Elassandra is a trademark of Strapdata SAS.

