/**
 * Copyright (C) 2019 Strapdata (https://www.strapdata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.elassandra.common;

import com.datastax.driver.core.*;
import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.management.transaction.NoTransactionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

/**
 * Common configuration for creating Cassandra Driver cluster and session with the provided options.
 *
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
public abstract class AbstractElassandraRepositoryConfiguration {
    private final Logger LOGGER = LoggerFactory.getLogger(AbstractElassandraRepositoryConfiguration.class);

    @Autowired
    private Environment environment;

    private String scope;

    protected abstract Scope getScope();

    public AbstractElassandraRepositoryConfiguration() {
        this.scope = getScope().getName();
    }


    /**
     * Build a Cassandra Cluster object with details about the corresponding Cassandra cluster.
     * It is the main entry point of the Datastax Driver.
     *
     * addContactPoints allows to connect to cluster nodes. It is not necessary to add all contact points because Cassandra driver will use auto-discovery mechanism.
     * withPort sets the CQL native transport port.
     * withClusterName sets the Cluster instance name. It does not relate to the name of the real Cassandra cluster.
     * withCredentials permits to connect to Cassandra when username and password are set. If not set, this option is ignored.
     * setConnectTimeoutMillis defines how long the driver waits to establish a new connection to a Cassandra node before giving up.
     * setReadTimeoutMillis controls how long the driver waits for a response from a given Cassandra node before considering it unresponsive.
     * setConsistencyLevel sets level of consistency for read & write access, e.g. ONE, QUORUM, ALL (see Datastax documentation for comprehensive list).
     *
     * @return Cassandra Cluster object
     */
    @Bean(destroyMethod = "close")
    public Cluster cluster() {
        LOGGER.debug("Building Cassandra Cluster object");
        return Cluster.builder()
                .addContactPoints(environment.getProperty(scope + ".cassandra.contactPoints", "localhost"))
                .withPort(environment.getProperty(scope + ".cassandra.port", Integer.class, 9042))
                .withClusterName(environment.getProperty(scope + ".cassandra.cluster_name", "Elassandra"))
                .withCredentials(
                        environment.getProperty(scope + ".cassandra.username", "admin"),
                        environment.getProperty(scope + ".cassandra.password", "admin"))
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(environment.getProperty(scope + ".cassandra.connectTimeoutMillis", Integer.class, SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS))
                        .setReadTimeoutMillis(environment.getProperty(scope + ".cassandra.readTimeoutMillis", Integer.class, SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS)))
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.valueOf(environment.getProperty(scope + ".cassandra.consistencyLevel", QueryOptions.DEFAULT_CONSISTENCY_LEVEL.name()))))
                .build();
    }


    /**
     * Create a session from the current Cassandra Cluster. Session will query in the defined keyspace.
     * @return Cassandra Session object
     */
    @Bean(destroyMethod = "close")
    public Session session() {
        LOGGER.debug("Creating Cassandra Session for the cluster " + cluster().getClusterName());
        return cluster().connect(environment.getProperty(scope + ".cassandra.keyspaceName", scope + "gravitee"));
    }

    /**
     * The repository does not use transactional workflow
     * @return transaction manager that does nothing
     */
    @Bean
    public AbstractPlatformTransactionManager graviteeTransactionManager() {
        return new NoTransactionManager();
    }

}
