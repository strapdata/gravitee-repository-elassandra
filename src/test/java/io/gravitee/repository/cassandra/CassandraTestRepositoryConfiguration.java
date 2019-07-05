/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
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
package io.gravitee.repository.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.management.ManagementRepositoryConfiguration;

import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Configuration
@ComponentScan("io.gravitee.repository.cassandra.management")
public class CassandraTestRepositoryConfiguration extends ManagementRepositoryConfiguration {

    @Autowired
    Environment environment;

    protected Scope getScope() {
        return Scope.MANAGEMENT;
    }

    @Bean(destroyMethod = "close")
    public Cluster cluster() {
        return Cluster.builder()
                .addContactPoints(environment.getProperty("management.cassandra.host"))
                .withPort(environment.getProperty("management.cassandra.port", Integer.class))
                .build();
    }

    @Bean(destroyMethod = "close")
    public Session session(final Cluster cluster) {
        try {
            // on jenkins, default timeout is reached
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(100000L);
        } catch (TTransportException | IOException e) {
            e.printStackTrace();
        }
        final Session session = cluster.newSession();
        // datacenter1 = Default DC for test
        session.execute("CREATE KEYSPACE IF NOT EXISTS gravitee WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': '1' };");
        return cluster.connect("gravitee");
    }

}
