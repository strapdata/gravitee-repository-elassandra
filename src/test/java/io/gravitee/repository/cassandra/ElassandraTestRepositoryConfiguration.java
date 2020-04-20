/**
 * This file is part of Gravitee.io APIM - API Management - Repository for Elassandra.
 *
 * Gravitee.io APIM - API Management - Repository for Elassandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gravitee.io APIM - API Management - Repository for Elassandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Gravitee.io APIM - API Management - Repository for Elassandra.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.gravitee.repository.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.management.ManagementRepositoryConfiguration;

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
@ComponentScan("io.gravitee.repository.elassandra.management")
public class ElassandraTestRepositoryConfiguration extends ManagementRepositoryConfiguration {

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
        } catch (IOException e) {
            e.printStackTrace();
        }
        final Session session = cluster.newSession();
        // datacenter1 = Default DC for test
        session.execute("CREATE KEYSPACE IF NOT EXISTS gravitee WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': '1' };");
        return cluster.connect("gravitee");
    }

}
