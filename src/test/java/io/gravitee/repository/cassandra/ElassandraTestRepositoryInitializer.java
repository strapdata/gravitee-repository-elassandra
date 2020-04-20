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

import com.datastax.driver.core.Session;
import io.gravitee.repository.config.TestRepositoryInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author vroyer
 */
public class ElassandraTestRepositoryInitializer implements TestRepositoryInitializer {

    private final Logger LOGGER = LoggerFactory.getLogger(ElassandraTestRepositoryInitializer.class);

    @Autowired
    private Session session;

    @Override
    public void setUp() {
        LOGGER.debug("Starting tests");
    }

    @Override
    public void tearDown() {
        LOGGER.debug("Ending tests");
        // drop keyspace takes too much time
        // session.execute("DROP KEYSPACE IF EXISTS gravitee;");
        session.execute("TRUNCATE gravitee.tenants;");
        session.execute("TRUNCATE gravitee.views;");
        session.execute("TRUNCATE gravitee.tags;");
        session.execute("TRUNCATE gravitee.apikeys;");
        session.execute("TRUNCATE gravitee.apis;");
        session.execute("TRUNCATE gravitee.api_headers;");
        session.execute("TRUNCATE gravitee.applications;");
        session.execute("TRUNCATE gravitee.generic_notification_configs;");
        session.execute("TRUNCATE gravitee.portal_notifications;");
        session.execute("TRUNCATE gravitee.portal_notification_configs;");
        session.execute("TRUNCATE gravitee.client_registration_providers;");
        session.execute("TRUNCATE gravitee.commands;");
        session.execute("TRUNCATE gravitee.dictionaries;");
        session.execute("TRUNCATE gravitee.events;");
        session.execute("TRUNCATE gravitee.entrypoints;");
        session.execute("TRUNCATE gravitee.groups;");
        session.execute("TRUNCATE gravitee.identity_providers;");
        session.execute("TRUNCATE gravitee.invitations;");
        session.execute("TRUNCATE gravitee.media;");
        session.execute("TRUNCATE gravitee.memberships;");
        session.execute("TRUNCATE gravitee.pages;");
        session.execute("TRUNCATE gravitee.plans;");
        session.execute("TRUNCATE gravitee.parameters;");
        session.execute("TRUNCATE gravitee.users;");
        session.execute("TRUNCATE gravitee.subscriptions;");
        session.execute("TRUNCATE gravitee.metadata;");
        session.execute("TRUNCATE gravitee.roles");
        session.execute("TRUNCATE gravitee.ratings");
        session.execute("TRUNCATE gravitee.ratinganswers");
        session.execute("TRUNCATE gravitee.audits");
        session.execute("TRUNCATE gravitee.alerts");
        session.execute("TRUNCATE gravitee.workflows");
        session.execute("TRUNCATE gravitee.dashboards");
        session.execute("TRUNCATE gravitee.quality_rules");
        session.execute("TRUNCATE gravitee.api_quality_rules");
        //session.execute("TRUNCATE gravitee.ratelimits;");
    }
}
