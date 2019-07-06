/**
 * Copyright (C) ${project.inceptionYear} Strapdata (https://www.strapdata.com)
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

import com.datastax.driver.core.Session;
import io.gravitee.repository.config.TestRepositoryInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author vroyer
 */
public class CassandraTestRepositoryInitializer implements TestRepositoryInitializer {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraTestRepositoryInitializer.class);

    @Autowired
    private Session session;

    @Override
    public void setUp() {
        LOGGER.debug("Starting tests");
        //session.execute("CREATE TABLE IF NOT EXISTS gravitee.ratelimits (id text PRIMARY KEY, last_request timestamp, counter bigint, reset_time timestamp, created_at timestamp, updated_at timestamp, async boolean);");
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
        session.execute("TRUNCATE gravitee.ratelimits;");
    }
}
