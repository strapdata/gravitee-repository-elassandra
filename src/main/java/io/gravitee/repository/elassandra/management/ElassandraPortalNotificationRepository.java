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
package io.gravitee.repository.elassandra.management;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.PortalNotificationRepository;
import io.gravitee.repository.management.model.PortalNotification;

/**
 * @author vroyer
 */
@Repository
public class ElassandraPortalNotificationRepository extends ElassandraCrud<PortalNotification, String> implements PortalNotificationRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraPortalNotificationRepository.class);

    public ElassandraPortalNotificationRepository() throws IOException {
        super("portal_notifications",
                new String[] { "id", "title", "message", "user", "created_at" },
                "portal_notifications",
                Settings.builder().put("synchronous_refresh", true),
                XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 0)
                            .field("cql_partition_key", true)
                        .endObject()
                        .startObject("title").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("message").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("user").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(PortalNotification portalNotification) {
        return new Object[] {
                portalNotification.getId(),
                portalNotification.getTitle(),
                portalNotification.getMessage(),
                portalNotification.getUser(),
                portalNotification.getCreatedAt()
        };
    }

    // CREATE TABLE portal_notification_configs( user text, hooks list<text>, reference_type text, reference_id text, created_at timestamp, updatedAt timestamp, PRIMARY KEY (user, reference_type, reference_id))
    @Override
    public PortalNotification fromRow(Row row) {
        if (row != null) {
            final PortalNotification portalNotification = new PortalNotification();
            portalNotification.setId(row.getString("id"));
            portalNotification.setTitle(row.getString("title"));
            portalNotification.setMessage(row.getString("message"));
            portalNotification.setUser(row.getString("user"));
            portalNotification.setCreatedAt(row.getTimestamp("created_at"));
            return portalNotification;
        }
        return null;
    }


    @Override
    public void deleteAll(String user) throws TechnicalException {
        LOGGER.debug("Delete protalNotidfication for user : {}", user);
        for(PortalNotification p : findByUser(user))
            delete(p.getId());
    }

    @Override
    public List<PortalNotification> findByUser(String user) throws TechnicalException {
        LOGGER.debug("Find notifications by user: {}", user);
        try {
            if (user == null)
                return Collections.EMPTY_LIST;

            String esQuery = new SearchSourceBuilder()
                    .query(QueryBuilders.termQuery("user", user))
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("es_query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            LOGGER.error("Failed to find protalNotidfication by user:" + user, ex);
            throw new TechnicalException("Failed to find by user:" + user, ex);
        }
    }
}
