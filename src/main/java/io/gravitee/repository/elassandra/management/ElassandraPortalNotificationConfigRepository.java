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
package io.gravitee.repository.elassandra.management;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.PortalNotificationConfigRepository;
import io.gravitee.repository.management.model.Metadata;
import io.gravitee.repository.management.model.MetadataReferenceType;
import io.gravitee.repository.management.model.NotificationReferenceType;
import io.gravitee.repository.management.model.PortalNotificationConfig;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraPortalNotificationConfigRepository extends ElassandraCrud<PortalNotificationConfig, Object[]> implements PortalNotificationConfigRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraPortalNotificationConfigRepository.class);

    public ElassandraPortalNotificationConfigRepository() throws IOException {
        super("portal_notification_configs",
                new String[]{"user", "reference_type", "reference_id", "hooks", "created_at", "updated_at"},
                "portal_notification_configs",
                Settings.builder().put("synchronous_refresh", true),
                XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("user")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 0)
                            .field("cql_partition_key", true)
                        .endObject()
                        .startObject("reference_type")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 1)
                        .endObject()
                        .startObject("reference_id")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 2)
                        .endObject()
                        .startObject("hooks").field("type", "keyword").field("cql_collection", "list").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(PortalNotificationConfig t) {
        return new Object[]{
                t.getUser(),
                t.getReferenceType() == null ? null : t.getReferenceType().toString(),
                t.getReferenceId(),
                t.getHooks(),
                t.getCreatedAt(),
                t.getUpdatedAt()
                };
    }

    @Override
    public PortalNotificationConfig fromRow(Row row) {
        if (row != null) {
            final PortalNotificationConfig portalNotificationConfig = new PortalNotificationConfig();
            portalNotificationConfig.setUser(row.getString("user"));
            portalNotificationConfig.setHooks(row.getList("hooks", String.class));
            portalNotificationConfig.setReferenceId(row.getString("reference_id"));
            portalNotificationConfig.setReferenceType(NotificationReferenceType.valueOf(row.getString("reference_type").toUpperCase()));
            portalNotificationConfig.setCreatedAt(row.getTimestamp("created_at"));
            portalNotificationConfig.setUpdatedAt(row.getTimestamp("updated_at"));
            return portalNotificationConfig;
        }
        return null;
    }

    @Override
    public Optional<PortalNotificationConfig> findById(String user, NotificationReferenceType referenceType, String referenceId) throws TechnicalException {
        if (user == null || referenceType == null || referenceId == null)
            return Optional.empty();
        LOGGER.debug("findById({}, {}, {})", user, referenceType, referenceId);
        return Optional.ofNullable(fromRow(session.execute(selectStmt.bind(user, referenceType == null ? null : referenceType.toString(), referenceId)).one()));
    }


    @Override
    public void delete(PortalNotificationConfig portalNotificationConfig) throws TechnicalException {
        if (portalNotificationConfig == null)
            throw new IllegalStateException("portalNotificationConfig key is null");
        LOGGER.debug("Delete with User [{}] & Reference ID [{}]", portalNotificationConfig.getUser(), portalNotificationConfig.getReferenceId());
        session.execute(deleteStmt.bind(
                portalNotificationConfig.getUser(),
                portalNotificationConfig.getReferenceType() == null ? null : portalNotificationConfig.getReferenceType().toString(),
                portalNotificationConfig.getReferenceId()));
    }

    @Override
    public List<PortalNotificationConfig> findByReferenceAndHook(String hook, NotificationReferenceType referenceType, String referenceId) throws TechnicalException {
        LOGGER.debug("ElassandraPortalNotificationConfigRepository.findByReferenceAndHook({}, {}, {})", hook, referenceType, referenceId);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                    .filter(QueryBuilders.termQuery("hooks", hook))
                    .filter(QueryBuilders.termQuery("reference_type", referenceType))
                    .filter(QueryBuilders.termQuery("reference_id", referenceId));
            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder)
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find notifications by reference and hook";
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }


}
