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
package io.gravitee.repository.elassandra.management;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import io.gravitee.repository.management.model.*;
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
    public void deleteByUser(String user) throws TechnicalException {
        if (user == null)
            throw new IllegalStateException("user key is null");

        LOGGER.debug("DeleteByUser [{}]", user);
        session.execute(deletePartitionStmt.bind(user));
    }

    @Override
    public void deleteReference(NotificationReferenceType referenceType, String referenceId) throws TechnicalException {
        LOGGER.debug("Delete Reference [{}]-[{}]", referenceType, referenceId);

        if (referenceType == null || Strings.isNullOrEmpty(referenceId)) {
            LOGGER.warn("Delete portal_notification_configs requires two parameter, received : [{}]-[{}]", referenceType, referenceId);
            throw new IllegalStateException("deleteReference require two parameter values");
        } else {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                    .filter(QueryBuilders.termsQuery("reference_id", referenceId))
                    .filter(QueryBuilders.termQuery("reference_type", referenceType.toString()));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder)
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            for (Row row : resultSet) {
                PortalNotificationConfig portalNotificationConfig = fromRow(row);
                LOGGER.debug("Delete Reference '{}'",portalNotificationConfig);
                delete(portalNotificationConfig);
            }
        }
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
