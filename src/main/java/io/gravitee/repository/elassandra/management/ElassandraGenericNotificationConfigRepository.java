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
import io.gravitee.repository.management.api.GenericNotificationConfigRepository;
import io.gravitee.repository.management.model.GenericNotificationConfig;
import io.gravitee.repository.management.model.Metadata;
import io.gravitee.repository.management.model.MetadataReferenceType;
import io.gravitee.repository.management.model.NotificationReferenceType;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraGenericNotificationConfigRepository extends ElassandraCrud<GenericNotificationConfig, String> implements GenericNotificationConfigRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraGenericNotificationConfigRepository.class);

    public ElassandraGenericNotificationConfigRepository() throws IOException {
        super( "generic_notification_configs",
                new String[]{"id", "name", "notifier","config", "hooks", "reference_id", "reference_type", "use_system_proxy", "created_at", "updated_at"},
                new String[]{"text", "text", "text","text", "list<text>", "text", "text", "boolean", "timestamp", "timestamp"},
                "generic_notification_configs",
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
                        .startObject("hooks").field("type", "keyword").field("cql_collection", "list").endObject()
                        .startObject("reference_type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("reference_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject());

    }

    @Override
    public Object[] values(GenericNotificationConfig genericNotificationConfig) {
        return new Object[]{genericNotificationConfig.getId(),
                genericNotificationConfig.getName(),
                genericNotificationConfig.getNotifier(),
                genericNotificationConfig.getConfig(),
                genericNotificationConfig.getHooks(),
                genericNotificationConfig.getReferenceId(),
                genericNotificationConfig.getReferenceType() == null ? null : genericNotificationConfig.getReferenceType().name(),
                genericNotificationConfig.isUseSystemProxy(),
                genericNotificationConfig.getCreatedAt(),
                genericNotificationConfig.getUpdatedAt()};
    }

    @Override
    public GenericNotificationConfig fromRow(Row row) {
        if (row != null) {
            final GenericNotificationConfig genericNotificationConfig = new GenericNotificationConfig();
            genericNotificationConfig.setId(row.getString("id"));
            genericNotificationConfig.setName(row.getString("name"));
            genericNotificationConfig.setNotifier(row.getString("notifier"));
            genericNotificationConfig.setConfig(row.getString("config"));
            genericNotificationConfig.setHooks(row.getList("hooks", String.class));
            genericNotificationConfig.setReferenceId(row.getString("reference_id"));
            genericNotificationConfig.setReferenceType(NotificationReferenceType.valueOf(row.getString("reference_type").toUpperCase()));
            genericNotificationConfig.setCreatedAt(row.getTimestamp("created_at"));
            genericNotificationConfig.setUpdatedAt(row.getTimestamp("updated_at"));
            genericNotificationConfig.setUseSystemProxy(row.isNull("use_system_proxy") ? null : row.getBool("use_system_proxy"));
            return genericNotificationConfig;
        }
        return null;
    }

    @Override
    public List<GenericNotificationConfig> findByReferenceAndHook(String hook, NotificationReferenceType referenceType,
            String referenceId) throws TechnicalException {
        LOGGER.debug("Find GenericNotificationConfig by [{}]-[{}]-[{}]", hook, referenceType, referenceId);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (hook != null)
                queryBuilder.filter(QueryBuilders.termQuery("hooks", hook));
            if (referenceType != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType.name()));
            if (referenceId != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_id", referenceId));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find GenericNotificationConfig by hook="+hook+" referenceType="+referenceType+" referenceId="+referenceId;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

    @Override
    public List<GenericNotificationConfig> findByReference(NotificationReferenceType referenceType, String referenceId)
            throws TechnicalException {
        LOGGER.debug("Find GenericNotificationConfig by [{}]-[{}]", referenceType, referenceId);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (referenceType != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType.name()));
            if (referenceId != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_id", referenceId));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find GenericNotificationConfig by referenceType="+referenceType+" referenceId="+referenceId;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

}
