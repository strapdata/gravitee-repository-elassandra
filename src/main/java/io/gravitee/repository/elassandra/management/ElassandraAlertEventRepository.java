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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.AlertEventRepository;
import io.gravitee.repository.management.api.AlertTriggerRepository;
import io.gravitee.repository.management.api.search.AlertEventCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.AlertEvent;
import io.gravitee.repository.management.model.AlertTrigger;
import io.gravitee.repository.management.model.User;
import io.gravitee.repository.management.model.UserStatus;
import org.elassandra.index.ElasticIncomingPayload;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraAlertEventRepository extends ElassandraCrud<AlertEvent, String> implements AlertEventRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraAlertEventRepository.class);

    public ElassandraAlertEventRepository() throws IOException {
        super("alert_events",
                new String[] { "id", "alert", "message", "created_at","updated_at" },
                new String[] { "text", "text", "text", "timestamp","timestamp" },
                "alert_events",
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
                        .startObject("alert").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("message").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(AlertEvent alertEvent) {
        return new Object[] {
                alertEvent.getId(),
                alertEvent.getAlert(),
                alertEvent.getMessage(),
                alertEvent.getCreatedAt(),
                alertEvent.getUpdatedAt()
        };
    }

    @Override
    public AlertEvent fromRow(Row row) {
        if (row != null) {
            final AlertEvent alertEvent = new AlertEvent();
            alertEvent.setId(row.getString("id"));
            alertEvent.setAlert(row.getString("alert"));
            alertEvent.setMessage(row.getString("message"));
            alertEvent.setCreatedAt(row.getTimestamp("created_at"));
            alertEvent.setUpdatedAt(row.getTimestamp("updated_at"));
            return alertEvent;
        }
        return null;
    }


    /**
     * Search for {@link AlertEvent} with {@link Pageable} feature.
     *
     * <p>
     * Note that events must be ordered by created date in DESC mode.
     * </p>
     *
     * @param criteria A criteria to search for {@link AlertEvent}.
     * @param pageable If user wants a paginable result. Can be <code>null</code>.
     * @return
     */
    @Override
    public Page<AlertEvent> search(AlertEventCriteria criteria, Pageable pageable) {
        LOGGER.debug("Search AlertEvent by criteria [{}]", criteria);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (criteria != null) {
                if (criteria.getAlert() != null && criteria.getAlert().length() > 0) {
                    queryBuilder.filter(QueryBuilders.termQuery("alert", criteria.getAlert()));
                }
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .sort("created_at", SortOrder.DESC);

            int pageSize = 1000;
            if (pageable != null) {
                if (pageable.from() >= 0)
                    searchSourceBuilder.from(pageable.from());
                if (pageable.pageSize() > 0)
                    pageSize = pageable.pageSize();
            }
            searchSourceBuilder.size(pageSize);

            String esQuery = searchSourceBuilder.toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("es_query={}", esQuery);

            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            ElasticIncomingPayload payload = new ElasticIncomingPayload(resultSet.getExecutionInfo().getIncomingPayload());
            List<AlertEvent> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
            return new Page<>(result,
                    (pageable == null) ? 0 : pageable.pageNumber(),
                    result.size(),
                    payload.hitTotal);
        } catch (final Exception ex) {
            LOGGER.error("Failed to search alertEvents:", ex);
            throw new RuntimeException("Failed to search alertEvents", ex);
        }
    }

    /**
     * delete all events of the provided alert
     *
     * @param alertId
     */
    @Override
    public void deleteAll(String alertId) {
        if (alertId == null)
            throw new IllegalStateException("cannot delete null primary key");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("alert", alertId));
        String esQuery = searchSourceBuilder.toString(ToXContent.EMPTY_PARAMS);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        for(Row row : resultSet)
            session.execute(deleteStmt.bind(row.getString("id")));
    }
}
