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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.join.ScoreMode;
import org.elassandra.index.ElasticIncomingPayload;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.management.api.EventRepository;
import io.gravitee.repository.management.api.search.EventCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Event;
import io.gravitee.repository.management.model.Event.EventProperties;
import io.gravitee.repository.management.model.EventType;

/**
 * @author vroyer
 */
@Repository
public class ElassandraEventRepository extends ElassandraCrud<Event, String> implements EventRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraEventRepository.class);

    public ElassandraEventRepository() throws IOException {
        super("events",
                new String[]{"id", "type", "payload", "parent_id", "event_properties", "created_at", "updated_at"},
                new String[]{"text", "text", "text", "text", "map<text,text>", "timestamp", "timestamp"},
                "events",
                Settings.builder(),
                XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 0)
                            .field("cql_partition_key", true)
                        .endObject()
                        .startObject("type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("payload").field("type", "text").field("cql_collection", "singleton").endObject()
                        .startObject("parent_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("event_properties").field("type", "nested").field("cql_collection", "singleton").field("cql_struct", "map")
                        .startObject("properties")
                          .startObject(EventProperties.ID.name().toLowerCase()).field("type", "keyword").endObject()
                          .startObject(EventProperties.API_ID.name().toLowerCase()).field("type", "keyword").endObject()
                          .startObject(EventProperties.DICTIONARY_ID.name().toLowerCase()).field("type", "keyword").endObject()
                          .startObject(EventProperties.ORIGIN.name().toLowerCase()).field("type", "keyword").endObject()
                          .startObject(EventProperties.USER.name().toLowerCase()).field("type", "keyword").endObject()
                        .endObject()
                      .endObject()
                    .endObject()
                .endObject()
                );
    }

    @Override
    public Object[] values(Event event) {
        return new Object[]{
                event.getId(),
                event.getType() == null ? null : event.getType().toString(),
                event.getPayload(),
                event.getParentId(),
                event.getProperties(),
                event.getCreatedAt(),
                event.getUpdatedAt()};
    }

    @Override
    public Event fromRow(Row row) {
        if (row != null) {
            final Event event = new Event();
            event.setId(row.getString("id"));
            event.setType(EventType.valueOf(row.getString("type").toUpperCase()));
            event.setPayload(row.getString("payload"));
            event.setParentId(row.getString("parent_id"));
            event.setProperties(row.getMap("event_properties", String.class, String.class));
            event.setCreatedAt(row.getTimestamp("created_at"));
            event.setUpdatedAt(row.getTimestamp("updated_at"));
            return event;
        }
        return null;
    }

    private String buildQuery(final EventCriteria criteria, Pageable pageable) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (criteria != null) {
            if (criteria.getFrom() > 0 || criteria.getTo() > 0) {
                RangeQueryBuilder rqb = QueryBuilders.rangeQuery("updated_at");
                if (criteria.getFrom() > 0)
                    rqb.gte(new Date(criteria.getFrom()));
                if (criteria.getTo() > 0)
                    rqb.lt(new Date(criteria.getTo()));
                queryBuilder.filter(rqb);
            }
            if (criteria.getTypes() != null && !criteria.getTypes().isEmpty())
                queryBuilder.filter(QueryBuilders.termsQuery("type",
                        criteria.getTypes().stream().map(EventType::name).collect(Collectors.toList())));
            if (criteria.getProperties() != null && !criteria.getProperties().isEmpty()) {
                for(Map.Entry<String, Object> entry : criteria.getProperties().entrySet()) {
                    if (entry.getValue() instanceof Collection) {
                        Collection collection = (Collection) entry.getValue();
                        queryBuilder.should(
                                QueryBuilders.nestedQuery("event_properties",
                                    QueryBuilders.termsQuery("event_properties." + entry.getKey(), collection.toArray()), ScoreMode.None));
                    } else {
                        queryBuilder.should(
                            QueryBuilders.nestedQuery("event_properties",
                                QueryBuilders.termQuery("event_properties." + entry.getKey(), entry.getValue()), ScoreMode.None));
                    }
                }
                queryBuilder.minimumShouldMatch(1);
            }
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .sort("updated_at", SortOrder.DESC);

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
        return esQuery;
    }

    @Override
    public List<Event> search(EventCriteria criteria) {
        LOGGER.debug("criteria={}", criteria);
        String esQuery = buildQuery(criteria, null);
        final ResultSet resultSet = session.execute(this.esQueryStmtWithLimit.bind(esQuery, 1000));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    @Override
    public  Page<Event> search(final EventCriteria criteria, Pageable pageable) {
        LOGGER.debug("criteria={} pageable={}", criteria, pageable);
        String esQuery = buildQuery(criteria, pageable);
        final ResultSet resultSet = session.execute(this.esQueryStmtWithLimit.bind(esQuery, pageable == null ? 1000 : pageable.pageSize()));
        ElasticIncomingPayload payload = new ElasticIncomingPayload(resultSet.getExecutionInfo().getIncomingPayload());
        List<Event> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        return new Page<>(result,
                        (pageable == null) ? 0 : pageable.pageNumber(),
                        result.size(),
                        payload.hitTotal);
    }
}
