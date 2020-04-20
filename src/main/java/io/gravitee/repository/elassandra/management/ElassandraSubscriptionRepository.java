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

import static org.springframework.util.CollectionUtils.isEmpty;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.springframework.util.StringUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.SubscriptionRepository;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.api.search.SubscriptionCriteria;
import io.gravitee.repository.management.model.Subscription;

/**
 * @author vroyer
 */
@Repository
public class ElassandraSubscriptionRepository extends ElassandraCrud<Subscription, String> implements SubscriptionRepository, PageableRepository<Subscription> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraSubscriptionRepository.class);

    public ElassandraSubscriptionRepository() throws IOException {
        super("subscriptions",
                new String[] {
                        "id",
                        "created_at",
                        "updated_at",
                        "starting_at",
                        "ending_at",
                        "processed_at",
                        "processed_by",
                        "subscribed_by",
                        "application",
                        "plan",
                        "reason",
                        "status",
                        "request",
                        "closed_at",
                        "paused_at",
                        "api",
                        "client_id"},
                "subscriptions",
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
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("processed_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("starting_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("ending_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("paused_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("closed_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("processed_by").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("subscribed_by").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("application").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("plan").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("email").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("reason").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("status").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("request").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("api").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("client_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Subscription t) {
        return new Object[]{
                t.getId(),
                t.getCreatedAt(),
                t.getUpdatedAt(),
                t.getStartingAt(),
                t.getEndingAt(),
                t.getProcessedAt(),
                t.getProcessedBy(),
                t.getSubscribedBy(),
                t.getApplication(),
                t.getPlan(),
                t.getReason(),
                t.getStatus() == null ? null : t.getStatus().toString(),
                t.getRequest(),
                t.getClosedAt(),
                t.getPausedAt(),
                t.getApi(),
                t.getClientId()
        };
    }

    @Override
    public Subscription fromRow(Row row) {
        if (row != null) {
            final Subscription subscription = new Subscription();
            subscription.setId(row.getString("id"));
            subscription.setCreatedAt(row.getTimestamp("created_at"));
            subscription.setUpdatedAt(row.getTimestamp("updated_at"));
            subscription.setStartingAt(row.getTimestamp("starting_at"));
            subscription.setEndingAt(row.getTimestamp("ending_at"));
            subscription.setProcessedAt(row.getTimestamp("processed_at"));
            subscription.setProcessedBy(row.getString("processed_by"));
            subscription.setSubscribedBy(row.getString("subscribed_by"));
            subscription.setApplication(row.getString("application"));
            subscription.setPlan(row.getString("plan"));
            subscription.setReason(row.getString("reason"));
            subscription.setRequest(row.getString("request"));
            subscription.setClosedAt(row.getTimestamp("closed_at"));
            subscription.setPausedAt(row.getTimestamp("paused_at"));
            subscription.setStatus(row.getString("status") == null ?  null : Subscription.Status.valueOf(row.getString("status")));
            subscription.setApi(row.getString("api"));
            subscription.setClientId(row.getString("client_id"));
            return subscription;
        }
        return null;
    }

    public Set<Subscription> findByPlan(String plan) throws TechnicalException {
        LOGGER.debug("Find Subscription by plan [{}]", plan);
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("plan", plan))
                .toString(ToXContent.EMPTY_PARAMS);
        return session.execute(this.esQueryStmt.bind(esQuery)).all()
                .stream().map(this::fromRow).collect(Collectors.toSet());
    }

    public Set<Subscription> findByApplication(String application) throws TechnicalException {
        LOGGER.debug("Find Subscription by application [{}]", application);
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("application", application))
                .toString(ToXContent.EMPTY_PARAMS);
        return session.execute(this.esQueryStmt.bind(esQuery)).all()
                .stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Page<Subscription> search(SubscriptionCriteria criteria, Pageable pageable) throws TechnicalException {
        return searchPage(criteria, pageable);
    }

    @Override
    public List<Subscription> search(SubscriptionCriteria criteria) throws TechnicalException {
        return searchPage(criteria, null).getContent();
    }

    private Page<Subscription> searchPage(final SubscriptionCriteria criteria, final Pageable pageable) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (criteria != null) {
            if (criteria.getFrom() > 0 || criteria.getTo() > 0) {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("updated_at");
                if (criteria.getFrom() > 0)
                    rangeQueryBuilder.gte(new Date(criteria.getFrom()));
                if (criteria.getTo() > 0)
                    rangeQueryBuilder.lte(new Date(criteria.getTo()));
                queryBuilder.filter(rangeQueryBuilder);
            }

            if (!StringUtils.isEmpty(criteria.getClientId()))
                queryBuilder.filter(QueryBuilders.termQuery("client_id", criteria.getClientId()));

            if (criteria.getPlans() != null && !criteria.getPlans().isEmpty())
                queryBuilder.filter(QueryBuilders.termsQuery("plan", criteria.getPlans()));

            if (criteria.getApplications() != null && !criteria.getApplications().isEmpty())
                queryBuilder.filter(QueryBuilders.termsQuery("application", criteria.getApplications()));

            if (criteria.getApis() != null && !criteria.getApis().isEmpty())
                queryBuilder.filter(QueryBuilders.termsQuery("api", criteria.getApis()));

            if (!isEmpty(criteria.getStatuses())) {
                final Collection<String> statuses = criteria.getStatuses().stream().map(Enum::name).collect(Collectors.toList());
                queryBuilder.filter(QueryBuilders.termsQuery("status", statuses));
            }
        }

        SearchSourceBuilder esQueryBuilder = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .sort("created_at", SortOrder.DESC);

        int pageSize = 1000;
        if (pageable != null) {
            if (pageable.from() >= 0)
                esQueryBuilder.from(pageable.from());
            if (pageable.pageSize() > 0) {
                pageSize = pageable.pageSize();
            }
        }
        esQueryBuilder.size(pageSize);

        String esQuery = esQueryBuilder.toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("es_query={}", esQuery);

        final ResultSet resultSet = session.execute(this.esQueryStmtWithLimit.bind(esQuery, pageSize));
        ElasticIncomingPayload payload = new ElasticIncomingPayload(resultSet.getExecutionInfo().getIncomingPayload());
        List<Subscription> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        return new Page<>(result,
                        (pageable == null) ? 0 : pageable.pageNumber(),
                        result.size(),
                        payload.hitTotal);
    }
}
