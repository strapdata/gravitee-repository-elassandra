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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.ApiKeyRepository;
import io.gravitee.repository.management.api.search.ApiKeyCriteria;
import io.gravitee.repository.management.model.ApiKey;

/**
 * @author vroyer
 */
@Repository
public class ElassandraApiKeyRepository extends ElassandraCrud<ApiKey, String> implements ApiKeyRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraApiKeyRepository.class);

    public ElassandraApiKeyRepository() throws IOException {
        super("apikeys",
              new String[] { "key", "subscription", "application", "plan", "expire_at", "created_at", "updated_at", "revoked_at", "revoked", "paused" },
              "apikeys",
              Settings.builder().put("synchronous_refresh", true),
              XContentFactory.jsonBuilder()
              .startObject()
                  .startObject("properties")
                      .startObject("key")
                          .field("type", "keyword")
                          .field("cql_collection", "singleton")
                          .field("cql_primary_key_order", 0)
                          .field("cql_partition_key", true)
                      .endObject()
                      .startObject("subscription").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("application").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("plan").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("expire_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("revoked_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("revoked").field("type", "boolean").field("cql_collection", "singleton").endObject()
                      .startObject("paused").field("type", "boolean").field("cql_collection", "singleton").endObject()
                  .endObject()
              .endObject()
              );
    }

    @Override
    public Object[] values(ApiKey apiKey) {
        return new Object[] {
                apiKey.getKey(),
                apiKey.getSubscription(),
                apiKey.getApplication(),
                apiKey.getPlan(),
                apiKey.getExpireAt(),
                apiKey.getCreatedAt(),
                apiKey.getUpdatedAt(),
                apiKey.getRevokedAt(),
                apiKey.isRevoked(),
                apiKey.isPaused()
        };
    }

    @Override
    public ApiKey fromRow(Row row) {
        if (row != null) {
            final ApiKey apiKey = new ApiKey();
            apiKey.setKey(row.getString("key"));
            apiKey.setSubscription(row.getString("subscription"));
            apiKey.setApplication(row.getString("application"));
            apiKey.setPlan(row.getString("plan"));
            apiKey.setExpireAt(row.getTimestamp("expire_at"));
            apiKey.setCreatedAt(row.getTimestamp("created_at"));
            apiKey.setUpdatedAt(row.getTimestamp("updated_at"));
            apiKey.setRevokedAt(row.getTimestamp("revoked_at"));
            apiKey.setRevoked(row.isNull("revoked") ? null : row.getBool("revoked"));
            apiKey.setPaused(row.isNull("paused") ? null : row.getBool("paused"));
            return apiKey;
        }
        return null;
    }

    @Override
    public Set<ApiKey> findBySubscription(String subscription) throws TechnicalException {
        LOGGER.debug("Find ApiKey by Subscription [{}]", subscription);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (subscription != null)
            queryBuilder.filter(QueryBuilders.termQuery("subscription", subscription));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .toString(ToXContent.EMPTY_PARAMS);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<ApiKey> findByPlan(String plan) throws TechnicalException {
        LOGGER.debug("Find ApiKey by Plan [{}]", plan);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (plan != null)
            queryBuilder.filter(QueryBuilders.termQuery("plan", plan));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .toString(ToXContent.EMPTY_PARAMS);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public List<ApiKey> findByCriteria(ApiKeyCriteria filter) throws TechnicalException {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (filter != null) {
            if (filter.getPlans() != null)
                queryBuilder.filter(QueryBuilders.termsQuery("plan", filter.getPlans()));
            if (!filter.isIncludeRevoked())
                queryBuilder.filter(QueryBuilders.termQuery("revoked", false));
            if (filter.getFrom() > 0 || (filter.getTo() > 0)) {
                RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("updated_at");
                if (filter.getFrom() > 0)
                    rangeQuery.gte(new Date(filter.getFrom()));
                if (filter.getTo() > 0)
                    rangeQuery.lte(new Date(filter.getTo()));
                queryBuilder.filter(rangeQuery);
            }
        }
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .sort("updated_at", SortOrder.DESC)
                .toString(ToXContent.EMPTY_PARAMS);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

}
