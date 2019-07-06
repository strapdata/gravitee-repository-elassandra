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


import static com.datastax.driver.core.querybuilder.QueryBuilder.in;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.UserRepository;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.api.search.UserCriteria;
import io.gravitee.repository.management.model.Audit;
import io.gravitee.repository.management.model.User;
import io.gravitee.repository.management.model.UserStatus;

/**
 * @author vroyer
 */
@Repository
public class ElassandraUserRepository extends ElassandraCrud<User, String> implements UserRepository, PageableRepository<User> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraUserRepository.class);

    public ElassandraUserRepository() throws IOException {
        super("users",
                new String[] {"id","source","source_id", "password", "email", "firstname", "lastname","created_at","updated_at", "last_connection_at", "picture", "status"},
                "users",
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
                        .startObject("source").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("source_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("password").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("email").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("firstname").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("lastname").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("last_connection_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("picture").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("status").field("type", "keyword").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject()
                );
    }

    @Override
    public Object[] values(User t) {
        return new Object[] {
                t.getId(),
                t.getSource(),
                t.getSourceId(),
                t.getPassword(),
                t.getEmail(),
                t.getFirstname(),
                t.getLastname(),
                t.getCreatedAt(),
                t.getUpdatedAt(),
                t.getLastConnectionAt(),
                t.getPicture(),
                t.getStatus() == null ? null : t.getStatus().name()
        };
    }

    @Override
    public User fromRow(Row row) {
        if (row != null) {
            final User t = new User();
            t.setId(row.getString("id"));
            t.setSource(row.getString("source"));
            t.setSourceId(row.getString("source_id"));
            t.setPassword(row.getString("password"));
            t.setEmail(row.getString("email"));
            t.setFirstname(row.getString("firstname"));
            t.setLastname(row.getString("lastname"));
            t.setCreatedAt(row.getTimestamp("created_at"));
            t.setUpdatedAt(row.getTimestamp("updated_at"));
            t.setLastConnectionAt(row.getTimestamp("last_connection_at"));
            t.setPicture(row.getString("picture"));
            t.setStatus(row.getString("status") == null ? null : UserStatus.valueOf(row.getString("status")));
            return t;
        }
        return null;
    }



    @Override
    public Set<User> findByIds(List<String> ids) throws TechnicalException {
        LOGGER.debug("Find User by Username list {}", ids);

        final String[] lastId = new String[1];
        List<String> uniqueIds = ids.stream().filter(id -> {
            if (id.equals(lastId[0])) {
                return false;
            } else {
                lastId[0] = id;
                return true;
            }
        }).collect(Collectors.toList());

        final Statement select = QueryBuilder.select().all().from(tableName).where(in("id", uniqueIds));
        final ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    // TODO: upper case sourceId match ?
    public Optional<User> findBySource(String source, String sourceId) throws TechnicalException {
        LOGGER.debug("Find User by source [{}]", source);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (source != null ) queryBuilder.filter(QueryBuilders.termQuery("source", source));
        if (sourceId != null) queryBuilder.should(QueryBuilders.termQuery("source_id", sourceId));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        final Row row = session.execute(this.esQueryStmt.bind(esQuery)).one();
        return Optional.ofNullable(fromRow(row));
    }

    @Override
    public Page<User> search(UserCriteria criteria, Pageable pageable) throws TechnicalException {
        LOGGER.debug("Search User by criteria [{}]", criteria);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (criteria != null) {
                if (criteria.getStatuses() != null && criteria.getStatuses().length > 0) {
                    List<UserStatus> statuses = Arrays.asList(criteria.getStatuses());
                    queryBuilder.filter(QueryBuilders.termsQuery("status",
                            statuses.stream().map(UserStatus::name).collect(Collectors.toList())));
                }
                if (criteria.hasNoStatus()) {
                    queryBuilder.mustNot(QueryBuilders.existsQuery("status"));
                }
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .sort("lastname", SortOrder.ASC)
                    .sort("firstname", SortOrder.ASC);

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
            List<User> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
            return new Page<>(result,
                            (pageable == null) ? 0 : pageable.pageNumber(),
                            result.size(),
                            payload.hitTotal);
        } catch (final Exception ex) {
            LOGGER.error("Failed to search users:", ex);
            throw new TechnicalException("Failed to search users", ex);
        }
    }
}
