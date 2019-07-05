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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.RatingRepository;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Audit;
import io.gravitee.repository.management.model.Rating;

/**
 * @author vroyer
 */
@Repository
public class ElassandraRatingRepository extends ElassandraCrud<Rating, String> implements RatingRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraRatingRepository.class);

    public ElassandraRatingRepository() throws IOException {
        super("ratings",
                new String[]{"id", "api", "user", "rate", "title", "comment", "created_at", "updated_at"},
                "ratings",
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
                        .startObject("api").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("user").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("rate").field("type", "byte").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("title").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("comment").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Rating rating) {
        return new Object[]{
                rating.getId(),
                rating.getApi(),
                rating.getUser(),
                rating.getRate(),
                rating.getTitle(),
                rating.getComment(),
                rating.getCreatedAt(),
                rating.getUpdatedAt()};
    }

     @Override
    public Rating fromRow(Row row) {
        if (row != null) {
            final Rating rating = new Rating();
            rating.setId(row.getString("id"));
            rating.setApi(row.getString("api"));
            rating.setUser(row.getString("user"));
            rating.setRate(row.getByte("rate"));
            rating.setTitle(row.getString("title"));
            rating.setComment(row.getString("comment"));
            rating.setCreatedAt(row.getTimestamp("created_at"));
            rating.setUpdatedAt(row.getTimestamp("updated_at"));
            return rating;
        }
        return null;
    }

    private static final String RATINGS_TABLE = "ratings";

    @Autowired
    private Session session;

    @Override
    public Page<Rating> findByApiPageable(String api, Pageable pageable) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}] pageable={}", api, pageable);

        SearchSourceBuilder esQueryBuilder = new SearchSourceBuilder()
                .query(api == null ? QueryBuilders.matchAllQuery() : QueryBuilders.termQuery("api", api))
                .sort("created_at",SortOrder.DESC);

        int pageSize = 1000;
        if (pageable != null) {
            if (pageable.from() > 0)
                esQueryBuilder.from(pageable.from());
            if (pageable.pageSize() > 0) {
                pageSize = pageable.pageSize();
                esQueryBuilder.size(pageable.pageSize());
            }
        }
        String esQuery = esQueryBuilder.toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("es_query={}", esQuery);

        final ResultSet resultSet = session.execute(this.esQueryStmtWithLimit.bind(esQuery, pageSize));
        ElasticIncomingPayload payload = new ElasticIncomingPayload(resultSet.getExecutionInfo().getIncomingPayload());
        List<Rating> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        return new Page<>(result,
                        (pageable == null) ? 0 : pageable.pageNumber(),
                        result.size(),
                        payload.hitTotal);
    }

    @Override
    public List<Rating> findByApi(String api) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}]", api);
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("api", api))
                .toString(ToXContent.EMPTY_PARAMS);
        return session.execute(this.esQueryStmt.bind(esQuery)).all()
                .stream().map(this::fromRow).collect(Collectors.toList());
    }

    @Override
    public Optional<Rating> findByApiAndUser(String api, String user) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}] and user [{}]", api, user);
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("api", api))
                .filter(QueryBuilders.termQuery("user", user));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);

        return Optional.ofNullable(fromRow(session.execute(this.esQueryStmt.bind(esQuery)).one()));
    }

}
