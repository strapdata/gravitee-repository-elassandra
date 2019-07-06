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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.RatingAnswerRepository;
import io.gravitee.repository.management.model.RatingAnswer;

/**
 * @author vroyer
 */
@Repository
public class ElassandraRatingAnswerRepository extends ElassandraCrud<RatingAnswer, String> implements RatingAnswerRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraRatingAnswerRepository.class);

    public ElassandraRatingAnswerRepository() throws IOException {
        super("ratinganswers",
                new String[] {"id","user","comment", "rating", "created_at", "updated_at"},
                "ratinganswers",
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
                        .startObject("user").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("comment").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("rating").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(RatingAnswer t) {
        return new Object[] {
                t.getId(),
                t.getUser(),
                t.getComment(),
                t.getRating(),
                t.getCreatedAt(),
                t.getUpdatedAt()
        };
    }

    @Override
    public RatingAnswer fromRow(Row row) {
        if (row != null) {
            final RatingAnswer ratingAnswer = new RatingAnswer();
            ratingAnswer.setId(row.getString("id"));
            ratingAnswer.setRating(row.getString("rating"));
            ratingAnswer.setUser(row.getString("user"));
            ratingAnswer.setComment(row.getString("comment"));
            ratingAnswer.setCreatedAt(row.getTimestamp("created_at"));
            ratingAnswer.setUpdatedAt(row.getTimestamp("updated_at"));
            return ratingAnswer;
        }
        return null;
    }


    @Override
    public List<RatingAnswer> findByRating(String rating) throws TechnicalException {
        LOGGER.debug("Find Rating by rating [{}]", rating);
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("rating", rating))
                .toString(ToXContent.EMPTY_PARAMS);
        final List<Row> rows = session.execute(this.esQueryStmt.bind(esQuery)).all();
        return rows.stream().map(this::fromRow).collect(toList());
    }
}
