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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsSetQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.CommandRepository;
import io.gravitee.repository.management.api.search.CommandCriteria;
import io.gravitee.repository.management.model.Command;
import io.gravitee.repository.management.model.User;
import io.gravitee.repository.management.model.UserStatus;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraCommandRepository extends ElassandraCrud<Command, String> implements CommandRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraCommandRepository.class);

    public ElassandraCommandRepository() throws IOException {
        super("commands",
              new String[]{"id", "xfrom", "xto", "tags", "content", "acknowledgments", "expired_at", "created_at", "updated_at"},
              "commands",
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
                      .startObject("xfrom").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("xto").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("tags").field("type", "keyword").field("cql_collection", "list").endObject()
                      .startObject("content").field("type", "keyword").field("cql_collection", "singleton").endObject()
                      .startObject("expired_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                      .startObject("acknowledgments").field("type", "keyword").field("cql_collection", "list").endObject()
                  .endObject()
              .endObject());
    }

    @Override
    public Object[] values(Command command) {
        return new Object[]{
                command.getId(),
                command.getFrom(),
                command.getTo(),
                command.getTags(),
                command.getContent(),
                command.getAcknowledgments(),
                command.getExpiredAt(),
                command.getCreatedAt(),
                command.getUpdatedAt()};
    }

    @Override
    public Command fromRow(Row row) {
        if (row != null) {
            final Command command = new Command();
            command.setId(row.getString("id"));
            command.setFrom(row.getString("xfrom"));
            command.setTo(row.getString("xto"));
            command.setTags(row.getList("tags", String.class));
            command.setContent(row.getString("content"));
            command.setAcknowledgments(row.getList("acknowledgments", String.class));
            command.setExpiredAt(row.getTimestamp("expired_at"));
            command.setCreatedAt(row.getTimestamp("created_at"));
            command.setUpdatedAt(row.getTimestamp("updated_at"));
            return command;
        }
        return null;
    }

    /**
     * private String to;
    private String[] tags;
    private boolean notExpired;
    private String notFrom;
    private String notAckBy;
     */
    @Override
    public List<Command> search(CommandCriteria criteria) {
        LOGGER.debug("Search Command by criteria [{}]", criteria);
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (criteria != null) {
            if (criteria.getTo() != null)
                queryBuilder.filter(QueryBuilders.termQuery("xto", criteria.getTo()));
            if (criteria.getTags() != null) {
                for(String tag : criteria.getTags())
                    queryBuilder.filter(QueryBuilders.termQuery("tags", tag));
            }
            if (criteria.isNotExpired())
                queryBuilder.filter(QueryBuilders.rangeQuery("expired_at").gte(new Date()));
            if (criteria.getNotAckBy() != null)
                queryBuilder.mustNot(QueryBuilders.termsQuery("acknowledgments", criteria.getNotAckBy()));
            if (criteria.getNotFrom() != null)
                queryBuilder.mustNot(QueryBuilders.termQuery("xfrom", criteria.getNotFrom()));
        }
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("es_query={}", esQuery);

        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

}
