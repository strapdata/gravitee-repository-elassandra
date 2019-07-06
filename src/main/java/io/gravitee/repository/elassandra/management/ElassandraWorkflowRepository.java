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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.WorkflowRepository;
import io.gravitee.repository.management.model.Workflow;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraWorkflowRepository extends ElassandraCrud<Workflow, String> implements WorkflowRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraWorkflowRepository.class);

    public ElassandraWorkflowRepository() throws IOException {
        super("workflows",
                new String[] {"id","reference_type","reference_id", "type", "state", "comment", "user","created_at"},
                "workflows",
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
                        .startObject("name").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("reference_type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("reference_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("state").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("comment").field("type", "text").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("user").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Workflow workflow) {
        return new Object[] {
                workflow.getId(),
                workflow.getReferenceType(),
                workflow.getReferenceId(),
                workflow.getType(),
                workflow.getState(),
                workflow.getComment(),
                workflow.getUser(),
                workflow.getCreatedAt()
        };
    }

    // CREATE TABLE workflows(id text primary key, name text, notifier text, config text, hooks list<text>, reference_type text, reference_id text, created_at timestamp, updatedAt timestamp, use_system_proxy boolean)
    @Override
    public Workflow fromRow(Row row) {
        if (row != null) {
            final Workflow workflow = new Workflow();
            workflow.setId(row.getString("id"));
            workflow.setReferenceType(row.getString("reference_type"));
            workflow.setReferenceId(row.getString("reference_id"));
            workflow.setType(row.getString("type"));
            workflow.setState(row.getString("state"));
            workflow.setComment(row.getString("comment"));
            workflow.setUser(row.getString("user"));
            workflow.setCreatedAt(row.getTimestamp("created_at"));
            return workflow;
        }
        return null;
    }


    @Override
    public List<Workflow> findByReferenceAndType(String referenceType, String referenceId, String type)
            throws TechnicalException {
        LOGGER.debug("Search by [{}][{}][{}],", referenceType, referenceId, type);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (referenceType != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));
            if (referenceId != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_id", referenceId));
            if (type != null)
                queryBuilder.filter(QueryBuilders.termsQuery("type", type));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .sort("created_at", SortOrder.DESC)
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("es_query={}", esQuery);

            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            LOGGER.error("Failed to findByReferenceAndType:", ex);
            throw new TechnicalException("Failed to findByReferenceAndType", ex);
        }
    }

}
