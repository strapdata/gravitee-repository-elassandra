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
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.InvitationRepository;
import io.gravitee.repository.management.model.Event;
import io.gravitee.repository.management.model.Invitation;
import io.gravitee.repository.management.model.Workflow;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraInvitationRepository extends ElassandraCrud<Invitation, String> implements InvitationRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraDictionaryRepository.class);

    public ElassandraInvitationRepository() throws IOException {
        super("invitations",
                new String[]{"id", "reference_type", "reference_id", "email", "api_role", "application_role", "created_at", "updated_at"},
                "invitations",
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
                        .startObject("reference_type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("reference_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("email").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("api_role").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("application_role").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Invitation t) {
        return new Object[] {
                t.getId(),
                t.getReferenceType(),
                t.getReferenceId(),
                t.getEmail(),
                t.getApiRole(),
                t.getApplicationRole(),
                t.getCreatedAt(),
                t.getUpdatedAt()
        };
    }

    @Override
    public Invitation fromRow(Row row) {
        if (row != null) {
            final Invitation i = new Invitation();
            i.setId(row.getString("id"));
            i.setReferenceType(row.getString("reference_type"));
            i.setReferenceId(row.getString("reference_id"));
            i.setEmail(row.getString("email"));
            i.setApiRole(row.getString("api_role"));
            i.setApplicationRole(row.getString("application_role"));
            i.setCreatedAt(row.getTimestamp("created_at"));
            i.setUpdatedAt(row.getTimestamp("updated_at"));
            return i;
        }
        return null;
    }

    @Override
    public List<Invitation> findByReference(String referenceType, String referenceId) throws TechnicalException {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("reference_id", referenceId))
                .filter(QueryBuilders.termQuery("reference_type", referenceType));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

}
