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
