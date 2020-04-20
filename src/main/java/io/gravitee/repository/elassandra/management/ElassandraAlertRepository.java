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
import java.util.List;
import java.util.stream.Collectors;

import io.gravitee.repository.management.api.AlertTriggerRepository;
import io.gravitee.repository.management.model.AlertTrigger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraAlertRepository extends ElassandraCrud<AlertTrigger, String> implements AlertTriggerRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraAlertRepository.class);

    public ElassandraAlertRepository() throws IOException {
        super("alerts",
                new String[] { "id", "name", "description", "reference_type", "reference_id", "type", "severity", "definition","enabled","created_at","updated_at" },
                new String[] { "text", "text", "text", "text", "text", "text", "text", "text","boolean","timestamp","timestamp" },
                "alerts",
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
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(AlertTrigger trigger) {
        return new Object[] {
                trigger.getId(),
                trigger.getName(),
                trigger.getDescription(),
                trigger.getReferenceType(),
                trigger.getReferenceId(),
                trigger.getType(),
                trigger.getSeverity(),
                trigger.getDefinition(),
                trigger.isEnabled(),
                trigger.getCreatedAt(),
                trigger.getUpdatedAt()
        };
    }

    @Override
    public AlertTrigger fromRow(Row row) {
        if (row != null) {
            final AlertTrigger trigger = new AlertTrigger();
            trigger.setId(row.getString("id"));
            trigger.setReferenceType(row.getString("reference_type"));
            trigger.setReferenceId(row.getString("reference_id"));
            trigger.setName(row.getString("name"));
            trigger.setSeverity(row.getString("severity"));// severity
            trigger.setType(row.getString("type"));
            trigger.setDescription(row.getString("description"));
            trigger.setDefinition(row.getString("definition")); // definition
            trigger.setEnabled(row.isNull("enabled") ? null : row.getBool("enabled"));
            trigger.setCreatedAt(row.getTimestamp("created_at"));
            trigger.setUpdatedAt(row.getTimestamp("updated_at"));
            return trigger;
        }
        return null;
    }


    @Override
    public List<AlertTrigger> findByReference(String referenceType, String referenceId)
            throws TechnicalException {
        LOGGER.debug("Find by [{}]-[{}]", referenceType, referenceId);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (referenceType != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));
            if (referenceId != null)
                queryBuilder.filter(QueryBuilders.termQuery("reference_id", referenceId));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find by referenceType="+referenceType+" referenceId="+referenceId;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }
}
