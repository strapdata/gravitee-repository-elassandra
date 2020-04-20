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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Strings;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.DashboardRepository;
import io.gravitee.repository.management.model.Dashboard;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class ElassandraDashboardRepository extends ElassandraCrud<Dashboard, String> implements DashboardRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraDashboardRepository.class);

    public ElassandraDashboardRepository() throws IOException {
        super("dashboards",
                new String[]{"id", "reference_type", "reference_id", "name", "query_filter", "definition", "dashboard_order", "enabled", "created_at", "updated_at"},
                new String[]{"text", "text", "text", "text", "text", "text", "int", "boolean", "timestamp", "timestamp"},
                "dashboards",
                Settings.builder().put("synchronous_refresh", true),
                // in this repository we search only by reference_type but in SNAPSHOT version of gravitee,
                // find by ID will be implemented... so keep id a partition/primary key
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("id")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .field("cql_primary_key_order", 0)
                        .field("cql_partition_key", true)
                        .endObject()
                        .startObject("reference_type")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .endObject()
                        .startObject("reference_id")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .endObject()
                        .startObject("name")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .endObject()
                        .startObject("dashboard_order")
                        .field("type", "integer")
                        .field("cql_collection", "singleton")
                        .endObject()
                        .endObject()
                        .endObject());
    }

    @Override
    public Object[] values(Dashboard dashboard) {
        return new Object[] {
                dashboard.getId(),
                dashboard.getReferenceType(),
                dashboard.getReferenceId(),
                dashboard.getName(),
                dashboard.getQueryFilter(),
                dashboard.getDefinition(),
                dashboard.getOrder(),
                dashboard.isEnabled(),
                dashboard.getCreatedAt(),
                dashboard.getUpdatedAt()
        };
    }

    @Override
    public Dashboard fromRow(Row row) {
        Dashboard dashboard = new Dashboard();
        dashboard.setId(row.getString("id"));
        dashboard.setReferenceType(row.getString("reference_type"));
        dashboard.setReferenceId(row.getString("reference_id"));
        dashboard.setName(row.getString("name"));
        dashboard.setQueryFilter(row.getString("query_filter"));
        dashboard.setDefinition(row.getString("definition"));
        dashboard.setOrder(row.getInt("dashboard_order"));
        dashboard.setEnabled(row.getBool("enabled"));
        dashboard.setCreatedAt(row.getTimestamp("created_at"));
        dashboard.setUpdatedAt(row.getTimestamp("updated_at"));
        return dashboard;
    }

    @Override
    public List<Dashboard> findByReferenceType(String referenceType) throws TechnicalException {
        LOGGER.debug("Find by ReferenceType [{}]", referenceType);
        if (Strings.isNullOrEmpty(referenceType)) {
            return Collections.EMPTY_LIST;
        }

        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));

            String esQuery = new SearchSourceBuilder()
                    .sort("dashboard_order", SortOrder.ASC)
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);

            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find by referenceType="+referenceType;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }
}