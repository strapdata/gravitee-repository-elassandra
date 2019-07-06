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
import java.util.List;
import java.util.stream.Collectors;

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
import io.gravitee.repository.management.api.AlertRepository;
import io.gravitee.repository.management.model.Alert;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraAlertRepository extends ElassandraCrud<Alert, String> implements AlertRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraAlertRepository.class);

    public ElassandraAlertRepository() throws IOException {
        super("alerts",
                new String[] { "id", "name", "description", "reference_type", "reference_id", "type","metric_type","metric","threshold_type","threshold","plan","enabled","created_at","updated_at" },
                new String[] { "text", "text", "text", "text", "text", "text","text","text","text","double","text","boolean","timestamp","timestamp" },
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
    public Object[] values(Alert a) {
        return new Object[] {
                a.getId(),
                a.getName(),
                a.getDescription(),
                a.getReferenceType(),
                a.getReferenceId(),
                a.getType(),
                a.getMetricType(),
                a.getMetric(),
                a.getThresholdType(),
                a.getThreshold(),
                a.getPlan(),
                a.isEnabled(),
                a.getCreatedAt(),
                a.getUpdatedAt()
        };
    }

    @Override
    public Alert fromRow(Row row) {
        if (row != null) {
            final Alert a = new Alert();
            a.setId(row.getString("id"));
            a.setName(row.getString("name"));
            a.setDescription(row.getString("description"));
            a.setReferenceType(row.getString("reference_type"));
            a.setReferenceId(row.getString("reference_id"));
            a.setType(row.getString("type"));
            a.setMetricType(row.getString("metric_type"));
            a.setMetric(row.getString("metric"));
            a.setThresholdType(row.getString("threshold_type"));
            a.setThreshold(row.isNull("threshold") ? null : row.getDouble("threshold"));
            a.setPlan(row.getString("plan"));
            a.setEnabled(row.isNull("enabled") ? null : row.getBool("enabled"));
            a.setCreatedAt(row.getTimestamp("created_at"));
            a.setUpdatedAt(row.getTimestamp("updated_at"));
            return a;
        }
        return null;
    }


    @Override
    public List<Alert> findByReference(String referenceType, String referenceId)
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
