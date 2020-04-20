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
import io.gravitee.repository.management.api.ApiQualityRuleRepository;
import io.gravitee.repository.management.model.ApiQualityRule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
public class ElassandraApiQualityRuleRepository extends ElassandraCrud<ApiQualityRule, Object[]> implements ApiQualityRuleRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraApiQualityRuleRepository.class);

    public ElassandraApiQualityRuleRepository() throws IOException {
        super("api_quality_rules",
                new String[]{"id", "quality_rule", "checked", "created_at", "updated_at"},
                new String[]{"text", "text", "boolean", "timestamp", "timestamp"},
                "api_quality_rules",
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
                                .startObject("quality_rule")
                                    .field("type", "keyword")
                                    .field("cql_collection", "singleton")
                                    .field("cql_primary_key_order", 1)
                                .endObject()
                                .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                                .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                                .startObject("checked").field("type", "boolean").field("cql_collection", "singleton").endObject()
                            .endObject()
                        .endObject());
    }

    @Override
    public Object[] values(ApiQualityRule apiQualityRule) {
        return new Object[]{
          apiQualityRule.getApi(),
          apiQualityRule.getQualityRule(),
          apiQualityRule.isChecked(),
          apiQualityRule.getCreatedAt(),
          apiQualityRule.getUpdatedAt()
        };
    }

    @Override
    public ApiQualityRule fromRow(Row row) {
        ApiQualityRule apiQualityRule = new ApiQualityRule();
        apiQualityRule.setApi(row.getString("id"));
        apiQualityRule.setQualityRule(row.getString("quality_rule"));
        apiQualityRule.setChecked(row.getBool("checked"));
        apiQualityRule.setCreatedAt(row.getTimestamp("created_at"));
        apiQualityRule.setUpdatedAt(row.getTimestamp("updated_at"));
        return apiQualityRule;
    }

    @Override
    public Optional<ApiQualityRule> findById(String api, String qualityRule) throws TechnicalException {
        LOGGER.debug("Find by ID [{}]-[{}]", api, qualityRule);
        if (Strings.isNullOrEmpty(api) || Strings.isNullOrEmpty(qualityRule)) {
            return Optional.empty();
        }
        return Optional.ofNullable(session.execute(selectStmt.bind(api, qualityRule)).one()).map(this::fromRow);
    }

    @Override
    public void delete(String api, String qualityRule) throws TechnicalException {
        LOGGER.debug("delete [{}]-[{}]", api, qualityRule);
        if (Strings.isNullOrEmpty(api) || Strings.isNullOrEmpty(qualityRule)) {
            throw new IllegalStateException("Delete requires api and qualityRule");
        }
        session.execute(deleteStmt.bind(api, qualityRule));
    }

    @Override
    public List<ApiQualityRule> findByQualityRule(String qualityRule) throws TechnicalException {
        LOGGER.debug("Find by QualityRule [{}]", qualityRule);
        if (Strings.isNullOrEmpty(qualityRule)) {
            return Collections.EMPTY_LIST;
        }

        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(QueryBuilders.termQuery("quality_rule", qualityRule));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);

            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find by qualityRule="+qualityRule;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

    @Override
    public List<ApiQualityRule> findByApi(String api) throws TechnicalException {
        LOGGER.debug("Find by Api [{}]", api);
        if (Strings.isNullOrEmpty(api)) {
            return Collections.EMPTY_LIST;
        }

        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(QueryBuilders.termQuery("id", api));

            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .toString(ToXContent.EMPTY_PARAMS);

            LOGGER.debug("query={}", esQuery);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find by api="+api;
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

    @Override
    public void deleteByQualityRule(String qualityRule) throws TechnicalException {
        LOGGER.debug("Delete by QualityRule [{}]", qualityRule);
        if (Strings.isNullOrEmpty(qualityRule)) {
            throw new IllegalStateException("QualityRule parameter is null");
        }

        for (ApiQualityRule rule : findByQualityRule (qualityRule)) {
            delete(rule.getApi(), rule.getQualityRule());
        }
    }

    @Override
    public void deleteByApi(String api) throws TechnicalException {
        LOGGER.debug("Delete by Api [{}]", api);
        if (Strings.isNullOrEmpty(api)) {
            throw new IllegalStateException("Api parameter is null");
        }
        deletePartition(api);
    }
}
