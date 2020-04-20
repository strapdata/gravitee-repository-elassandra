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

import static org.springframework.util.CollectionUtils.isEmpty;
import static org.springframework.util.StringUtils.isEmpty;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
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
import org.springframework.data.cassandra.core.cql.support.CachedPreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.support.PreparedStatementCache;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.management.api.ApiRepository;
import io.gravitee.repository.management.api.search.ApiCriteria;
import io.gravitee.repository.management.api.search.ApiFieldExclusionFilter;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Api;
import io.gravitee.repository.management.model.ApiLifecycleState;
import io.gravitee.repository.management.model.LifecycleState;
import io.gravitee.repository.management.model.Visibility;

/**
 * @author vroyer@strapdata.com
 */
@Repository
public class ElassandraApiRepository extends ElassandraCrud<Api, String> implements ApiRepository, PageableRepository<Api> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraApiRepository.class);

    private final PreparedStatementCache cache = PreparedStatementCache.create();

    public ElassandraApiRepository() throws IOException {
        super("apis",
                new String[] { "id", "name", "description", "version", "definition", "deployed_at", "created_at", "updated_at", "visibility", "lifecycle_state", "api_lifecycle_state", "picture", "groups", "views", "labels" },
                "apis",
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
                        .startObject("description").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("version").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("definition").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("deployed_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").endObject()
                        .startObject("visibility").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("lifecycle_state").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("api_lifecycle_state").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("picture").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("groups").field("type", "keyword").field("cql_collection", "set").endObject()
                        .startObject("views").field("type", "keyword").field("cql_collection", "set").endObject()
                        .startObject("labels").field("type", "keyword").field("cql_collection", "list").endObject()
                    .endObject()
                .endObject()
                );
    }

    @Override
    public Object[] values(Api api) {
        return new Object[]{
                api.getId(),
                api.getName(),
                api.getDescription(),
                api.getVersion(),
                api.getDefinition(),
                api.getDeployedAt(),
                api.getCreatedAt(),
                api.getUpdatedAt(),
                api.getVisibility() == null ? null : api.getVisibility().name(),
                api.getLifecycleState() == null ? null : api.getLifecycleState().name(),
                api.getApiLifecycleState() == null ? null : api.getApiLifecycleState().name(),
                api.getPicture(),
                api.getGroups(),
                api.getViews(),
                api.getLabels()};
    }

    @Override
    public Api fromRow(Row row) {
        if (row != null) {
            final Api api = new Api();
            api.setId(row.getString("id"));
            api.setName(row.getString("name"));
            api.setDescription(row.getString("description"));
            api.setVersion(row.getString("version"));
            api.setDefinition(row.getColumnDefinitions().contains("definition") ? row.getString("definition") : null);
            api.setDeployedAt(row.getTimestamp("deployed_at"));
            api.setCreatedAt(row.getTimestamp("created_at"));
            api.setUpdatedAt(row.getTimestamp("updated_at"));
            api.setVisibility(row.getString("visibility") == null ? null : Visibility.valueOf(row.getString("visibility")));
            api.setLifecycleState(row.isNull("lifecycle_state") ? LifecycleState.STOPPED : LifecycleState.valueOf(row.getString("lifecycle_state")));
            api.setApiLifecycleState(row.isNull("api_lifecycle_state") ? ApiLifecycleState.CREATED : ApiLifecycleState.valueOf(row.getString("api_lifecycle_state")));
            api.setPicture(row.getColumnDefinitions().contains("picture") ? row.getString("picture") : null);
            api.setGroups(row.getColumnDefinitions().contains("groups") ? row.getSet("groups", String.class) : null);
            api.setViews(row.getSet("views", String.class));
            api.setLabels(row.getColumnDefinitions().contains("labels") ? row.getList("labels", String.class) : null);
            return api;
        }
        return null;
    }

    @Override
    public Page<Api> search(ApiCriteria apiCriteria, Pageable page) {
        final List<Api> apis = findByCriteria(apiCriteria, null);
        return getResultAsPage(page, apis);
    }

    @Override
    public List<Api> search(ApiCriteria apiCriteria) {
        return findByCriteria(apiCriteria, null);
    }

    @Override
    public List<Api> search(ApiCriteria apiCriteria, ApiFieldExclusionFilter apiFieldExclusionFilter) {
        return findByCriteria(apiCriteria, apiFieldExclusionFilter);
    }

    private List<Api> findByCriteria(ApiCriteria apiCriteria, ApiFieldExclusionFilter apiFieldExclusionFilter) {
        LOGGER.debug("search({})", apiCriteria);
        List<String> projection = Lists.newArrayList("id", "name", "description", "version", "deployed_at", "created_at", "updated_at", "visibility", "lifecycle_state", "api_lifecycle_state", "views");
        if (apiFieldExclusionFilter == null || !apiFieldExclusionFilter.isDefinition()) {
            projection.add("definition");
        }
        if (apiFieldExclusionFilter == null || !apiFieldExclusionFilter.isPicture()) {
            projection.add("picture");
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (apiCriteria != null) {
            if (!isEmpty(apiCriteria.getGroups())) {
                queryBuilder.filter(QueryBuilders.termsQuery("groups", apiCriteria.getGroups()));
            }
            if (!isEmpty(apiCriteria.getLabel())) {
                queryBuilder.filter(QueryBuilders.termQuery("labels", apiCriteria.getLabel()));
            }
            if (!isEmpty(apiCriteria.getIds())) {
                queryBuilder.filter(QueryBuilders.termsQuery("id", apiCriteria.getIds()));
            }
            if (!isEmpty(apiCriteria.getName())) {
                queryBuilder.filter(QueryBuilders.termQuery("name", apiCriteria.getName()));
            }
            if (apiCriteria.getState() != null) {
                queryBuilder.filter(QueryBuilders.termQuery("lifecycle_state", apiCriteria.getState().name()));
            }
            if (!isEmpty(apiCriteria.getVersion())) {
                queryBuilder.filter(QueryBuilders.termQuery("version", apiCriteria.getVersion()));
            }
            if (!isEmpty(apiCriteria.getView())) {
                queryBuilder.filter(QueryBuilders.termQuery("views", apiCriteria.getView()));
            }
            if (apiCriteria.getVisibility() != null) {
                queryBuilder.filter(QueryBuilders.termQuery("visibility", apiCriteria.getVisibility().toString()));
            }
            if (apiCriteria.getLifecycleStates() != null && !apiCriteria.getLifecycleStates().isEmpty()) {
                queryBuilder.filter(QueryBuilders.termsQuery("api_lifecycle_state", apiCriteria.getLifecycleStates().stream().map(ApiLifecycleState::name).collect(Collectors.toList())));
            }
        }

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .sort("name", SortOrder.ASC)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);

        PreparedStatement stmt = CachedPreparedStatementCreator.of(cache,
                String.format(Locale.ROOT,"SELECT %s FROM %s WHERE es_query = ? AND es_options='indices=%s' LIMIT ? ALLOW FILTERING", String.join(",", projection), tableName, indexName))
                .createPreparedStatement(session);
        final ResultSet resultSet = session.execute(stmt.bind(esQuery, 1000));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }
}
