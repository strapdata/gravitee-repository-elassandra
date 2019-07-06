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
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.PlanRepository;
import io.gravitee.repository.management.model.Plan;
import io.gravitee.repository.management.model.Plan.PlanSecurityType;

/**
 * @author vroyer
 */
@Repository
public class ElassandraPlanRepository extends ElassandraCrud<Plan, String> implements PlanRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraPlanRepository.class);

    public ElassandraPlanRepository() throws IOException {
        super("plans",
                new String[]{
                        "id",
                        "name",
                        "description",
                        "security",
                        "security_definition",
                        "selection_rule",
                        "validation",
                        "type",
                        "plan_order",
                        "apis",
                        "created_at",
                        "updated_at",
                        "definition",
                        "characteristics",
                        "status",
                        "excluded_groups",
                        "published_at",
                        "closed_at",
                        "need_redeploy_at",
                        "comment_required",
                        "comment_message",
                        "tags"
                        },
                "plans",
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
                        .startObject("name").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("description").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("security").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("security_definition").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("selection_rule").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("validation").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("type").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("plan_order").field("type", "integer").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("apis").field("type", "keyword").field("cql_collection", "set").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("definition").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("characteristics").field("type", "keyword").field("cql_collection", "list").field("index", false).endObject()
                        .startObject("status").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("excluded_groups").field("type", "keyword").field("cql_collection", "list").field("index", false).endObject()
                        .startObject("published_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("closed_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("need_redeploy_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("comment_required").field("type", "boolean").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("comment_message").field("type", "text").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("tags").field("type", "keyword").field("cql_collection", "set").endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Plan plan) {
        return new Object[]{
                plan.getId(),
                plan.getName(),
                plan.getDescription(),
                plan.getSecurity() == null ? null : plan.getSecurity().name(),
                plan.getSecurityDefinition(),
                plan.getSelectionRule(),
                plan.getValidation() == null ? Plan.PlanValidationType.MANUAL.toString() : plan.getValidation().toString(),
                plan.getType() == null ? Plan.PlanType.API.toString() : plan.getType().toString(),
                plan.getOrder(),
                plan.getApis(),
                plan.getCreatedAt(),
                plan.getUpdatedAt(),
                plan.getDefinition(),
                plan.getCharacteristics(),
                plan.getStatus() == null ? null : plan.getStatus().toString(),
                plan.getExcludedGroups(),
                plan.getPublishedAt(),
                plan.getClosedAt(),
                plan.getNeedRedeployAt(),
                plan.isCommentRequired(),
                plan.getCommentMessage(),
                plan.getTags()
        };
    }

    @Override
    public Plan fromRow(Row row) {
        if (row != null) {
            final Plan plan = new Plan();
            plan.setId(row.getString("id"));
            plan.setName(row.getString("name"));
            plan.setDescription(row.getString("description"));
            plan.setSecurity(row.isNull("security") ? null : PlanSecurityType.valueOf(row.getString("security")));
            plan.setSecurityDefinition(row.getString("security_definition"));
            plan.setSelectionRule(row.getString("selection_rule"));
            plan.setValidation(Plan.PlanValidationType.valueOf(row.getString("validation").toUpperCase()));
            plan.setType(Plan.PlanType.valueOf(row.getString("type").toUpperCase()));
            plan.setOrder(row.getInt("plan_order"));
            plan.setApis(row.getSet("apis", String.class));
            plan.setCreatedAt(row.getTimestamp("created_at"));
            plan.setUpdatedAt(row.getTimestamp("updated_at"));
            plan.setDefinition(row.getString("definition"));
            plan.setCharacteristics(row.getList("characteristics", String.class));
            plan.setStatus(row.isNull("status") ? null : Plan.Status.valueOf(row.getString("status")));
            plan.setExcludedGroups(row.getList("excluded_groups", String.class));
            plan.setPublishedAt(row.getTimestamp("published_at"));
            plan.setClosedAt(row.getTimestamp("closed_at"));
            plan.setNeedRedeployAt(row.getTimestamp("need_redeploy_at"));
            plan.setCommentRequired(row.getBool("comment_required"));
            plan.setCommentMessage(row.getString("comment_message"));
            plan.setTags(row.getSet("tags", String.class));
            return plan;
        }
        return null;
    }



    @Override
    public Set<Plan> findByApi(String api) throws TechnicalException {
        LOGGER.debug("Find Plans by Api [{}]", api);

        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("apis", api))
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("es_query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));

        return resultSet.all().stream()
                .map(this::fromRow)
                .collect(Collectors.toSet());
    }

}
