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

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.lucene.search.join.ScoreMode;
import org.elassandra.index.ElasticIncomingPayload;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.AuditRepository;
import io.gravitee.repository.management.api.search.AuditCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.api.search.builder.PageableBuilder;
import io.gravitee.repository.management.model.Audit;
import io.gravitee.repository.management.model.Audit.AuditProperties;
import io.gravitee.repository.management.model.User;
import io.gravitee.repository.management.model.UserStatus;

/**
 * @author vroyer
 */
@Repository
public class ElassandraAuditRepository extends ElassandraCrud<Audit, String> implements AuditRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraAuditRepository.class);

    public ElassandraAuditRepository() throws IOException {
        super("audits",
                new String[]{"id", "reference_type", "reference_id", "username", "event", "patch", "audit_properties", "created_at" },
                new String[]{"text", "text", "text", "text", "text", "text", "map<text,text>", "timestamp" },
                "audits",
                Settings.builder(),
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
                        .startObject("username").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("event").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("patch").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("audit_properties").field("type", "nested").field("cql_collection", "singleton").field("cql_struct", "map")
                          .startObject("properties")
                            .startObject(AuditProperties.API.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.API_HEADER.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.API_KEY.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.APPLICATION.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.CLIENT_REGISTRATION_PROVIDER.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.DICTIONARY.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.ENTRYPOINT.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.GROUP.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.IDENTITY_PROVIDER.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.METADATA.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.PAGE.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.PARAMETER.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.PLAN.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.REQUEST_ID.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.ROLE.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.TAG.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.TENANT.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.USER.name().toLowerCase()).field("type", "keyword").endObject()
                            .startObject(AuditProperties.VIEW.name().toLowerCase()).field("type", "keyword").endObject()
                          .endObject()
                        .endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject()
                );
    }

    @Override
    public Object[] values(Audit audit) {
        return new Object[] {
                audit.getId(),
                audit.getReferenceType() == null ? null : audit.getReferenceType().name(),
                audit.getReferenceId(),
                audit.getUser(),
                audit.getEvent(),
                audit.getPatch(),
                audit.getProperties(),
                audit.getCreatedAt()
        };
    }

    @Override
    public Audit fromRow(Row row) {
        if (row!= null) {
            final Audit audit = new Audit();
            audit.setId(row.getString("id"));
            audit.setReferenceType(Audit.AuditReferenceType.valueOf(row.getString("reference_type")));
            audit.setReferenceId(row.getString("reference_id"));
            audit.setUser(row.getString("username"));
            audit.setEvent(row.getString("event"));
            audit.setPatch(row.getString("patch"));
            audit.setProperties(row.getMap("audit_properties", String.class, String.class));
            audit.setCreatedAt(row.getTimestamp("created_at"));
            return audit;
        }
        return null;
    }

    @Override
    public Page<Audit> search(AuditCriteria criteria, Pageable pageable) {
        LOGGER.debug("Search User by criteria [{}]", criteria);
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (criteria != null) {
            if (criteria.getFrom() > 0 || criteria.getTo() > 0) {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("created_at");
                if (criteria.getFrom() > 0)
                    rangeQueryBuilder.gte(new Date(criteria.getFrom()));
                if (criteria.getTo() > 0)
                    rangeQueryBuilder.lte(new Date(criteria.getTo()));
                queryBuilder.filter(rangeQueryBuilder);
            }
            if (criteria.getEvents() != null && !criteria.getEvents().isEmpty())
                queryBuilder.filter(QueryBuilders.termsQuery("event", criteria.getEvents()));
            if (criteria.getReferences() != null && !criteria.getReferences().isEmpty()) {
                BoolQueryBuilder auditRefQuery = new BoolQueryBuilder();
                for (Entry<Audit.AuditReferenceType, List<String>> ref : criteria.getReferences().entrySet()) {
                    auditRefQuery.should(new BoolQueryBuilder()
                        .filter(QueryBuilders.termQuery("reference_type", ref.getKey()))
                        .filter(QueryBuilders.termsQuery("reference_id", ref.getValue())));
                }
                auditRefQuery.minimumShouldMatch(1);
                queryBuilder.filter(auditRefQuery);
            }
            if (criteria.getProperties() != null && !criteria.getProperties().isEmpty()) {
                BoolQueryBuilder auditPropQuery = new BoolQueryBuilder();
                for (Entry<String, String> property : criteria.getProperties().entrySet()) {
                    auditPropQuery.should(
                            QueryBuilders.nestedQuery("audit_properties",
                            QueryBuilders.termQuery("audit_properties."+property.getKey().toLowerCase(), property.getValue()),
                            ScoreMode.None));
                }
                auditPropQuery.minimumShouldMatch(1);
                queryBuilder.filter(auditPropQuery);
            }
        }

        SearchSourceBuilder esQueryBuilder = new SearchSourceBuilder()
                .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                .sort("created_at", SortOrder.DESC);

        int pageSize = 1000;
        if (pageable != null) {
            if (pageable.from() >= 0)
                esQueryBuilder.from(pageable.from());
            if (pageable.pageSize() > 0) {
                pageSize = pageable.pageSize();
            }
        }
        esQueryBuilder.size(pageSize);

        String esQuery = esQueryBuilder.toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("es_query={}", esQuery);

        final ResultSet resultSet = session.execute(this.esQueryStmtWithLimit.bind(esQuery, pageSize));
        ElasticIncomingPayload payload = new ElasticIncomingPayload(resultSet.getExecutionInfo().getIncomingPayload());
        List<Audit> result = resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        return new Page<>(result,
                        (pageable == null) ? 0 : pageable.pageNumber(),
                        result.size(),
                        payload.hitTotal);
    }
}
