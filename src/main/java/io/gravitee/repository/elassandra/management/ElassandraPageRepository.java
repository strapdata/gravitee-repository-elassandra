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
import java.util.Collections;
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
import io.gravitee.repository.management.api.PageRepository;
import io.gravitee.repository.management.api.search.PageCriteria;
import io.gravitee.repository.management.model.Page;
import io.gravitee.repository.management.model.PageSource;
import io.gravitee.repository.management.model.PageType;

/**
 * @author vroyer
 */
@Repository
public class ElassandraPageRepository extends ElassandraCrud<Page, String> implements PageRepository {

    private static  final Logger LOGGER = LoggerFactory.getLogger(ElassandraPageRepository.class);

    private static final String PAGES_TABLE = "pages";
    private static final String NULL = "NULL";
    private static final String PAGE_ORDER = "page_order";

    public ElassandraPageRepository() throws IOException {
        super("pages",
                new String[]{
                        "id",
                        "type",
                        "parent_id",
                        "name",
                        "content",
                        "last_contributor",
                        "page_order",
                        "published",
                        "source_type",
                        "source_configuration",
                        "configuration",
                        "api",
                        "created_at",
                        "updated_at",
                        "homepage",
                        "excluded_groups",
                        "metadata"},
                new String[]{
                        "text",
                        "text",
                        "text",
                        "text",
                        "text",
                        "text",
                        "int",
                        "boolean",
                        "text",
                        "text",
                        "map<text,text>",
                        "text",
                        "timestamp",
                        "timestamp",
                        "boolean",
                        "list<text>",
                        "map<text,text>"},
                "pages",
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
                        .startObject("type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("parent_id").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("name").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("content").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("last_contributor").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("page_order").field("type", "integer").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("published").field("type", "boolean").field("cql_collection", "singleton").endObject()
                        .startObject("source_type").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("source_configuration").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("api").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("homepage").field("type", "boolean").field("cql_collection", "singleton").endObject()
                        .startObject("excluded_groups").field("type", "keyword").field("cql_collection", "list").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Page page) {
        return new Object[]{
                page.getId(),
                page.getType() == null ? null : page.getType().toString(),
                page.getParentId(),
                page.getName(),
                page.getContent(),
                page.getLastContributor(),
                page.getOrder(),
                page.isPublished(),
                page.getSource() == null ? null : page.getSource().getType(),
                page.getSource() == null ? null : page.getSource().getConfiguration(),
                page.getConfiguration(),
                page.getApi(),
                page.getCreatedAt(),
                page.getUpdatedAt(),
                page.isHomepage(),
                page.getExcludedGroups(),
                page.getMetadata()};
    }

    @Override
    public Page fromRow(Row row) {
        if (row != null) {
            final Page page = new Page();
            page.setId(row.getString("id"));
            page.setName(row.getString("name"));
            page.setParentId(row.getString("parent_id"));
            page.setType(row.isNull("type") ? null : PageType.valueOf(row.getString("type").toUpperCase()));
            page.setContent(row.getString("content"));
            page.setLastContributor(row.getString("last_contributor"));
            page.setOrder(row.getInt(PAGE_ORDER));
            page.setPublished(row.isNull("published") ? null : row.getBool("published"));
            page.setHomepage(row.isNull("homepage") ? null : row.getBool("homepage"));
            page.setExcludedGroups(row.getList("excluded_groups", String.class));

            final String sourceType = row.getString("source_type");
            final String sourceConfiguration = row.getString("source_configuration");
            if (sourceConfiguration != null || sourceType != null) {
                final PageSource pageSource = new PageSource();
                pageSource.setType(sourceType);
                pageSource.setConfiguration(sourceConfiguration);
                page.setSource(pageSource);
            }
            page.setConfiguration(row.isNull("configuration") ? Collections.EMPTY_MAP : row.getMap("configuration", String.class, String.class));
            page.setApi(NULL.equals(row.getString("api")) ? null : row.getString("api"));
            page.setCreatedAt(row.getTimestamp("created_at"));
            page.setUpdatedAt(row.getTimestamp("updated_at"));
            page.setMetadata(row.getMap("metadata", String.class, String.class));
            return page;
        }
        return null;
    }

    @Override
    public Integer findMaxPortalPageOrder() throws TechnicalException {
        LOGGER.debug("Find max Portal Pages order");

        String esQuery = new SearchSourceBuilder()
                .query(new BoolQueryBuilder().mustNot(QueryBuilders.existsQuery("api")))
                .sort(PAGE_ORDER, SortOrder.DESC)
                .size(1)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);

        final Row row = session.execute(this.esQueryStmt.bind(esQuery)).one();
        return row == null ? 0 : row.getInt(PAGE_ORDER);
    }

    @Override
    public Integer findMaxApiPageOrderByApiId(String apiId) throws TechnicalException {
        LOGGER.debug("Find max Page order by Api ID [{}]", apiId);

        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("api", apiId))
                .sort(PAGE_ORDER, SortOrder.DESC)
                .size(1)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);

        final Row row = session.execute(this.esQueryStmt.bind(esQuery)).one();
        return row == null ? 0 : row.getInt(PAGE_ORDER);
    }

    @Override
    public List<Page> search(PageCriteria criteria) throws TechnicalException {
        LOGGER.debug("search({})", criteria);
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            if (criteria != null) {
                if (criteria.getHomepage() != null) {
                    queryBuilder.filter(QueryBuilders.termQuery("homepage", criteria.getHomepage()));
                }
                if (criteria.getApi() == null) {
                    queryBuilder.mustNot(QueryBuilders.existsQuery("api"));
                } else {
                    queryBuilder.filter(QueryBuilders.termQuery("api", criteria.getApi()));
                }
                if (criteria.getPublished() != null) {
                    queryBuilder.filter(QueryBuilders.termQuery("published", criteria.getPublished()));
                }
                if (criteria.getName() != null) {
                    queryBuilder.filter(QueryBuilders.termQuery("name", criteria.getName()));
                }
                if (criteria.getParent() != null) {
                    queryBuilder.filter(QueryBuilders.termQuery("parent_id", criteria.getParent()));
                }
                if (criteria.getRootParent() != null && criteria.getRootParent().equals(Boolean.TRUE)) {
                    queryBuilder.mustNot(QueryBuilders.existsQuery("parent_id"));
                }
                if (criteria.getType() != null) {
                    queryBuilder.filter(QueryBuilders.termQuery("type", criteria.getType()));
                }
            }
            String esQuery = new SearchSourceBuilder()
                    .query(queryBuilder.hasClauses() ? queryBuilder : QueryBuilders.matchAllQuery())
                    .sort(PAGE_ORDER, SortOrder.DESC)
                    .toString(ToXContent.EMPTY_PARAMS);
            LOGGER.debug("query={}", esQuery);

            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
        } catch (final Exception ex) {
            final String message = "Failed to find portal pages";
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }
}
