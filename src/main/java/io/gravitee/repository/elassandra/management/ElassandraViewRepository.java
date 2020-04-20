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

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.model.User;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.ViewRepository;
import io.gravitee.repository.management.model.View;

import java.io.IOException;
import java.util.Optional;

/**
 * @author vroyer
 */
@Repository
public class ElassandraViewRepository extends ElassandraCrud<View, String> implements ViewRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraViewRepository.class);

    public ElassandraViewRepository() throws IOException {
        super("views",
                new String[] {
                        "id",
                        "key",
                        "name",
                        "description",
                        "default_view",
                        "hidden",
                        "view_order",
                        "created_at",
                        "updated_at",
                        "picture",
                        "highlight_api"
                },
                "views",
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
                        .startObject("key").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("name").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("description").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("default_view").field("type", "boolean").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("hidden").field("type", "boolean").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("view_order").field("type", "integer").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("picture").field("type", "text").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("highlight_api").field("type", "text").field("cql_collection", "singleton").field("index", false).endObject()
                        .endObject()
                        .endObject());
    }

    @Override
    public Object[] values(View t) {
        return new Object[] {
                t.getId(),
                t.getKey(),
                t.getName(),
                t.getDescription(),
                t.isDefaultView(),
                t.isHidden(),
                t.getOrder(),
                t.getCreatedAt(),
                t.getUpdatedAt(),
                t.getPicture(),
                t.getHighlightApi()
        };
    }

    @Override
    public View fromRow(Row row) {
        if (row != null) {
            final View view = new View();
            view.setId(row.getString("id"));
            view.setKey(row.getString("key"));
            view.setName(row.getString("name"));
            view.setDescription(row.getString("description"));
            view.setDefaultView(row.isNull("default_view") ? null : row.getBool("default_view"));
            view.setHidden(row.isNull("hidden") ? null : row.getBool("hidden"));
            view.setOrder(row.getInt("view_order"));
            view.setCreatedAt(row.getTimestamp("created_at"));
            view.setUpdatedAt(row.getTimestamp("updated_at"));
            view.setPicture(row.getString("picture"));
            view.setHighlightApi(row.getString("highlight_api"));
            return view;
        }
        return null;
    }

    @Override
    public Optional<View> findByKey(String key) throws TechnicalException {
        LOGGER.debug("Find View by key [{}]", key);
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("key", key))
                .toString(ToXContent.EMPTY_PARAMS);
        final Row row = session.execute(this.esQueryStmt.bind(esQuery)).one();
        return Optional.ofNullable(fromRow(row));
    }
}
